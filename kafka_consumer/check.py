# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
import random
from collections import defaultdict
import time

# 3p
from kafka.client import KafkaClient
from kafka.protocol.offset import OffsetRequest
from kafka.protocol.commit import GroupCoordinatorRequest
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

# project
from checks import AgentCheck


DEFAULT_KAFKA_TIMEOUT = 5
DEFAULT_ZK_TIMEOUT = 5


class KafkaCheck(AgentCheck):
    """
    Check Consumer Lag for Kafka consumers that store their offsets in Zookeeper.

    WARNING: Modern Kafka consumer store their offsets in Kafka rather than
    zookeeper.

    This check also returns broker highwater offsets.
    """

    SOURCE_TYPE_NAME = 'kafka'

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances=instances)
        self.zk_timeout = int(
            init_config.get('zk_timeout', DEFAULT_ZK_TIMEOUT))
        self.kafka_timeout = int(
            init_config.get('kafka_timeout', DEFAULT_KAFKA_TIMEOUT))

        self.kafka_clients = {}

    def stop(self):
        """
        cleanup kafka connections (to all brokers) to avoid leaving
        stale connections in older kafkas.
        """
        for cli in self.kafka_clients.itervalues():
            cli.close()

    def _get_instance_key(self, instance):
        return self.read_config(instance, 'kafka_connect_str')

    def _get_kafka_client(self, instance):
        kafka_conn_str = self.read_config(instance, 'kafka_connect_str')
        if not kafka_conn_str:
            raise Exception('Bad instance')

        instance_key = self._get_instance_key(instance)
        if kafka_conn_str not in self.kafka_clients:
            cli = KafkaClient(bootstrap_servers=kafka_conn_str, client_id='dd-agent')
            self.kafka_clients[instance_key] = cli

        return self.kafka_clients[instance_key]

    def _get_group_coordinator(self, client, group):
        request = GroupCoordinatorRequest[0](group)

        # not all brokers might return a good response... Try all of them
        for broker in client.cluster.brokers():
            try:
                coord_resp = self._make_blocking_req(client, request, nodeid=broker.nodeId)
                if coord_resp:
                    client.cluster.add_group_coordinator(group, coord_resp)
                    return client.cluster.coordinator_for_group(group)
            except AssertionError:
                continue

        return None

    def _get_random_node_id(self, client):
        brokers = client.cluster.brokers()
        if not brokers:
            raise Exception('No known available brokers... make this a specific exception')
        nodeid = random.sample(brokers, 1)[0].nodeId

    def _make_req_async(self, client, request, nodeid=None, cb=None):
        if not nodeid:
            nodeid = self._get_random_node_id(client)

        future = client.send(nodeid, request)
        if cb:
            future.add_callback(cb, request, nodeid, self.current_ts)

    def _make_blocking_req(self, client, request, nodeid=None):
        if not nodeid:
            nodeid = self._get_random_node_id(client)

        future = client.send(nodeid, request)
        client.poll(future=future)  # block until we get response.
        assert future.succeeded()
        response = future.value

        return response

    def _process_highwater_offsets(self, request, instance, nodeid, response):
        highwater_offsets = {}
        topic_partitions_without_a_leader = {}

        instance_key = self._get_instance_key(instance)
        for topic, partitions in response.topics:
            for partition, error_code, offsets in partitions:
                if error_code == 0:
                    highwater_offsets[(topic, partition)] = offsets[0]
                    # Valid error codes:
                    # https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-PossibleErrorCodes.2
                elif error_code == -1:
                    self.log.error("Kafka broker returned UNKNOWN (error_code -1) for topic: %s, partition: %s. "
                                   "This should never happen.", topic, partition)
                elif error_code == 3:
                    self.log.warn("Kafka broker returned UNKNOWN_TOPIC_OR_PARTITION (error_code 3) for "
                                  "topic: %s, partition: %s. This should only happen if the topic is currently being deleted.",
                                  topic, partition)
                elif error_code == 6:
                    self.log.warn("Kafka broker returned NOT_LEADER_FOR_PARTITION (error_code 6) for "
                                  "topic: %s, partition: %s. This should only happen if the broker that was the partition "
                                  "leader when kafka_client.cluster last fetched metadata is no longer the leader.",
                                  topic, partition)
                    topic_partitions_without_a_leader.append((topic, partition))

            return highwater_offsets, topic_partitions_without_a_leader

    def _get_broker_offsets(self, instance):
        """
        Fetch highwater offsets for each topic/partition from Kafka cluster.

        Do this for all partitions in the cluster because even if it has no
        consumers, we may want to measure whether producers are successfully
        producing. No need to limit this for performance because fetching broker
        offsets from Kafka is a relatively inexpensive operation.

        Sends one OffsetRequest per broker to get offsets for all partitions
        where that broker is the leader:
        https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI(AKAListOffset)

        Can we cleanup connections on agent restart?
        Brokers before 0.9 - accumulate stale connections on restarts.
        In 0.9 Kafka added connections.max.idle.ms
        https://issues.apache.org/jira/browse/KAFKA-1282
        """

        # Connect to Kafka
        highwater_offsets = {}
        topic_partitions_without_a_leader = []
        cli = self._get_kafka_client(instance)
        try:
            # store partitions that exist but unable to fetch offsets for later
            # error checking
            for broker in cli.cluster.brokers():
                if not cli.ready(broker.nodeId):
                    self.log.info('kafka broker unavailable this iteration - skipping')
                    continue

                # Group partitions by topic in order to construct the OffsetRequest
                partitions_grouped_by_topic = defaultdict(list)
                # partitions_for_broker returns all partitions for which this
                # broker is leader. So any partitions that don't currently have
                # leaders will be missed. Ignore as they'll be caught on next check run.
                broker_partitions = cli.cluster.partitions_for_broker(broker.nodeId)
                if broker_partitions:
                    for topic, partition in broker_partitions:
                        partitions_grouped_by_topic[topic].append(partition)

                        # Construct the OffsetRequest
                        timestamp = -1  # -1 for latest, -2 for earliest
                        max_offsets = 1
                        request = OffsetRequest[0](
                            replica_id=-1,
                            topics=[
                                (topic, [
                                    (partition, timestamp, max_offsets) for partition in partitions])
                                for topic, partitions in partitions_grouped_by_topic.iteritems()])

                    response = self._make_blocking_req(cli, request, nodeid=broker.nodeId)
                    offsets, unlead = self._process_highwater_offsets(request, instance, broker.nodeId, response)
                    highwater_offsets.update(offsets)
                    topic_partitions_without_a_leader.append(unlead)
        except Exception:
            self.log.exception('There was a problem collecting the high watermark offsets')

        return highwater_offsets, list(set(topic_partitions_without_a_leader))

    def _get_zk_path_children(self, zk_conn, zk_path, name_for_error):
        """Fetch child nodes for a given Zookeeper path."""
        children = []
        try:
            children = zk_conn.get_children(zk_path)
        except NoNodeError:
            self.log.info('No zookeeper node at %s', zk_path)
        except Exception:
            self.log.exception('Could not read %s from %s', name_for_error, zk_path)
        return children

    def _get_zk_consumer_offsets(self, zk_hosts_ports, consumer_groups=None, zk_prefix=''):
        """
        Fetch Consumer Group offsets from Zookeeper.

        Also fetch consumer_groups, topics, and partitions if not
        already specified in consumer_groups.

        :param dict consumer_groups: The consumer groups, topics, and partitions
            that you want to fetch offsets for. If consumer_groups is None, will
            fetch offsets for all consumer_groups. For examples of what this
            dict can look like, see _validate_consumer_groups().
        """
        zk_consumer_offsets = {}

        # Construct the Zookeeper path pattern
        # /consumers/[groupId]/offsets/[topic]/[partitionId]
        zk_path_consumer = zk_prefix + '/consumers/'
        zk_path_topic_tmpl = zk_path_consumer + '{group}/offsets/'
        zk_path_partition_tmpl = zk_path_topic_tmpl + '{topic}/'

        zk_conn = KazooClient(zk_hosts_ports, timeout=self.zk_timeout)
        zk_conn.start()
        try:
            if consumer_groups is None:
                # If consumer groups aren't specified, fetch them from ZK
                consumer_groups = {consumer_group: None for consumer_group in
                    self._get_zk_path_children(zk_conn, zk_path_consumer, 'consumer groups')}

            for consumer_group, topics in consumer_groups.iteritems():
                if topics is None:
                    # If topics are't specified, fetch them from ZK
                    zk_path_topics = zk_path_topic_tmpl.format(group=consumer_group)
                    topics = {topic: None for topic in
                        self._get_zk_path_children(zk_conn, zk_path_topics, 'topics')}

                for topic, partitions in topics.iteritems():
                    if partitions is not None:
                        partitions = set(partitions)  # defend against bad user input
                    else:
                        # If partitions aren't specified, fetch them from ZK
                        zk_path_partitions = zk_path_partition_tmpl.format(
                            group=consumer_group, topic=topic)
                        # Zookeeper returns the partition IDs as strings because
                        # they are extracted from the node path
                        partitions = [int(x) for x in self._get_zk_path_children(
                            zk_conn, zk_path_partitions, 'partitions')]

                    # Fetch consumer offsets for each partition from ZK
                    for partition in partitions:
                        zk_path = (zk_path_partition_tmpl + '{partition}/').format(
                            group=consumer_group, topic=topic, partition=partition)
                        try:
                            consumer_offset = int(zk_conn.get(zk_path)[0])
                            key = (consumer_group, topic, partition)
                            zk_consumer_offsets[key] = consumer_offset
                        except NoNodeError:
                            self.log.info('No zookeeper node at %s', zk_path)
                        except Exception:
                            self.log.exception('Could not read consumer offset from %s', zk_path)
        finally:
            try:
                zk_conn.stop()
                zk_conn.close()
            except Exception:
                self.log.exception('Error cleaning up Zookeeper connection')
        return zk_consumer_offsets

    def check(self, instance):
        # For calculating lag, we have to fetch offsets from both kafka and
        # zookeeper. There's a potential race condition because whichever one we
        # check first may be outdated by the time we check the other. Better to
        # check consumer offset before checking broker offset because worst case
        # is that overstates consumer lag a little. Doing it the other way can
        # understate consumer lag to the point of having negative consumer lag,
        # which just creates confusion because it's theoretically impossible.

        # Fetch consumer group offsets from Zookeeper
        zk_hosts_ports = self.read_config(instance, 'zk_connect_str')
        zk_prefix = instance.get('zk_prefix', '')

        # If monitor_unlisted_consumer_groups is True, fetch all groups stored in ZK
        if instance.get('monitor_unlisted_consumer_groups', False):
            consumer_groups = None
        else:
            consumer_groups = self.read_config(instance, 'consumer_groups',
                                               cast=self._validate_consumer_groups)

        consumer_offsets = self._get_zk_consumer_offsets(
            zk_hosts_ports, consumer_groups, zk_prefix)

        # Fetch the broker highwater offsets
        highwater_offsets, topic_partitions_without_a_leader = self._get_broker_offsets(instance)

        # Report the broker highwater offset
        for (topic, partition), highwater_offset in highwater_offsets.iteritems():
            broker_tags = ['topic:%s' % topic, 'partition:%s' % partition]
            self.gauge('kafka.broker_offset', highwater_offset, tags=broker_tags)

        # Report the consumer group offsets and consumer lag
        for (consumer_group, topic, partition), consumer_offset in consumer_offsets.iteritems():
            consumer_group_tags = ['topic:%s' % topic, 'partition:%s' % partition,
                'consumer_group:%s' % consumer_group]
            self.gauge('kafka.consumer_offset', consumer_offset, tags=consumer_group_tags)
            if (topic, partition) not in highwater_offsets:
                if (topic, partition) not in topic_partitions_without_a_leader:
                    self.log.warn("Consumer group: %s has offsets for topic: %s "
                        "partition: %s, but that topic partition doesn't actually "
                        "exist in the cluster.", consumer_group, topic, partition)
                continue
            consumer_lag = highwater_offsets[(topic, partition)] - consumer_offset
            if consumer_lag < 0:
                message = "Consumer lag for consumer group: {group}, topic: {topic}, " \
                    "partition: {partition} is negative. This should never happen.".format(
                        group=consumer_group,
                        topic=topic,
                        partition=partition
                    )
                self.log.info(message)

            self.gauge('kafka.consumer_lag', consumer_lag,
               tags=consumer_group_tags)

    # Private config validation/marshalling functions

    def _validate_consumer_groups(self, val):
        # val = {'consumer_group': {'topic': [0, 1]}}
        try:
            # consumer groups are optional
            assert isinstance(val, dict) or val is None
            if val is not None:
                for consumer_group, topics in val.iteritems():
                    assert isinstance(consumer_group, basestring)
                    # topics are optional
                    assert isinstance(topics, dict) or topics is None
                    if topics is not None:
                        for topic, partitions in topics.iteritems():
                            assert isinstance(topic, basestring)
                            # partitions are optional
                            assert isinstance(partitions, (list, tuple)) or partitions is None
                            if partitions is not None:
                                for partition in partitions:
                                    assert isinstance(partition, int)
            return val
        except Exception as e:
            self.log.exception(e)
            raise Exception("""The `consumer_groups` value must be a mapping of mappings, like this:
consumer_groups:
  myconsumer0: # consumer group name
    mytopic0: [0, 1] # topic_name: list of partitions
  myconsumer1:
    mytopic0: [0, 1, 2]
    mytopic1: [10, 12]
  myconsumer2:
    mytopic0:
  myconsumer3:

Note that each level of values is optional. Any omitted values will be fetched from Zookeeper.
You can omit partitions (example: myconsumer2), topics (example: myconsumer3), and even consumer_groups.
If you omit consumer_groups, you must set the flag 'monitor_unlisted_consumer_groups': True.
If a value is omitted, the parent value must still be it's expected type (typically a dict).
""")

