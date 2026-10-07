"""
An in-process Kafka for tests and local demos, with no Java and no Docker.

librdkafka (bundled with the confluent-kafka package) includes a mock cluster
that listens on a local TCP port and speaks the real Kafka wire protocol:
produce, fetch, consumer groups, offset commits, idempotent producers. Any
Kafka client can connect to it, so the app's aiokafka code runs unchanged.

It is not Apache Kafka: one process, nothing on disk, no security, and broker
behaviour is simplified. Topics are created on first use with 4 partitions. Good for proving the client logic, not for load or
failover testing.

Run it standalone for a local demo (prints the address, runs until Ctrl+C):

    python -m tests.support.mock_kafka
"""

import time


class MockKafkaCluster:
    def __init__(self, brokers: int = 1):
        from confluent_kafka import Producer

        # The mock cluster lives as long as this client object does
        self._owner = Producer({"test.mock.num.brokers": brokers})
        metadata = self._owner.list_topics(timeout=10)
        self.bootstrap_servers = ",".join(f"{b.host}:{b.port}" for b in metadata.brokers.values())


if __name__ == "__main__":
    cluster = MockKafkaCluster()
    print(f"KAFKA_BOOTSTRAP_SERVERS={cluster.bootstrap_servers}", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
