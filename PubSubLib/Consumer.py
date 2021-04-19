from concurrent.futures.thread import ThreadPoolExecutor
from typing import Callable

from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from json import loads

from kafka.consumer.fetcher import ConsumerRecord

from PubSubLib.Enums import Topic
from PubSubLib.Task import Task


class Consumer:

    def __init__(self, group_id: str, purge: bool = False, max_workers: int = 8):
        self._internal_consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest' if not purge else 'latest',
            enable_auto_commit=False,
            group_id=group_id,
            value_deserializer=lambda x: loads(x.decode('utf-8')))

        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._subscriptions : {TopicPartition: Task} = {}
        self._runningTasks : {TopicPartition: Task}= {}

    def __del__(self):
        if self is not None:
            if self._internal_consumer is not None:
                self._internal_consumer.close()
            if self._executor is not None:
                self._executor.shutdown()

    def subscribe(self, topic: Topic):
        def wrapper(callback: Callable):
            if not isinstance(topic, Topic):
                raise Exception("Incorrect subscription: use Enums.Topic")
            if topic.value in self._subscriptions:
                raise Exception(f"Multiple subscriptions to topic \"{topic}\"")
            task = Task(callback)
            self._subscriptions[topic.value] = task

        return wrapper

    def _handle_records(self, records: {TopicPartition: [ConsumerRecord]}):
        for topic_partition in records.keys():
            topic = topic_partition.topic
            topic_records = records[topic_partition]
            task = self._subscriptions[topic]
            task.records = topic_records
            self._executor.submit(task)
            self._runningTasks[topic] = task
            self._internal_consumer.pause(topic_partition)

    def _update_tasks_status(self):
        offsets = {}
        topics_to_resume = []

        for topic, task in self._runningTasks.items():
            if task.get_offset() > 0:
                for partition in self._internal_consumer.partitions_for_topic(topic):
                    topic_partition = TopicPartition(topic, partition)
                    offsets[topic_partition] = OffsetAndMetadata(task.get_offset(), topic_partition)
            if not task.is_running():
                for partition in self._internal_consumer.partitions_for_topic(topic):
                    topic_partition = TopicPartition(topic, partition)
                    topics_to_resume.append(topic_partition)

        self._internal_consumer.commit_async(offsets)
        for topic_partition in topics_to_resume:
            self._runningTasks.pop(topic_partition.topic)
            self._internal_consumer.resume(topic_partition)

    def consume(self):
        try:
            self._internal_consumer.subscribe(list(self._subscriptions.keys()))
            while True:
                records = self._internal_consumer.poll(500)
                if records is None:
                    continue

                self._handle_records(records)
                self._update_tasks_status()
        finally:
            del self
