from typing import Callable

from kafka.consumer.fetcher import ConsumerRecord


class Task(Callable[[ConsumerRecord], None]):

    def __init__(self, callback: Callable[[ConsumerRecord], None]):
        self._callback = callback
        self._running = False
        self._offset = -1

        self.records: [ConsumerRecord] = []

    def __call__(self) -> None:
        self._running = True
        for record in self.records:
            self._callback(record)
            self._offset = record.offset + 1
        self._running = False

    def is_running(self) -> bool:
        return self._running

    def get_offset(self) -> int:
        return self._offset
