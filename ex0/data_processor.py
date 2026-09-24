from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self, rank: int):
        self.rank = rank

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:


class NumericProcessor(DataProcessor):


class TextProcessor(DataProcessor):


class class LogProcessor(DataProcessor):


if __name__ == "__main__":
