from abc import ABC, abstractmethod
from typing import Any, Union


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.rank: int = 0
        self.data: list[str] = []

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.data:
            raise IndexError("No data available")
        value = self.data.pop(0)
        rank = self.rank
        self.rank += 1

        return rank, value


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True

        if isinstance(data, list):
            return all(
                isinstance(item, (int, float)) for item in data)
        return False

    def ingest(
        self,
        data: Union[int, float, list[int], list[float],
                    list[Union[int, float]]]
    ) -> None:
        if not self.validate(data):
            raise TypeError("Improper numeric data")
        if isinstance(data, list):
            for item in data:
                self.data.append(str(item))
        else:
            self.data.append(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True

        if isinstance(data, list):
            return all(
                isinstance(item, str) for item in data)
        return False

    def ingest(self, data: Union[str, list[str]]) -> None:
        if not self.validate(data):
            raise TypeError("Improper text data")

        if isinstance(data, list):
            for item in data:
                self.data.append(item)
        else:
            self.data.append(data)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return all(isinstance(key, str) and isinstance(value, str)
                       for key, value in data.items())

        if isinstance(data, list):
            return all(
                isinstance(item, dict)
                and all(
                    isinstance(key, str) and isinstance(value, str)
                    for key, value in item.items()
                )
                for item in data
            )
        return False

    def ingest(
        self, data: Union[dict[str, str], list[dict[str, str]]]
    ) -> None:
        if not self.validate(data):
            raise TypeError("Improper log data")
        if isinstance(data, list):
            for item in data:
                self.data.append(
                    f"{item['log_level']}: "
                    f"{item['log_message']}"
                )
        else:
            self.data.append(
                f"{data['log_level']}: "
                f"{data['log_message']}"
            )


def main() -> None:
    print("=== Code Nexus - Data Processor ===")
    print()
    print("Testing Numeric Processor...")
    Numeric = NumericProcessor()
    print(f"Trying to validate input '42': {Numeric.validate(42)}")
    print(f"Trying to validate input 'Hello': {Numeric.validate('Hello')}")
    print("Test invalid ingestion of string 'foo' without prior validation:")

    try:
        Numeric.ingest("foo")
    except TypeError as err:
        print(f"ot exception: {err}")

    print("Processing data: [1, 2, 3, 4, 5]")
    Numeric.ingest([1, 2, 3, 4, 5])
    print("Extracting 3 values...")
    for _ in range(3):
        rank, value = Numeric.output()
        print(f"Numeric value {rank}: {value}")    

    print()
    print("Testing Text Processor...")
    Text = TextProcessor()
    print(f"Trying to validate input '42': {Text.validate(42)}")
    print("Processing data: ['Hello', 'Nexus', 'World']")
    Text.ingest(["Hello", "Nexus", "World"])
    print("Extracting 1 value...")
    rank, value = Text.output()
    print(f"Text value {rank}: {value}")

    print()
    print("Testing Log Processor...")
    Log = LogProcessor()
    print(f"Trying to validate input 'Hello': {Log.validate('Hello')}")
    
    Log_data = [
    {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
    {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
	]
    print(f"Processing data: {Log_data}")
    Log.ingest(Log_data)
    print("Extracting 2 values...")

    for _ in range(2):
        rank, value = Log.output()
        print(f"Log entry {rank}: {value}")

if __name__ == "__main__":
    main()