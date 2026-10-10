from abc import ABC, abstractmethod
from typing import Any, Union


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.rank: int = 0
        self.data: list[str] = []
        self.total: int = 0
        self.name: str = "DataProcessor"

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
    def __init__(self) -> None:
        super().__init__()
        self.name = "Numeric Processor"

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
                self.total += 1
        else:
            self.data.append(str(data))
            self.total += 1


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.name = "Text Processor"

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
                self.total += 1
        else:
            self.data.append(data)
            self.total += 1


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.name = "Log Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return ("log_level" in data
                    and "log_message" in data
                    and all(isinstance(key, str) and isinstance(value, str)
                            for key, value in data.items()))

        if isinstance(data, list):
            return all(
                isinstance(item, dict)
                and "log_level" in item
                and "log_message" in item
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
                self.total += 1
        else:
            self.data.append(
                f"{data['log_level']}: "
                f"{data['log_message']}"
            )
            self.total += 1


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            processed = False
            for proc in self.processors:
                if proc.validate(element):
                    proc.ingest(element)
                    processed = True
                    break
            if not processed:
                print("DataStream error - "
                      f"Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
        for proc in self.processors:
            print(f"{proc.name}: total {proc.total} items processed, "
                  f"remaining {len(proc.data)} on processor")


def main() -> None:
    print("=== Code Nexus - Data Stream ===")
    print()
    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print()
    print("Registering Numeric Processor")
    numeric = NumericProcessor()
    stream.register_processor(numeric)

    batch: list[Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {"log_level": "WARNING",
             "log_message": "Telnet access! Use ssh instead"},
            {"log_level": "INFO",
             "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]

    print()
    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print()
    print("Registering other data processors")
    text = TextProcessor()
    log = LogProcessor()
    stream.register_processor(text)
    stream.register_processor(log)

    print("Send the same batch again")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print()
    print("Consume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")
    for _ in range(3):
        numeric.output()
    for _ in range(2):
        text.output()
    log.output()
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
