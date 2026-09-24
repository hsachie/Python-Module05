from abc import ABC, abstractmethod
from typing import any

class DataProcessor(ABC):
	def __init__(self):

	@abstractmethod
	def validate(self, data: Any) -> bool:

	@abstractmethod
	def inget(self, data: Any) -> None:

	def output(self) -> tuple[int, str]:
		
