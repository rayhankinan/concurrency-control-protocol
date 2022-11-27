from typing import Callable
from FileAccess import FileAccess
from Data import Data


class Query:
    def __init__(self, filename: str) -> None:
        self.fileAccess = FileAccess(filename)

    def execute(self) -> None:
        raise NotImplementedError()


class ReadQuery(Query):
    def __init__(self, filename: str) -> None:
        super().__init__(filename)

    def execute(self, data: Data) -> None:
        data.setValue(self.fileAccess.read())


class WriteQuery(Query):
    def __init__(self, filename: str) -> None:
        super().__init__(filename)

    def execute(self, data: Data) -> None:
        self.fileAccess.write(data.getValue())


class FunctionQuery(Query):
    def __init__(self, filename: str, function: Callable[[int], int]) -> None:
        super().__init__(filename)
        self.function = function

    def execute(self, data: Data) -> None:
        data.setValue(self.function(data.getValue()))
