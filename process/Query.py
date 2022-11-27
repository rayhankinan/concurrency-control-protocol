from typing import Callable, List, Tuple
from process.FileAccess import FileAccess
from process.Data import Data


class Query:
    def __init__(self, *args: str) -> None:
        self.fileNames: List[str] = []
        self.fileAccesses: List[FileAccess] = []

        for filename in args:
            self.fileNames.append(filename)
            self.fileAccesses.append(FileAccess(filename))

    def getFileNames(self) -> List[str]:
        return self.fileNames

    def execute(self) -> None:
        raise NotImplementedError()


class ReadQuery(Query):
    def __init__(self, *args: str) -> None:
        super().__init__(*args)

    def execute(self, *args: Data) -> None:
        for i in range(len(args)):
            data = args[i]
            fileAccess = self.fileAccesses[i]
            data.setValue(fileAccess.read())


class WriteQuery(Query):
    def __init__(self, *args: str) -> None:
        super().__init__(*args)

    def execute(self, *args: Data) -> None:
        for i in range(len(args)):
            data = args[i]
            fileAccess = self.fileAccesses[i]
            fileAccess.write(data.getValue())


class FunctionQuery(Query):
    def __init__(self, *args: str, **kwargs: Callable[..., int]) -> None:
        super().__init__(*args)
        self.function = kwargs.get("function", lambda *X: X[0])

    def execute(self, *args: Data) -> None:
        value: List[int] = []
        for data in args:
            value.append(data.getValue())
        args[0].setValue(self.function(*value))


class DisplayQuery(Query):
    def __init__(self, *args: str, **kwargs: Callable[..., int]) -> None:
        super().__init__(*args)
        self.function = kwargs.get(
            "function", lambda *X: X[0] if len(X) == 1 else X)

    def execute(self, *args: Data) -> None:
        value: List[int] = []
        for data in args:
            value.append(data.getValue())
        print(self.function(*value))
