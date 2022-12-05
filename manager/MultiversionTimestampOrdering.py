from typing import Callable, Dict, List
import inspect
import math

from manager.ConcurrencyControl import ConcurrencyControl
from process.Transaction import Transaction
from process.Query import Query, ReadQuery, WriteQuery, DisplayQuery, FunctionQuery
from process.FileAccess import FileAccess
from process.Data import Data


class Multiversion(Data):
    def __init__(self) -> None:
        self.value: int = 0
        self.writeTimestamp: int = 0
        self.readTimestamp: int = 0

    def getValue(self) -> int:
        return self.value

    def setValue(self, value: int) -> None:
        self.value = value

    def getWriteTimestamp(self) -> None:
        return self.writeTimestamp

    def setWriteTimestamp(self, writeTimestamp: int):
        self.writeTimestamp = writeTimestamp

    def getReadTimestamp(self) -> int:
        return self.readTimestamp

    def setReadTimestamp(self, readTimestamp: int) -> None:
        self.readTimestamp = readTimestamp


class MultiversionAccess(FileAccess):
    listOfVersion: Dict[str, List[Multiversion]] = dict()

    def __init__(self, filename: str) -> None:
        self.filename: str = filename
        self.initialValue = super().read()

    def read(self, timestamp: int) -> int:
        if len(MultiversionAccess.listOfVersion[self.filename]) == 0:
            multiversion = Multiversion()
            multiversion.setValue(self.initialValue)
            multiversion.setWriteTimestamp(0)
            multiversion.setReadTimestamp(timestamp)
            MultiversionAccess.listOfVersion[self.filename].append(
                multiversion
            )

            return self.initialValue

        else:
            index = 0

            for i in range(len(MultiversionAccess.listOfVersion[self.filename])):
                if MultiversionAccess.listOfVersion[self.filename][i].writeTimestamp <= timestamp:
                    index = i
                else:
                    break

            if MultiversionAccess.listOfVersion[self.filename][index].readTimestamp < timestamp:
                MultiversionAccess.listOfVersion[self.filename][index].readTimestamp = timestamp

            return MultiversionAccess.listOfVersion[self.filename][index].getValue()

    def write(self, value: int, timestamp: int) -> None:
        if len(MultiversionAccess.listOfVersion[self.filename]) == 0:
            multiversion = Multiversion()
            multiversion.setValue(value)
            multiversion.setWriteTimestamp(timestamp)
            multiversion.setReadTimestamp(0)
            MultiversionAccess.listOfVersion[self.filename].append(
                multiversion
            )

        else:
            index = 0

            for i in range(len(MultiversionAccess.listOfVersion[self.filename])):
                if MultiversionAccess.listOfVersion[self.filename][i].writeTimestamp <= timestamp:
                    index = i
                else:
                    break

            if timestamp < MultiversionAccess.listOfVersion[self.filename][index].readTimestamp:
                return -1
            elif timestamp == MultiversionAccess.listOfVersion[self.filename][index].readTimestamp:
                MultiversionAccess.listOfVersion[self.filename][index] = value
            else:
                multiversion = Multiversion()
                multiversion.setValue(value)
                multiversion.setWriteTimestamp(timestamp)
                multiversion.setReadTimestamp(0)
                MultiversionAccess.listOfVersion[self.filename].append(
                    multiversion
                )

    def commit(self):
        if len(MultiversionAccess.listOfVersion[self.filename]) == 0:
            # Do nothing
            pass
        else:
            multiversion = MultiversionAccess.listOfVersion[self.filename][-1]
            value = multiversion.getValue()
            super().write(value)


class MultiversionReadQuery(ReadQuery):
    def __init__(self, *args: str) -> None:
        super().__init__(*args)

        self.multiversionAccesses: List[MultiversionAccess] = []

        for filename in args:
            self.multiversionAccesses.append(MultiversionAccess(filename))

    def execute(self, timestamp: int, *args: Data) -> None:
        for i in range(len(args)):
            data = args[i]
            multiversionAccess = self.multiversionAccesses[i]
            data.setValue(multiversionAccess.read(timestamp))


class MultiversionWriteQuery(WriteQuery):
    def __init__(self, *args: str) -> None:
        super().__init__(*args)

        self.multiversionAccesses: List[MultiversionAccess] = []

        for filename in args:
            self.multiversionAccesses.append(MultiversionAccess(filename))

    def execute(self, timestamp: int, *args: Data) -> None:
        for i in range(len(args)):
            data = args[i]
            multiversionAccess = self.multiversionAccesses[i]
            multiversionAccess.write(data.getValue(), timestamp)


class MultiversionTransaction(Transaction):
    pass


class MultiversionControl(ConcurrencyControl):
    pass
