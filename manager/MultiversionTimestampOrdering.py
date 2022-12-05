from typing import List
import math

from manager.ConcurrencyControl import ConcurrencyControl
from process.Transaction import Transaction
from process.Query import Query
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
    def __init__(self, filename: str) -> None:
        self.filename: str = filename

    def read(self, listOfVersion: List[Multiversion], timestamp: int) -> int:
        if len(listOfVersion) == 0:
            file = open(self.filename, "rb")
            content = file.read()
            file.close()

            result = int.from_bytes(content, byteorder="big", signed=False)

            multiversion = Multiversion()
            multiversion.setValue(result)
            multiversion.setWriteTimestamp(0)
            multiversion.setReadTimestamp(timestamp)
            listOfVersion.append(multiversion)

            return result

        else:
            index = 0

            for i in range(len(listOfVersion)):
                if listOfVersion[i].writeTimestamp <= timestamp:
                    index = i
                else:
                    break

            if listOfVersion[index].readTimestamp < timestamp:
                listOfVersion[index].readTimestamp = timestamp

            return listOfVersion[index].getValue()

    def write(self, value: int, listOfVersion: List[Multiversion], timestamp: int) -> None:
        if len(listOfVersion) == 0:
            multiversion = Multiversion()
            multiversion.setValue(value)
            multiversion.setWriteTimestamp(timestamp)
            multiversion.setReadTimestamp(0)
            listOfVersion.append(multiversion)

            file = open(self.filename, "wb")

            length = math.ceil(value / (1 << 8))
            content = value.to_bytes(length, byteorder="big", signed=False)
            file.write(content)

            file.close()
        else:
            index = 0

            for i in range(len(listOfVersion)):
                if listOfVersion[i].writeTimestamp <= timestamp:
                    index = i
                else:
                    break

            if timestamp < listOfVersion[index].readTimestamp:
                return -1
            elif timestamp == listOfVersion[index].readTimestamp:
                listOfVersion[index] = value
            else:
                multiversion = Multiversion()
                multiversion.setValue(value)
                multiversion.setWriteTimestamp(timestamp)
                multiversion.setReadTimestamp(0)
                listOfVersion.append(multiversion)


class MultiversionControl(ConcurrencyControl):
    pass
