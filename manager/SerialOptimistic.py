from __future__ import annotations
from typing import List, Set

from manager.ConcurrencyControl import ConcurrencyControl
from process.Transaction import Transaction
from process.Query import Query, ReadQuery, WriteQuery, FunctionQuery, DisplayQuery


class SerialOptimisticTransaction(Transaction):
    def __init__(self, timestamp: int, listOfQuery: List[Query]) -> None:
        super().__init__(timestamp, listOfQuery)

        # Mencatat data items written dan data items read
        self.dataItemWritten: Set[str] = set()
        self.dataItemRead: Set[str] = set()

        # Melakukan sorting pada list of query sehingga read query dan function query di depan serta write query dan display query di belakang
        for i in range(1, self.getLength()):
            j = i - 1
            key = self.listOfQuery[i]

            while j >= 0 and (type(key) is ReadQuery or type(key) is FunctionQuery) and (type(self.listOfQuery[j]) is WriteQuery or type(self.listOfQuery[j]) is DisplayQuery):
                self.listOfQuery[j + 1] = self.listOfQuery[j]
                j -= 1
            self.listOfQuery[j + 1] = key

    def validationTest(self, other: SerialOptimisticTransaction) -> bool:
        if self.getTimestamp() > other.getTimestamp():
            if self.dataItemRead.intersection(other.dataItemWritten):
                return False
            else:
                return True
        else:
            return True

    def executeCurrentQuery(self) -> None:
        if self.getCurrentQuery() is WriteQuery:
            for filename in self.getCurrentQuery().getFileNames():
                self.dataItemWritten.add(filename)
        if self.getCurrentQuery() is ReadQuery:
            for filename in self.getCurrentQuery().getFileNames():
                self.dataItemRead.add(filename)

        super().executeCurrentQuery()


class SerialOptimisticControl(ConcurrencyControl):
    def run():
        pass
