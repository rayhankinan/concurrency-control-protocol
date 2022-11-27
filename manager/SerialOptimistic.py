from __future__ import annotations
from typing import List, Set
import sys

from manager.ConcurrencyControl import ConcurrencyControl
from process.Transaction import Transaction
from process.Query import Query, ReadQuery, WriteQuery, FunctionQuery, DisplayQuery


class SerialOptimisticTransaction(Transaction):
    def __init__(self, timestamp: int, listOfQuery: List[Query]) -> None:
        super().__init__(timestamp, listOfQuery)

        # Menambahkan end timestamp
        self.endTimestamp: int = sys.maxsize

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

    def getEnd(self) -> int:
        return self.endTimestamp

    # TODO: Rollback Bug
    def validationTest(self, counterTimestamp: int, other: SerialOptimisticTransaction) -> bool:
        if self.timestamp <= other.timestamp:
            return True
        if self.timestamp >= other.endTimestamp:
            return True
        if self.timestamp < other.endTimestamp and counterTimestamp >= other.endTimestamp and self.dataItemRead.intersection(other.dataItemWritten) == set():
            return True
        return False

    def executeCurrentQuery(self) -> None:
        if type(self.getCurrentQuery()) is WriteQuery:
            for filename in self.getCurrentQuery().getFileNames():
                self.dataItemWritten.add(filename)
        if type(self.getCurrentQuery()) is ReadQuery:
            for filename in self.getCurrentQuery().getFileNames():
                self.dataItemRead.add(filename)

        super().executeCurrentQuery()

    def reset(self, newTimestamp: int) -> None:
        super().reset(newTimestamp)
        self.dataItemWritten = set()
        self.dataItemRead = set()


class SerialOptimisticControl(ConcurrencyControl):
    def __init__(self, listOfTransaction: List[SerialOptimisticTransaction], schedule: List[int]) -> None:
        super().__init__(listOfTransaction, schedule)

    def getTransaction(self, timestamp: int) -> SerialOptimisticTransaction:
        return super().getTransaction(timestamp)

    def run(self):
        tempSchedule: List[int] = [timestamp for timestamp in self.schedule]
        activeTimestamp: List[int] = []
        counter = 0

        while tempSchedule:
            currentTimestamp = tempSchedule.pop(0)

            if currentTimestamp not in activeTimestamp:
                activeTimestamp.append(currentTimestamp)

            transaction = self.getTransaction(currentTimestamp)
            query = transaction.getCurrentQuery()

            if type(query) is WriteQuery or type(query) is DisplayQuery:
                valid = True
                for timestamp in activeTimestamp:
                    valid = valid and transaction.validationTest(
                        currentTimestamp +
                        counter, self.getTransaction(timestamp)
                    )

                if not valid:
                    print(f"ROLLBACK : {currentTimestamp}")  # DELETE THIS
                    tempSchedule = list(filter(
                        lambda X: X != currentTimestamp, tempSchedule
                    ))
                    activeTimestamp = list(filter(
                        lambda X: X != currentTimestamp, activeTimestamp
                    ))

                    newTimestamp = currentTimestamp + counter
                    transaction.reset(newTimestamp)
                    tempSchedule.extend(
                        newTimestamp for _ in range(transaction.getLength())
                    )
                else:
                    transaction.executeCurrentQuery()
            else:
                transaction.executeCurrentQuery()

            if transaction.isFinishedExecuting():
                transaction.endTimestamp = currentTimestamp + counter

            counter += 1
