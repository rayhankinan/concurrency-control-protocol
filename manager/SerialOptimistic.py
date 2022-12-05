from __future__ import annotations
from typing import List, Set
import sys

from manager.ConcurrencyControl import ConcurrencyControl
from process.Transaction import Transaction
from process.Query import Query, ReadQuery, WriteQuery, FunctionQuery, DisplayQuery


class SerialOptimisticTransaction(Transaction):
    def __init__(self, startTimestamp: int, listOfQuery: List[Query]) -> None:
        super().__init__(startTimestamp, listOfQuery)

        # Menambahkan end timestamp
        self.endTimestamp: int = sys.maxsize

        # Mencatat data items written dan data items read
        self.dataItemWritten: Set[str] = set()
        self.dataItemRead: Set[str] = set()

    # TODO: Rollback Bug
    def validationTest(self, counterTimestamp: int, other: SerialOptimisticTransaction) -> bool:
        if self.startTimestamp <= other.startTimestamp:
            return True
        if self.startTimestamp >= other.endTimestamp:
            return True
        if self.startTimestamp < other.endTimestamp and counterTimestamp >= other.endTimestamp and self.dataItemRead.intersection(other.dataItemWritten) == set():
            return True
        return False

    def nextQuery(self) -> None:
        if type(self.getCurrentQuery()) is WriteQuery:
            for filename in self.getCurrentQuery().getFileNames():
                self.dataItemWritten.add(filename)
        if type(self.getCurrentQuery()) is ReadQuery:
            for filename in self.getCurrentQuery().getFileNames():
                self.dataItemRead.add(filename)

        super().nextQuery()

    def rollback(self, newTimestamp: int) -> None:
        super().rollback(newTimestamp)

        self.dataItemWritten = set()
        self.dataItemRead = set()


class SerialOptimisticControl(ConcurrencyControl):
    def __init__(self, listOfTransaction: List[SerialOptimisticTransaction], schedule: List[int]) -> None:
        super().__init__(listOfTransaction, schedule)

    def getStartTransaction(self, startTimestamp: int) -> SerialOptimisticTransaction:
        return super().getTransaction(startTimestamp)

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

                    newTimestamp = currentTimestamp + \
                        counter + len(activeTimestamp)
                    transaction.rollback(newTimestamp)
                    tempSchedule.extend(
                        newTimestamp for _ in range(transaction.getLength())
                    )
                else:
                    transaction.nextQuery()
            else:
                transaction.nextQuery()

            if transaction.isFinished():
                transaction.commit()
                transaction.endTimestamp = currentTimestamp + counter

            counter += 1
