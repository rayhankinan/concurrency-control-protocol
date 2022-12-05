from __future__ import annotations
from typing import List, Set
import inspect
import sys

from manager.ConcurrencyControl import ConcurrencyControl
from process.Transaction import Transaction
from process.Query import Query, ReadQuery, WriteQuery, DisplayQuery, FunctionQuery


class SerialOptimisticTransaction(Transaction):
    def __init__(self, startTimestamp: int, listOfQuery: List[Query]) -> None:
        super().__init__(startTimestamp, listOfQuery)

        # Menambahkan end timestamp
        self.endTimestamp: int = sys.maxsize

        # Mencatat data items written dan data items read
        self.dataItemWritten: Set[str] = set()
        self.dataItemRead: Set[str] = set()

    def validationTest(self, counterTimestamp: int, other: SerialOptimisticTransaction) -> bool:
        if self.startTimestamp <= other.startTimestamp:
            return True
        if self.startTimestamp >= other.endTimestamp:
            return True
        if self.startTimestamp < other.endTimestamp and counterTimestamp >= other.endTimestamp and self.dataItemRead.intersection(other.dataItemWritten) == set():
            return True
        return False

    def nextQuery(self) -> None:
        currentQuery = self.getCurrentQuery()
        if type(currentQuery) is WriteQuery:
            for filename in currentQuery.getFileNames():
                self.dataItemWritten.add(filename)
                print(f"[WRITE: {filename}]")

        if type(currentQuery) is ReadQuery:
            for filename in currentQuery.getFileNames():
                self.dataItemRead.add(filename)
                print(f"[READ: {filename}]")

        if type(currentQuery) is DisplayQuery:
            print(
                f"[DISPLAY: {inspect.getsource(currentQuery.function).replace(',', '').strip()}]")

        if type(currentQuery) is FunctionQuery:
            print(
                f"[FUNCTION: {inspect.getsource(currentQuery.function).replace(',', '').strip()}]")

        super().nextQuery()

    def rollback(self, newTimestamp: int) -> None:
        self.dataItemWritten = set()
        self.dataItemRead = set()

        super().rollback(newTimestamp)


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
                print(f"[BEGIN TRANSACTION {currentTimestamp}]")

            transaction = self.getTransaction(currentTimestamp)

            if transaction.isFinished():
                valid = all(transaction.validationTest(
                    currentTimestamp +
                    counter, self.getTransaction(timestamp)
                ) for timestamp in activeTimestamp)

                if valid:
                    print(
                        f"[COMMIT TRANSACTION {transaction.getStartTimestamp()}]")
                    print("RESULT:")
                    transaction.commit()
                    transaction.endTimestamp = currentTimestamp + counter

                else:
                    tempSchedule = list(filter(
                        lambda X: X != currentTimestamp, tempSchedule
                    ))
                    activeTimestamp = list(filter(
                        lambda X: X != currentTimestamp, activeTimestamp
                    ))
                    newTimestamp = currentTimestamp + \
                        counter + len(activeTimestamp)

                    print(f"[ROLLBACK TRANSACTION {currentTimestamp}]")
                    transaction.rollback(newTimestamp)

                    tempSchedule.extend(
                        newTimestamp for _ in range(transaction.getLength()))
            else:
                transaction.nextQuery()

            counter += 1
