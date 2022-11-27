from typing import List
from process.Transaction import Transaction


class ConcurrencyControl:
    def __init__(self, listOfTransaction: List[Transaction], schedule: List[int]) -> None:
        self.listOfTransaction = listOfTransaction
        self.schedule = schedule

    def getTransaction(self, timestamp: int) -> Transaction:
        return next(transaction for transaction in self.listOfTransaction if transaction.timestamp == timestamp)

    def run(self):
        raise NotImplementedError()
