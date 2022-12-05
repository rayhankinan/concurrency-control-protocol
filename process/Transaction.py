from typing import Dict, List

from process.Query import Query
from process.Data import Data


class Transaction:
    def __init__(self, startTimestamp: int, listOfQuery: List[Query]) -> None:
        self.startTimestamp = startTimestamp
        self.listOfQuery = listOfQuery
        self.queryIndex = 0
        self.dictData: Dict[str, Data] = dict()

    def getStartTimestamp(self):
        return self.startTimestamp

    def getLength(self) -> int:
        return len(self.listOfQuery)

    def getCurrentQuery(self) -> Query:
        return self.listOfQuery[self.queryIndex]

    def isFinished(self) -> bool:
        return self.queryIndex == self.getLength()

    def nextQuery(self) -> None:
        self.queryIndex += 1

    def rollback(self, newTimestamp) -> None:
        self.queryIndex = 0
        self.startTimestamp = newTimestamp

    def commit(self) -> None:
        for i in range(self.getLength()):
            currentQuery = self.listOfQuery[i]
            currentFileNames = currentQuery.getFileNames()
            currentData: List[Data] = []

            for filename in currentFileNames:
                currentData.append(self.dictData.setdefault(filename, Data()))

            currentQuery.execute(*currentData)
