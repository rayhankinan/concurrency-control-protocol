from typing import Dict, List

from process.Query import Query
from process.Data import Data


class Transaction:
    def __init__(self, timestamp: int, listOfQuery: List[Query]) -> None:
        self.timestamp = timestamp
        self.listOfQuery = listOfQuery
        self.queryIndex = 0
        self.dictData: Dict[str, Data] = dict()

    def getTimestamp(self):
        return self.timestamp

    def getLength(self) -> int:
        return len(self.listOfQuery)

    def getCurrentQuery(self) -> Query:
        return self.listOfQuery[self.queryIndex]

    def isFinishedExecuting(self) -> bool:
        return self.queryIndex == self.getLength()

    def executeCurrentQuery(self) -> None:
        currentQuery = self.listOfQuery[self.queryIndex]
        currentFileNames = currentQuery.getFileNames()
        currentData: List[Data] = []

        for filename in currentFileNames:
            currentData.append(self.dictData.setdefault(filename, Data()))

        currentQuery.execute(*currentData)
        self.queryIndex += 1

    def resetIndex(self) -> None:
        self.queryIndex = 0
