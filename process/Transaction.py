from typing import Dict, List
from process.Query import Query
from process.Data import Data


class Transaction:
    def __init__(self, id: int, listOfQuery: List[Query]) -> None:
        self.id = id
        self.listOfQuery = listOfQuery
        self.queryIndex = 0
        self.dictData: Dict[str, Data] = dict()

    def getId(self):
        return self.id

    def getLength(self) -> int:
        return len(self.listOfQuery)

    def getCurrentQuery(self) -> Query:
        return self.listOfQuery[self.queryIndex]

    def executeCurrentQuery(self) -> None:
        currentQuery = self.listOfQuery[self.queryIndex]
        currentFileNames = currentQuery.getFileNames()
        currentData: List[Data] = []

        for filename in currentFileNames:
            currentData.append(self.dictData.setdefault(filename, Data()))

        currentQuery.execute(*currentData)
        self.queryIndex += 1
