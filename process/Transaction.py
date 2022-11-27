from typing import List
from Query import Query


class Transaction:
    def __init__(self, id: int, listOfQuery: List[Query]) -> None:
        self.id = id
        self.listOfQuery = listOfQuery

    def getId(self):
        return self.id

    def getLength(self) -> int:
        return len(self.listOfQuery)

    def getQuery(self, index: int) -> Query:
        return self.listOfQuery[index]
