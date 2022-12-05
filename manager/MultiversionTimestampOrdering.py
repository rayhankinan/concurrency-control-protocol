from typing import Callable, Dict, List

from manager.ConcurrencyControl import ConcurrencyControl
from process.Transaction import Transaction
from process.Query import Query, ReadQuery, WriteQuery, DisplayQuery, FunctionQuery
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
    listOfVersion: Dict[str, List[Multiversion]] = dict()

    def __init__(self, filename: str) -> None:
        self.filename: str = filename
        self.initialValue = super().read()

    def read(self, timestamp: int) -> int:
        if len(MultiversionAccess.listOfVersion.setdefault(self.filename, [])) == 0:
            multiversion = Multiversion()
            multiversion.setValue(self.initialValue)
            multiversion.setWriteTimestamp(0)
            multiversion.setReadTimestamp(timestamp)
            MultiversionAccess.listOfVersion[self.filename].append(
                multiversion
            )

            return self.initialValue

        else:
            index = 0
            for i in range(len(MultiversionAccess.listOfVersion[self.filename])):
                if MultiversionAccess.listOfVersion[self.filename][i].writeTimestamp <= timestamp:
                    index = i
                else:
                    break

            if MultiversionAccess.listOfVersion[self.filename][index].readTimestamp < timestamp:
                MultiversionAccess.listOfVersion[self.filename][index].readTimestamp = timestamp

            return MultiversionAccess.listOfVersion[self.filename][index].getValue()

    def write(self, value: int, timestamp: int) -> None:
        if len(MultiversionAccess.listOfVersion.setdefault(self.filename, [])) == 0:
            multiversion = Multiversion()
            multiversion.setValue(value)
            multiversion.setWriteTimestamp(timestamp)
            multiversion.setReadTimestamp(0)
            MultiversionAccess.listOfVersion[self.filename].append(
                multiversion
            )

        else:
            index = 0
            for i in range(len(MultiversionAccess.listOfVersion[self.filename])):
                if MultiversionAccess.listOfVersion[self.filename][i].writeTimestamp <= timestamp:
                    index = i
                else:
                    break

            if timestamp < MultiversionAccess.listOfVersion[self.filename][index].readTimestamp:
                raise Exception("ROLLBACK")

            elif timestamp == MultiversionAccess.listOfVersion[self.filename][index].writeTimestamp:
                MultiversionAccess.listOfVersion[self.filename][index].setValue(
                    value
                )

            else:
                multiversion = Multiversion()
                multiversion.setValue(value)
                multiversion.setWriteTimestamp(timestamp)
                multiversion.setReadTimestamp(0)
                MultiversionAccess.listOfVersion[self.filename].append(
                    multiversion
                )

    def commit(self, timestamp: int):
        if len(MultiversionAccess.listOfVersion.setdefault(self.filename, [])) == 0:
            # Do nothing
            pass
        else:
            index = 0
            for i in range(len(MultiversionAccess.listOfVersion[self.filename])):
                if MultiversionAccess.listOfVersion[self.filename][i].writeTimestamp <= timestamp:
                    index = i
                else:
                    break

            multiversion = MultiversionAccess.listOfVersion[self.filename][index]
            value = multiversion.getValue()
            super().write(value)


class MultiversionQuery(Query):
    def execute(self, timestamp: int, *args: Data) -> None:
        raise NotImplementedError()

    def commit(self, timestamp: int, *args: Data):
        raise NotImplementedError()


class MultiversionReadQuery(MultiversionQuery, ReadQuery):
    def __init__(self, *args: str) -> None:
        super().__init__(*args)

        self.multiversionAccesses: List[MultiversionAccess] = []

        for filename in args:
            self.multiversionAccesses.append(MultiversionAccess(filename))

    def execute(self, timestamp: int, *args: Data) -> None:
        for i in range(len(args)):
            data = args[i]
            multiversionAccess = self.multiversionAccesses[i]
            data.setValue(multiversionAccess.read(timestamp))
            print("[READ]")

    def commit(self, timestamp: int, *args: Data):
        for i in range(len(args)):
            data = args[i]
            multiversionAccess = self.multiversionAccesses[i]
            data.setValue(multiversionAccess.read(timestamp))


class MultiversionWriteQuery(MultiversionQuery, WriteQuery):
    def __init__(self, *args: str) -> None:
        super().__init__(*args)

        self.multiversionAccesses: List[MultiversionAccess] = []

        for filename in args:
            self.multiversionAccesses.append(MultiversionAccess(filename))

    def execute(self, timestamp: int, *args: Data) -> None:
        for i in range(len(args)):
            data = args[i]
            multiversionAccess = self.multiversionAccesses[i]
            multiversionAccess.write(data.getValue(), timestamp)
            print("[WRITE]")

    def commit(self, timestamp: int, *args: Data):
        for i in range(len(args)):
            data = args[i]
            multiversionAccess = self.multiversionAccesses[i]
            multiversionAccess.write(data.getValue(), timestamp)
            multiversionAccess.commit(timestamp)


class MultiversionFunctionQuery(MultiversionQuery, FunctionQuery):
    def __init__(self, *args: str, **kwargs: Callable[..., int]) -> None:
        super().__init__(*args)
        self.function = kwargs.get("function", lambda *args: args[0])

    def execute(self, timestamp: int, *args: Data) -> None:
        value: List[int] = []
        for data in args:
            value.append(data.getValue())
        args[0].setValue(self.function(*value))

        print("[FUNCTION]")

    def commit(self, timestamp: int, *args: Data) -> None:
        value: List[int] = []
        for data in args:
            value.append(data.getValue())
        args[0].setValue(self.function(*value))


class MultiversionDisplayQuery(MultiversionQuery, DisplayQuery):
    def __init__(self, *args: str, **kwargs: Callable[..., int]) -> None:
        super().__init__(*args)
        self.function = kwargs.get("function", lambda *args: args)

    def execute(self, timestamp: int, *args: Data) -> None:
        print("[DISPLAY]")

    def commit(self, timestamp: int, *args: Data) -> None:
        value: List[int] = []
        for data in args:
            value.append(data.getValue())

        result = self.function(*value)
        if type(result) is tuple:
            print(" ".join(map(str, result)))
        if type(result) is int:
            print(result)


class MultiversionTransaction(Transaction):
    def __init__(self, startTimestamp: int, listOfQuery: List[Query]) -> None:
        newListOfQuery: List[Query] = []

        for query in listOfQuery:
            if type(query) is ReadQuery:
                newListOfQuery.append(
                    MultiversionReadQuery(*query.getFileNames())
                )
            elif type(query) is WriteQuery:
                newListOfQuery.append(
                    MultiversionWriteQuery(*query.getFileNames())
                )
            elif type(query) is FunctionQuery:
                newListOfQuery.append(
                    MultiversionFunctionQuery(
                        *query.getFileNames(), function=query.function)
                )
            elif type(query) is DisplayQuery:
                newListOfQuery.append(
                    MultiversionDisplayQuery(
                        *query.getFileNames(), function=query.function)
                )
            else:
                raise Exception("TYPE INVALID")

        super().__init__(startTimestamp, newListOfQuery)

    def nextQuery(self) -> None:
        currentQuery: MultiversionQuery = self.getCurrentQuery()
        currentFileNames = currentQuery.getFileNames()
        currentData: List[Data] = []

        for filename in currentFileNames:
            currentData.append(self.dictData.setdefault(filename, Data()))

        currentQuery.execute(self.getStartTimestamp(), *currentData)

        super().nextQuery()

    def commit(self) -> None:
        MultiversionAccess.listOfVersion.clear()

        for i in range(self.getLength()):
            currentQuery: MultiversionQuery = self.listOfQuery[i]
            currentFileNames = currentQuery.getFileNames()
            currentData: List[Data] = []

            for filename in currentFileNames:
                currentData.append(self.dictData.setdefault(filename, Data()))

            currentQuery.commit(self.getStartTimestamp(), *currentData)


class MultiversionControl(ConcurrencyControl):
    def __init__(self, listOfTransaction: List[MultiversionTransaction], schedule: List[int]) -> None:
        super().__init__(listOfTransaction, schedule)

    def run(self):
        tempSchedule: List[int] = [timestamp for timestamp in self.schedule]
        activeTimestamp: List[int] = []
        maxTimestamp = 0

        while tempSchedule:
            currentTimestamp = tempSchedule.pop(0)

            if currentTimestamp not in activeTimestamp:
                activeTimestamp.append(currentTimestamp)
                maxTimestamp = max(maxTimestamp, currentTimestamp)

                print(f"[BEGIN TRANSACTION {currentTimestamp}]")

            transaction: MultiversionTransaction = self.getTransaction(
                currentTimestamp
            )

            try:
                transaction.nextQuery()

                if transaction.isFinished():
                    print(
                        f"[COMMIT TRANSACTION {transaction.getStartTimestamp()}]"
                    )
                    print("RESULT:")
                    transaction.commit()

            except:
                tempSchedule = list(filter(
                    lambda X: X != currentTimestamp, tempSchedule
                ))
                activeTimestamp = list(filter(
                    lambda X: X != currentTimestamp, activeTimestamp
                ))

                newTimestamp = maxTimestamp + 1

                print(f"[ROLLBACK TRANSACTION {currentTimestamp}]")
                transaction.rollback(newTimestamp)

                tempSchedule.extend(
                    newTimestamp for _ in range(transaction.getLength())
                )
