from process.Query import ReadQuery, WriteQuery, FunctionQuery, DisplayQuery
from manager.SerialOptimistic import SerialOptimisticTransaction

if __name__ == "__main__":
    T25 = SerialOptimisticTransaction(25, [
        ReadQuery("binary/B"),
        FunctionQuery("binary/B", function=lambda B: B - 50),
        ReadQuery("binary/A"),
        FunctionQuery("binary/A", function=lambda A: A + 50),
        WriteQuery("binary/B"),
        WriteQuery("binary/A")
    ])

    T26 = SerialOptimisticTransaction(26, [
        ReadQuery("binary/B"),
        ReadQuery("binary/A"),
        DisplayQuery("binary/A", "binary/B", function=lambda A, B: A + B)
    ])
