from process.Transaction import Transaction
from process.Query import ReadQuery, WriteQuery, FunctionQuery, DisplayQuery

if __name__ == "__main__":
    T = Transaction(1, [
        ReadQuery("binary/A", "binary/B"),
        FunctionQuery("binary/A", "binary/B", function=lambda A, B: A + B),
        DisplayQuery("binary/A", "binary/B"),
        WriteQuery("binary/A")
    ])

    length = T.getLength()
    for i in range(length):
        T.executeCurrentQuery()
