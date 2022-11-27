from process.Transaction import Transaction
from process.Query import ReadQuery, WriteQuery, FunctionQuery, DisplayQuery

if __name__ == "__main__":
    T = Transaction(1, [
        ReadQuery("binary/A"),
        DisplayQuery("binary/A"),
        FunctionQuery("binary/A", lambda A: A + 50),
        DisplayQuery("binary/A"),
        WriteQuery("binary/A")
    ])

    length = T.getLength()
    for i in range(length):
        T.executeCurrentQuery()
