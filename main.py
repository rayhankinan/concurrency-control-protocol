from process.Query import ReadQuery, WriteQuery, FunctionQuery, DisplayQuery
from manager.SerialOptimistic import SerialOptimisticTransaction, SerialOptimisticControl

if __name__ == "__main__":
    T25 = SerialOptimisticTransaction(25, [
        ReadQuery("binary/B"),
        ReadQuery("binary/A"),
        WriteQuery("binary/B"),
        WriteQuery("binary/A")
    ])

    T26 = SerialOptimisticTransaction(26, [
        ReadQuery("binary/B"),
        FunctionQuery("binary/B", function=lambda B: B - 50),
        ReadQuery("binary/A"),
        FunctionQuery("binary/A", function=lambda A: A + 50),
        WriteQuery("binary/B"),
        WriteQuery("binary/A")
    ])

    concurrencyManager = SerialOptimisticControl(
        [T25, T26],
        [25, 25, 26, 26, 26, 26, 26, 26, 25, 25]
    )

    concurrencyManager.run()
