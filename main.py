from process.Query import ReadQuery, WriteQuery, FunctionQuery, DisplayQuery
from manager.SerialOptimistic import SerialOptimisticTransaction, SerialOptimisticControl
from manager.MultiversionTimestampOrdering import MultiversionTransaction, MultiversionControl

if __name__ == "__main__":
    T25 = SerialOptimisticTransaction(25, [
        ReadQuery("binary/B"),
        WriteQuery("binary/B"),
        ReadQuery("binary/A"),
        WriteQuery("binary/A")
    ])

    T26 = SerialOptimisticTransaction(26, [
        ReadQuery("binary/B"),
        DisplayQuery("binary/B", function=lambda B: B),
        FunctionQuery("binary/B", function=lambda B: B + 50),
        WriteQuery("binary/B"),
        ReadQuery("binary/A"),
        DisplayQuery("binary/A", function=lambda A: A),
        FunctionQuery("binary/A", function=lambda A: A + 50),
        WriteQuery("binary/A")
    ])

    concurrencyManager = SerialOptimisticControl(
        [T25, T26],
        [25, 25, 26, 26, 26, 26, 26, 26, 26, 26, 25, 25]
    )

    concurrencyManager.run()
