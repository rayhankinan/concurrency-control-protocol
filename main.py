from process.Query import ReadQuery, WriteQuery, FunctionQuery, DisplayQuery
from manager.SerialOptimistic import SerialOptimisticTransaction, SerialOptimisticControl
from manager.MultiversionTimestampOrdering import MultiversionTransaction, MultiversionControl

if __name__ == "__main__":
    T25 = MultiversionTransaction(25, [
        ReadQuery("binary/B"),
        DisplayQuery("binary/B", function=lambda B: B),
        ReadQuery("binary/A"),
        DisplayQuery("binary/A", function=lambda A: A),
    ])

    T26 = MultiversionTransaction(26, [
        ReadQuery("binary/B"),
        DisplayQuery("binary/B", function=lambda B: B),
        FunctionQuery("binary/B", function=lambda B: B + 50),
        DisplayQuery("binary/B", function=lambda B: B),
        WriteQuery("binary/B"),
        ReadQuery("binary/A"),
        DisplayQuery("binary/A", function=lambda A: A),
        FunctionQuery("binary/A", function=lambda A: A + 50),
        DisplayQuery("binary/A", function=lambda A: A),
        WriteQuery("binary/A")
    ])

    concurrencyManager = MultiversionControl(
        [T25, T26],
        [25, 25, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 25, 25]
    )

    concurrencyManager.run()
