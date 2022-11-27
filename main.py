from process.Data import Data


if __name__ == "__main__":
    A = Data("binary/A")
    content = A.read()
    print(content)
    content = 15
    A.write(content)
