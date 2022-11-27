from process.FileAccess import FileAccess


if __name__ == "__main__":
    A = FileAccess("binary/A")
    content = A.read()

    print(content)
    content += 15
    print(content)

    A.write(content)
