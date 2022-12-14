# concurrency-control-protocol

## Description

This is a project for the course "Database Management" at Bandung Institute of Technology. The project is about implementing a concurrency control protocol for a database management system. There are 3 protocols implemented in this project, which are Simple Locking Protocol, OCC, and MVCC. The project is implemented in Python 3.

## Requirements

- Python 3

## How to run

### Simple locking protocol

1. Navigate to the directory of the project and change the directory to /manager
2. Run the following command

   ```
   python SimpleLocking.py
   ```

3. Follow the instructions on the screen
4. If you want to run the program with file input, prepare a file with the following format

   ```
   <operation type> <transaction id> optional: (<item data id>)
   ```

   e.g.

   ```
   R1(X); R2(Y); R1(Y); R2(X); C1; C2;
   ```

   Then run select 2 when prompted for the input method and enter the file name

5. If you want to run the program with terminal input, select 2 when prompted for the input method, then enter the operations with the same format as the file input

### OCC
1. Navigate to the directory of the project
2. Open the `main.py` and edit your own transaction and schedule by using `Query` for defining operation class
3. To run Optimistic Concurrency Control Algorithm, please use `SerialOptimisticTransaction` for transaction class and `SerialOptimisticControl` for concurrency control class
4. Run the following command
   ```
   python main.py
   ```
5. The program will output the serializable schedule on the terminal
### MVCC
1. Navigate to the directory of the project
2. Open the `main.py` and edit your own transaction and schedule by using `Query` for defining operation class
3. To run Optimistic Concurrency Control Algorithm, please use `MultiversionTransaction` for transaction class and `MultiversionControl` for concurrency control class
4. Run the following command
   ```
   python main.py
   ```
5. The program will output the serializable schedule on the terminal