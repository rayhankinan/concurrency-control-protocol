class SimpleLockingControl():
    def __init__(self, schedule):
        self.schedule = schedule # example ['R1(X)', 'W2(X)', 'W2(Y)', 'W3(Y)', 'W1(X)', 'C1', 'C2', 'C3']
        self.lockTable = {} # key = data item, value = transaction that has the lock for that data item, example: {'X': '1', 'Y': '2'}
        self.waitingQueue = []
        self.finalSchedule = []
        self.previousWaitingQueue = []

    def lock(self, transactionId, dataItem):
        if dataItem in self.lockTable and self.lockTable[dataItem] != transactionId and self.lockTable[dataItem] != None:      
            return False
        else:
            self.lockTable[dataItem] = transactionId
            return True
        
    def unlock(self, transactionId, dataItem):
        if self.lockTable[dataItem] == transactionId:
            self.lockTable.pop(dataItem)

    def hasLock(self, transactionId, dataItem):
        if dataItem in self.lockTable:
            return self.lockTable[dataItem] == transactionId
        else:
            return False

    def printState(self):
        print (f"{'Schedule':20} : {self.schedule}")
        print (f"{'Lock Table':20} : {self.lockTable}")
        print (f"{'Waiting Queue':20} : {self.waitingQueue}")
        print (f"{'Final Schedule':20} : {self.finalSchedule}")
        print ()

    def run(self, verbose=False):
        if verbose:
            self.printState()

        while len(self.schedule) > 0:
            operation = self.schedule.pop(0)
            if verbose:
                print (f"{'Operation':20} : {operation}")
            if operation[0] == 'R' or operation[0] == 'W':
                transactionId = operation[1]
                dataItem = operation[3]
                if self.hasLock(transactionId, dataItem):
                    self.finalSchedule.append(operation)
                else:
                    if self.lock(transactionId, dataItem):
                        self.finalSchedule.append('L' + operation[1:])
                        self.finalSchedule.append(operation)
                    else:
                        # add to waiting queue every operation for the transaction
                        temp = []
                        temp.append(operation)
                        for op in self.schedule:
                            if op[1] == transactionId:
                                temp.append(op)
                        for op in temp[1:]:
                            self.schedule.remove(op)
                        self.waitingQueue += temp

            if operation[0] == 'C':
                transactionId = operation[1]
                # remove all locks for the transaction
                self.finalSchedule.append(operation)
                for dataItem in self.lockTable:
                    if self.lockTable[dataItem] == transactionId:
                        self.lockTable[dataItem] = None
                        self.finalSchedule.append('U' + transactionId + '(' + dataItem + ')')
                # remove all operations for the transaction from the waiting queue and add them in front of the schedule
                while len(self.waitingQueue) > 0:
                    self.schedule.insert(0, self.waitingQueue.pop())
            if verbose:
                self.printState()
        while len(self.waitingQueue) > 0:
            # if the waiting queue is the same as the previous one, then there is a deadlock
            if self.waitingQueue == self.previousWaitingQueue:
                raise Exception("Deadlock Detected")
            self.previousWaitingQueue = self.waitingQueue
            self.run(verbose)

        return self.finalSchedule


if __name__ == '__main__':
    # Test
    sampleInput = "R1(X); W2(X); W2(Y); W3(Y); W1(X); C1; C2; C3;"
    sampleInput = sampleInput.split(";")
    sampleInput = [x.strip() for x in sampleInput]
    sampleInput = [x for x in sampleInput if x != ""]

    sampleInput2 = "R1(X); R2(Y); R1(Y); W2(Y); W1(X); C1; C2;"
    sampleInput2 = sampleInput2.split(";")
    sampleInput2 = [x.strip() for x in sampleInput2]
    sampleInput2 = [x for x in sampleInput2 if x != ""]

    sampleInput3 = "R1(X); R2(Y); R1(Y); R2(X); C1; C2;" # deadlock
    sampleInput3 = sampleInput3.split(";")
    sampleInput3 = [x.strip() for x in sampleInput3]
    sampleInput3 = [x for x in sampleInput3 if x != ""]

    print("Select input type:")
    print("1. From a file")
    print("2. From terminal")

    userInput = ""
    choice = input("Enter your choice: ")
    if choice == "1":
        filename = input("Enter the file name: ")
        with open(filename, 'r') as f:
            userInput = f.read()
    elif choice == "2":
        userInput = input("Enter the input: ")
    userInput = userInput.split(";")
    userInput = [x.strip() for x in userInput]
    userInput = [x for x in userInput if x != ""]



    print(f"{'User input':20} : {userInput}")

    control = SimpleLockingControl(userInput)
    try:
        control.run()
        print(f"{'Final Schedule':20} : {control.finalSchedule}")
    except Exception as e:
        print (e)