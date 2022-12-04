class SimpleLockingControl():
    def __init__(self, schedule):
        self.schedule = schedule # example ['R1(X)', 'W2(X)', 'W2(Y)', 'W3(Y)', 'W1(X)', 'C1', 'C2', 'C3']
        self.lockTable = {} # key = data item, value = transaction that has the lock for that data item, example: {'X': '1', 'Y': '2'}
        self.waitingQueue = [] # example: ['R1(X)', 'W2(X)', 'W2(Y)', 'W3(Y)', 'W1(X)', 'C1', 'C2', 'C3']
        self.finalSchedule = [] # example: ['R1(X)', 'W2(X)', 'W2(Y)', 'W3(Y)', 'W1(X)', 'C1', 'C2', 'C3']

    def lock(self, transactionId, dataItem):
        # add the lock to the lock table
        if dataItem in self.lockTable and self.lockTable[dataItem] != transactionId and self.lockTable[dataItem] != None:      
            return False
        else:
            self.lockTable[dataItem] = transactionId
            return True
        
    def unlock(self, transactionId, dataItem):
        # remove the lock from the lock table
        if self.lockTable[dataItem] == transactionId:
            self.lockTable.pop(dataItem)

    def hasLock(self, transactionId, dataItem):
        if dataItem in self.lockTable:
            return self.lockTable[dataItem] == transactionId
        else:
            return False

    def printState(self):
        # print with a nice format the current state of the schedule
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
                        self.waitingQueue.append(operation)
                        for op in self.schedule:
                            if op[1] == transactionId:
                                self.schedule.remove(op)
                                self.waitingQueue.append(op)
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
            op = self.waitingQueue.pop(0)
            self.finalSchedule.append(op)
            if verbose:
                self.printState()
        return self.finalSchedule


if __name__ == '__main__':
    # Test
    sampleInput = "R1(X); W2(X); W2(Y); W3(Y); W1(X); C1; C2; C3;"
    sampleInput = sampleInput.split(";")
    sampleInput = [x.strip() for x in sampleInput]
    sampleInput = [x for x in sampleInput if x != ""]
    print(f"{'Input':20} : {sampleInput}")

    control = SimpleLockingControl(sampleInput)
    control.run()
    print(f"{'Final Schedule':20} : {control.finalSchedule}")