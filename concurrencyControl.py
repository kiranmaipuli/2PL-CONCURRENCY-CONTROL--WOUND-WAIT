import pandas as pd
import sys

timeStampCounter = 0
columnNames = ["TID", "timeStamp", "state","blockedBy","blockedOperations"]
transactionTable = pd.DataFrame(columns = columnNames) #creating the schema for transaction table
transactionTable.set_index('TID',inplace = True) #using transaction_id as the index of the transaction table
columnNames = ["dataItem", "lockMode", "TIDList","blockedTIDS"]
lockTable = pd.DataFrame(columns = columnNames) #creating the schema for lock table
lockTable.set_index('dataItem',inplace = True) #using dataItem as the index of the lock table

#input operation processing
def inputOperations(inputLine):
    print("*******************BEGIN*******************")
#    print("\n")
    print("operation : "+inputLine)
    inputLine = inputLine.replace(" ","")    
    operation = inputLine[0]
    transactionNo = inputLine[1]
    if len(inputLine) > 3:
        DataItem = inputLine[3]
    global timeStampCounter
    if operation == 'b':
        timeStampCounter += 1
        begin(transactionNo)
    if operation == 'r':
        read(transactionNo,DataItem)        
    if operation == 'w':
        write(transactionNo,DataItem)
    if operation == 'e':
        end(inputLine[1])

#begin operation        
def begin(i):
    transaction = 'T'+i
    print("Begin transaction : "+transaction)
    transactionTable.loc[transaction] = [timeStampCounter, 'Active',[],[]]
    print("\n")
    print("                  Transaction Table                 ")
    print(transactionTable)
    print("\n")
    print("                 Lock Table                 ")
    print(lockTable)

#read operation
def read(i,X):
    transaction = 'T'+i
    if transactionTable.loc[transaction]['state'] == 'Active':
        readLock(i,X)
    elif transactionTable.loc[transaction]['state'] == 'Blocked':
        inputLine = 'r'+i+'('+X+');'
        transactionTable.loc[transaction]['blockedOperations'].append(inputLine)
        print("operation is appended to the BlockedOperations of the transaction "+transaction)
        print("\n")
        print("                  Transaction Table                 ")
        print(transactionTable)
        print("\n")
        print("                 Lock Table                 ")
        print(lockTable)
    elif transactionTable.loc[transaction]['state'] == 'Aborted':
        print(transaction+' already aborted')

def readLock(i,X):
    transaction = 'T'+i
    if X not in lockTable.index:
        #inserting the data item into the lock table
        lockTable.loc[X] = ['R', [transaction],[]]
        print("Item "+X+" read locked by "+transaction)
        print("\n")
        print("                  Transaction Table                 ")
        print(transactionTable)
        print("\n")
        print("                 Lock Table                 ")
        print(lockTable)
    else:
        if lockTable.loc[X]['lockMode'] == 'R':
            #sharing of read lock with other transactions
            if transaction not in lockTable.loc[X]['TIDList']:
                lockTable.loc[X]['TIDList'].append(transaction)
                print("Item "+X+" read locked by "+transaction)
                print("\n")
                print("                  Transaction Table                 ")
                print(transactionTable)
                print("\n")
                print("                 Lock Table                 ")
                print(lockTable)
            else:
                print("read lock already exists on "+X+"by "+transaction)
        else:
            # if there is a write lock on the data item by the current transaction
            if lockTable.loc[X]['TIDList'][0] == transaction:
                # downgrading of the write lock of the current transaction
                lockTable.loc[X]['lockMode'] = 'R'
                print("lock mode is downgraded for the data item "+X)
                print("\n")
                print("                  Transaction Table                 ")
                print(transactionTable)
                print("\n")
                print("                 Lock Table                 ")
                print(lockTable)
            else:
                # read-write conflict
                deadLock(i,X,'r')
                
#write operation
def write(i,X):
    transaction = 'T'+i
    if transactionTable.loc[transaction]['state'] == 'Active':
        writeLock(i,X)
    elif transactionTable.loc[transaction]['state'] == 'Blocked':
        inputLine = 'w'+i+'('+X+');'
        transactionTable.loc[transaction]['blockedOperations'].append(inputLine)
        print("operation is appended to the BlockedOperations of the transaction "+transaction)
        print("\n")
        print("                  Transaction Table                 ")
        print(transactionTable)
        print("\n")
        print("                 Lock Table                 ")
        print(lockTable)
    elif transactionTable.loc[transaction]['state'] == 'Aborted':
        print(transaction+' already aborted')
        

def writeLock(i,X):
    transaction = 'T'+i
    # if the data item does not exist in the lock table
    if X not in lockTable.index:
        #insert the data item into the table with write lock
        lockTable.loc[X] = ['W', [transaction],[]]
        print("Item "+X+" write locked by "+transaction)
        print("\n")
        print("                  Transaction Table                 ")
        print(transactionTable)
        print("\n")
        print("                 Lock Table                 ")
        print(lockTable)
#        print(X+" write locked by "+transaction+": Lock table record for "+X+" is created with mode W ("+transaction+" holds lock)")
    else:
        # upgrading of read lock of the current transaction on the data item
        if (lockTable.loc[X]['lockMode'] == 'R') and (len(lockTable.loc[X]['TIDList']) == 1) and (lockTable.loc[X]['TIDList'][0] == transaction):
            lockTable.loc[X]['lockMode'] = 'W'
            print("lock mode is upgraded for the data item "+X)
            print("\n")
            print("                  Transaction Table                 ")
            print(transactionTable)
            print("\n")
            print("                 Lock Table                 ")
            print(lockTable)
        else:
            #read-write conflict
            deadLock(i,X,'w')
                
def deadLock(i,X,mode):
    print("deadlock is encountered")
    transaction = 'T'+i
    for j in lockTable.loc[X]['TIDList']:
        if transactionTable.loc[transaction]['timeStamp'] < transactionTable.loc[j]['timeStamp']:
            abort(j[1])
        elif transactionTable.loc[transaction]['timeStamp'] > transactionTable.loc[j]['timeStamp']:
            if transaction not in lockTable.loc[X]['blockedTIDS']:
                lockTable.loc[X]['blockedTIDS'].append(transaction)
                transactionTable.loc[transaction]['state'] = 'Blocked'
                inputLine = mode+i+'('+X+');'
                transactionTable.loc[transaction]['blockedOperations'].append(inputLine)    
            if j not in transactionTable.loc[transaction]['blockedBy']:
                transactionTable.loc[transaction]['blockedBy'].append(j)
    unblock()
    if X in lockTable.index:
        if len(lockTable.loc[X]['blockedTIDS']) == 0 and len(lockTable.loc[X]['TIDList']) == 1 and lockTable.loc[X]['TIDList'][0] == transaction:
            if mode == 'r':
                lockTable.loc[X]['lockMode'] = 'R'
            else:
                lockTable.loc[X]['lockMode'] = 'W'
        print("\n")
        print("                  Transaction Table                 ")
        print(transactionTable)
        print("\n")
        print("                 Lock Table                 ")
        print(lockTable)
    
# aborting transaction Ti
def abort(i):
    #updating transaction table
    transaction = 'T'+i
    transactionTable.loc[transaction]['state'] = 'Aborted'
    transactionTable.loc[transaction]['blockedBy'] = []
    transactionTable.loc[transaction]['blockedOperations'] = []
    print(transaction+" is aborted")
    unLock(i)        

def unLock(i):
    transaction = 'T'+i
    for j in transactionTable.index:
        if transaction in transactionTable.loc[j]['blockedBy']:
            transactionTable.loc[j]['blockedBy'].remove(transaction)

    # unlocking the data items locked by Ti      
    for dataValue in lockTable.index:
        # removing Ti from TIDList
        temp = []
        if transaction in lockTable.loc[dataValue]['TIDList']:
            temp.append(transaction)
        for transactions in temp:    
            lockTable.loc[dataValue]['TIDList'].remove(transactions)

        # removing Ti from blockedTIDS
        temp = []
        if transaction in lockTable.loc[dataValue]['blockedTIDS']:
            temp.append(transaction)
        for transaction in temp:
            lockTable.loc[dataValue]['blockedTIDS'].remove(transaction)    

    # removing data items from the lock table that are completely free            
    for dataValue in lockTable.index:
        if len(lockTable.loc[dataValue]['TIDList']) == 0 and len(lockTable.loc[dataValue]['blockedTIDS']) == 0: 
            lockTable.drop([dataValue],axis=0, inplace=True)
            


def unblock():
    for dataValue in lockTable.index:
        blockList = lockTable.loc[dataValue]['blockedTIDS'][:]
        tidList = lockTable.loc[dataValue]['TIDList']
        # unblock the blocked transactions

        if len(blockList) != 0:
            if (len(tidList) == 0) or (len(tidList) == 1 and (tidList[0] == blockList[0])):
                for transactions in blockList:
                    # if the transaction is not blocked by any one 
                    if len(transactionTable.loc[transactions]['blockedBy']) == 0:
                        if len(lockTable.loc[dataValue]['blockedTIDS']) == 1:
                            lockTable.drop([dataValue],axis=0, inplace=True)
                        else:                            
                            lockTable.loc[dataValue]['blockedTIDS'].remove(transactions)
                        transactionTable.loc[transactions]['state'] = 'Active'
                        # execute the blocked operations
                        size = len(transactionTable.loc[transactions]['blockedOperations'])
                        for k in range(0,size):
                            if len(transactionTable.loc[transactions]['blockedOperations']) >= 1:
                                a = transactionTable.loc[transactions]['blockedOperations'][0]
                                del transactionTable.loc[transactions]['blockedOperations'][0]
                                inputOperations(a)
            
#end operation
def end(i):
    transaction = 'T'+i
    if transactionTable.loc[transaction]['state'] == "Active":
        transactionTable.loc[transaction]['state'] = 'Committed'
        print("transaction "+transaction+" is committed")
        unLock(i)
        unblock()
        print("\n")        
        print("operation e"+i)
        print("\n")
        print("                  Transaction Table                 ")
        print(transactionTable)
        print("\n")
        print("                 Lock Table                 ")
        print(lockTable)
    elif transactionTable.loc[transaction]['state'] == 'Blocked':
        inputLine = 'e'+i+';'
        transactionTable.loc[transaction]['blockedOperations'].append(inputLine)
        print("input operation is added to the BlockedOperations list of transaction")
        print("\n")
        print("                  Transaction Table                 ")
        print(transactionTable)
        print("\n")
        print("                 Lock Table                 ")
        print(lockTable)
    elif transactionTable.loc[transaction]['state'] == 'Aborted':
        print(transaction+' is already aborted')        

#input file is the name of the input file. File name is passed as a command line argument while executing the input
inputFile = sys.argv[1]        

#reading input from the file
with open(inputFile) as openfileobject:
    lines = openfileobject.readlines()
    for line in lines:
        if line != '\n':
            inputLine = line.rstrip()        
            inputOperations(inputLine)     
            print("*******************END*******************")
            print("\n")

