import random
import sqlite3

MAX_CPU = 100
MAX_MEM = 2048
MAX_DCS = 20
MAX_NODES = 10
MAX_REQS = 200

class Queue:

  def __init__(self):
      self.queue = list()

  def enqueue(self, cpu, mem):
      #Checking to avoid duplicate entry (not mandatory)
      #if data not in self.queue:
     self.queue.insert(0,(cpu,mem))
     return True
      #return False

  def dequeue(self):
      if len(self.queue)>0:
          return self.queue.pop()
      return ("Queue Empty!")

  def size(self):
      return len(self.queue)

  def printQueue(self):
      return self.queue

def initiateQueue(myQueue):
      for x in range(random.randint(1, MAX_REQS + 1)):
           cpu = random.randint(1,MAX_CPU + 1)
           mem = random.randint(1,MAX_MEM + 1)
           myQueue.enqueue(cpu, mem)

class Database:

  def __init__(self):
      self.conn = sqlite3.connect('mydb.db')
      self.c = self.conn.cursor()
      

  def createDataCenterTable(self):
  	self.c.execute('PRAGMA foreign_keys = ON')
  	self.c.execute('''CREATE TABLE IF NOT EXISTS `DataCenter` (
  	`DCID`	INTEGER,
  	`CPU`	INTEGER,
  	`MEM`	INTEGER,
  	PRIMARY KEY(`DCID`)
  );'''
  )
  	

  def  createNodeTable(self):
  	self.c.execute('PRAGMA foreign_keys = ON')
  	self.c.execute('''CREATE TABLE IF NOT EXISTS `Node` (
  	`NodeID`	INTEGER,
  	`FDCID`	INTEGER,
  	`CPU`	INTEGER DEFAULT 0,
  	`MEM`	INTEGER DEFAULT 0,
  	PRIMARY KEY(`NodeID`)
  	FOREIGN KEY (`FDCID`) REFERENCES DataCenter(`DCID`)
  );
  ''')
  	
  def generateRandomDCs(self):
    num_of_dcs = random.randint(1,MAX_DCS + 1) 
    print("creating " + str(num_of_dcs) + " datacenters")
    for x in range(1, num_of_dcs + 1):
      self.insertIntoDCTable(x)
    return num_of_dcs

  def generateRandomNodes(self, num_of_dcs):
  	node_table_cntr = 1
    	for dc_cnt in range(1, num_of_dcs + 1):
    		num_of_nodes = random.randint(1, MAX_NODES + 1)
    		cpu = random.randint(1, MAX_CPU + 1)
    		mem = random.randint(1, MAX_MEM + 1)
    		total_cpu = cpu * num_of_nodes
    		total_mem = mem * num_of_nodes
    		for node_cnt in range(1, num_of_nodes + 1):
    			self.insertIntoNodeTable(node_table_cntr, dc_cnt, cpu, mem)
    			node_table_cntr += 1

    		self.c.execute("UPDATE DataCenter SET CPU = " + str(total_cpu) + ", MEM =  " + str(total_mem) + " WHERE DCID = " + str(dc_cnt))

  def insertIntoDCTable(self, rowid):

    exec_str = "INSERT INTO DataCenter VALUES (" + str(rowid) + ", " + "NULL" + ", " + "NULL" + ")" 
    self.c.execute(exec_str)


  def insertIntoNodeTable(self, rowid, dcid, cpu, mem):
    exec_str = "INSERT INTO Node VALUES (" + str(rowid) + ", " +str(dcid) + ", "+ str(cpu) + ", " + str(mem) + ")"  
    #print(exec_str)
    self.c.execute(exec_str)

  def printDCTable(self):
    for row in self.c.execute("SELECT * FROM DataCenter"):
      print(row)

  def printNodeTable(self):
    for row in self.c.execute("SELECT * FROM Node"):
     print(row)




def main():

  myQueue = Queue()
  initiateQueue(myQueue)
  size = myQueue.size()
  print(size)

  myDB = Database()
  myDB.createDataCenterTable()
  myDB.createNodeTable()
  
  num_of_dcs = myDB.generateRandomDCs()
  myDB.generateRandomNodes(num_of_dcs)
  myDB.printDCTable()
  myDB.printNodeTable()

  print("here")

if __name__== "__main__":
  main()


