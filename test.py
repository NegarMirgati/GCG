import random
import sqlite3

MAX_CPU = 500
MAX_MEM = 2048
MAX_DCS = 30
MAX_NODES = 20
MAX_REQS = 40

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
      return (-1,-1)

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

  def getPossibleDCs(self, cpu, mem):
    return (self.c.execute("SELECT DCID FROM DataCenter WHERE CPU >= " + str(cpu) + " AND MEM >= " + str(mem)))

  def getPossibleNodes(self, possible_dcs, cpu, mem):
    dcid_list = []
    str1 = "("
    flag = False
    for row in possible_dcs:
      #print(" row is ", row)
      if(flag == False) :
        str1 += str(row[0])
        flag = True
      else:
        str1 += " ," + str(row[0]) 
      
    str1 += ")"
    #print(str1)

    return (self.c.execute("SELECT * FROM Node WHERE FDCID IN " + str1 + " AND CPU >= " + str(cpu) + " AND MEM >= " + str(mem)))


  def findBestNode(self, possible_nodes, cpu, mem):
    min_node = 0
    min_dc = 0
    min_cpu = float("inf")
    min_mem = float("inf")

    for row in possible_nodes:
      cpu_diff = row[2] - min_cpu
      mem_diff = row[3] - min_mem
      if(cpu_diff <= 0 and mem_diff <= 0):
        # TODO
        min_node = row[0]
        min_dc = row[1]

    print(min_node, min_dc)
    return(min_node, min_dc)

  def updateDCTable(self, dcid, cpu, mem):
    print("updating dctable dc with id  " + str(dcid))
    for row in (self.c.execute("SELECT * FROM DataCenter WHERE DCID = " + str(dcid))):
      print(row)
    self.c.execute("UPDATE DataCenter SET CPU = CPU - " + str(cpu) + ", MEM = MEM - " + str(mem) + " WHERE DCID = " + str(dcid))
    for row in self.c.execute("SELECT * FROM DataCenter WHERE DCID = " + str(dcid)):
      print(row)

  def updateNodeTable(self, node, cpu, mem):

    print("updating nodetable with node id  " + str(node))
    for row in (self.c.execute("SELECT * FROM Node WHERE NodeID= " + str(node))):
      print(row)
    self.c.execute("UPDATE Node SET CPU = CPU - " + str(cpu) + ", MEM = MEM - " + str(mem) + " WHERE NodeID = " + str(node))
    for row in (self.c.execute("SELECT * FROM Node WHERE NodeID= " + str(node))):
      print(row)


def distributeTasks(myQueue, myDB):

  cntr = 1
  #myDB.printNodeTable()
  while(True):
    request = myQueue.dequeue()
    if(request != (-1, -1)):
      cpu = request[0]
      mem = request[1]
      print("Task No." + str(cntr) + " needs cpu = " + str(cpu) + " and mem = " + str(mem))
      possible_dcs = myDB.getPossibleDCs(cpu, mem)
      possible_nodes = myDB.getPossibleNodes(possible_dcs, cpu, mem)
      (BestNode, BestDC) = (myDB.findBestNode(possible_nodes, cpu, mem))
      if(BestNode == 0 or BestDC == 0): 
        print("Task no." + str(cntr) +  " could not be assigned to any node.")
      else:
        myDB.updateDCTable( BestDC, cpu, mem)
        myDB.updateNodeTable( BestNode, cpu, mem)
      cntr += 1
    else:
      print("Finished distrubuting Tasks")
      return


def main():

  myQueue = Queue()
  initiateQueue(myQueue)

  #distributeTasks(myQueue)

  myDB = Database()
  myDB.createDataCenterTable()
  myDB.createNodeTable()
  
  num_of_dcs = myDB.generateRandomDCs()
  myDB.generateRandomNodes(num_of_dcs)

  #myDB.printNodeTable()


  distributeTasks(myQueue, myDB)
  #myDB.printDCTable()
  #myDB.printNodeTable()


if __name__== "__main__":
  main()


