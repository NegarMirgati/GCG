import random
import sqlite3

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
      for x in range(random.randint(1,101)):
           cpu = random.randint(1,101)
           mem = random.randint(1,101)
           myQueue.enqueue(cpu, mem)

def createDataCenterTable(c):
	c.execute('PRAGMA foreign_keys = ON')
	c.execute('''CREATE TABLE IF NOT EXISTS `DataCenter` (
	`DCID`	INTEGER,
	`CPU`	INTEGER,
	`MEM`	INTEGER,
	PRIMARY KEY(`DCID`)
);'''
)
	

def  createNodeTable(c):
	c.execute('PRAGMA foreign_keys = ON')
	c.execute('''CREATE TABLE IF NOT EXISTS `Node` (
	`NodeID`	INTEGER,
	`FDCID`	INTEGER,
	`CPU`	INTEGER DEFAULT 0,
	`MEM`	INTEGER DEFAULT 0,
	PRIMARY KEY(`NodeID`)
	FOREIGN KEY (`FDCID`) REFERENCES DataCenter(`DCID`)
);
''')
	

def insertIntoDCTable(c, rowid):

	exec_str = "INSERT INTO DataCenter VALUES (" + str(rowid) + ", " + "NULL" + ", " + "NULL" + ")" 
	c.execute(exec_str)


def insertIntoNodeTable(c, rowid, dcid, cpu, mem):
	exec_str = "INSERT INTO Node VALUES (" + str(rowid) + ", " +str(dcid) + ", "+ str(cpu) + ", " + str(mem) + ")"  
	#print(exec_str)
	c.execute(exec_str)

def generateRandomDCs(c):
  num_of_dcs = random.randint(1,101) # number of data centers between 1 and 100
  print("creating " + str(num_of_dcs) + " datacenters")
  for x in range(1, num_of_dcs + 1):
  	insertIntoDCTable(c, x)
  return num_of_dcs

def generateRandomNodes(c, num_of_dcs):
	node_table_cntr = 0
  	for dc_cnt in range(1, num_of_dcs + 1):
  		num_of_nodes = random.randint(1, 51)
  		cpu = random.randint(1, 101)
  		mem = random.randint(1, 2049)
  		total_cpu = cpu * num_of_nodes
  		total_mem = mem * num_of_nodes
  		for node_cnt in range(1, num_of_nodes + 1):
  			insertIntoNodeTable(c, node_table_cntr, dc_cnt, cpu, mem)
  			node_table_cntr += 1

  		c.execute("UPDATE DataCenter SET CPU = " + str(total_cpu) + ", MEM =  " + str(total_mem) + " WHERE DCID = " + str(dc_cnt))



def main():

  myQueue = Queue()
  initiateQueue(myQueue)
  size = myQueue.size()
  print(size)
  conn = sqlite3.connect('mydb.db')
  c = conn.cursor()
  createDataCenterTable(c)
  #c.execute("DROP TABLE Node")
  #c.execute("DELETE FROM DataCenter")
  #conn.commit()
  createNodeTable(c)


  num_of_dcs = generateRandomDCs(c)
  generateRandomNodes(c, num_of_dcs)

  print("here")

  for row in c.execute("SELECT * FROM DataCenter"):
  	print(row)

  for row in c.execute("SELECT * FROM Node"):
  	print(row)
  		

if __name__== "__main__":
  main()


