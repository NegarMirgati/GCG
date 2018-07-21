import random
import sqlite3

MAX_CPU = 500
MAX_MEM = 2048
MAX_DCS = 10
MAX_NODES = 10
MAX_REQS = 500
MIN_PRIORITY = 3

task_id = 1

class Queue:

  def __init__(self):
      self.queue = list()

  def enqueue(self, task_id, cpu, cpu_type, mem, priority):
      #Checking to avoid duplicate entry (not mandatory)
      #if data not in self.queue:
     self.queue.insert(0,(task_id, cpu, cpu_type, mem, priority))
     return True
      #return False
  def enqueuelist(self, toEnqueue):
    for x in toEnqueue:
      task_id = x[0]
      cpu = x[3]
      cpu_type = x[4]
      mem = x[5]
      priority = x[6]
      print("Enqueueing ",task_id, cpu, cpu_type, mem, priority)
      self.queue.insert(0, (task_id, cpu, cpu_type, mem, priority))

  def dequeue(self):
      if len(self.queue)>0:
          return self.queue.pop()
      return (-1,-1, -1, -1, -1)

  def size(self):
      return len(self.queue)

  def printQueue(self):
      return self.queue

  def initiateQueue(self):
      # TASK_ID, CPU, CP_TYPE, MEM, PRIORITY
      self.enqueue(1, 50, 2, 700, 2)
      self.enqueue(2, 50, 2, 100, 3)
      self.enqueue(3, 50, 2, 800, 1)
      self.enqueue(4, 200, 1, 1500, 3)
      self.enqueue(5, 350, 1, 700, 2)
      global task_id
      task_id = 6
      #global task_id
      # task_id = 1
      # for x in range(random.randint(1, MAX_REQS)):

      #      cpu = random.randint(1,MAX_CPU)
      #      cpu_type = random.randint(1, 2)
      #      mem = random.randint(1,MAX_MEM)
      #      priority = random.randint(1, MIN_PRIORITY)
      #      self.enqueue(task_id, cpu, cpu_type, mem, priority)
      #      task_id += 1

class Database:

  def __init__(self):
      self.conn = sqlite3.connect('setareee.db')
      self.c = self.conn.cursor()
      

  def createDataCenterTable(self):
      self.c.execute('PRAGMA foreign_keys = ON')
      self.c.execute('''CREATE TABLE IF NOT EXISTS `DataCenter` (
      `DCID`    INTEGER,
      `TOT_CPU` INTEGER,
      `CPU`    INTEGER,
      `CPU_TYPE` INTEGER,
      `TOT_MEM` INTEGER DEFAULT 0,
      `MEM`    INTEGER,
      PRIMARY KEY(`DCID`)
  );'''
  )
      

  def  createNodeTable(self):
      self.c.execute('PRAGMA foreign_keys = ON')
      self.c.execute('''CREATE TABLE IF NOT EXISTS `Node` (
      `NodeID`    INTEGER,
      `FDCID`    INTEGER,
      `TOT_CPU` INTEGER DEFAULT 0,
      `CPU`    INTEGER DEFAULT 0,
      `CPU_TYPE`    INTEGER DEFAULT 0,
      `TOT_MEM` INTEGER DEFAULT 0,
      `MEM`    INTEGER DEFAULT 0,
      PRIMARY KEY(`NodeID`)
      FOREIGN KEY (`FDCID`) REFERENCES DataCenter(`DCID`) ON DELETE CASCADE
  );
  ''')

  def createTaskTable(self):
    self.c.execute('PRAGMA foreign_keys = ON')
    self.c.execute('''CREATE TABLE IF NOT EXISTS `Task` (
      `TASKID`    INTEGER,
      `DCID`    INTEGER,
      `NODEID` INTEGER,
      `CPU` INTEGER,
      `CPU_TYPE` INTEGER,
      `MEM` INTEGER,
      `PRIORITY` INTEGER,

      PRIMARY KEY(`TASKID`),
      FOREIGN KEY (`DCID`) REFERENCES DataCenter(`DCID`) ON DELETE CASCADE
      FOREIGN KEY (`NODEID`) REFERENCES Node(`NodeID`) ON DELETE CASCADE
  );'''
  )

    global task_id
    for x in range(1, task_id):
      print("inserting task")
      self.c.execute("INSERT INTO Task VALUES (" + str(x) + ", NULL, NULL, NULL, NULL, NULL, NULL)" )
      
  def generateRandomDCs(self):
    #num_of_dcs = random.randint(1,MAX_DCS) 
    num_of_dcs = random.randint(1,1) 
    print("creating " + str(num_of_dcs) + " datacenters")
    for x in range(1, num_of_dcs + 1):
      self.insertIntoDCTable(x)
    return num_of_dcs

  def generateRandomNodes(self, num_of_dcs):
    # node_table_cntr = 1
    # for dc_cnt in range(1, num_of_dcs + 1):
    #   num_of_nodes = random.randint(1, MAX_NODES)
    #   cpu = random.randint(1, MAX_CPU)
    #   cpu_type = random.randint(1, 2)
    #   mem = random.randint(1, MAX_MEM)

    #   total_cpu = cpu * num_of_nodes
    #   total_mem = mem * num_of_nodes

    #   for node_cnt in range(1, num_of_nodes + 1):
    #         self.insertIntoNodeTable(node_table_cntr, dc_cnt, cpu, cpu, cpu_type, mem, mem)
    #         node_table_cntr += 1
    # self.enqueue(1, 100, 2, 700, 2)
    # self.enqueue(2, 50, 2, 800, 1)
    # self.enqueue(3, 50, 2, 800, 3)
    # self.enqueue(4, 200, 1, 1500, 3)
    # self.enqueue(5, 350, 1, 700, 2)
    self.insertIntoNodeTable(1, 1, 120, 120, 2, 800, 800)
    total_cpu = 120
    total_mem = 800
    cpu_type = 2
    dc_cnt = 1


    self.c.execute("UPDATE DataCenter SET TOT_CPU = " + str(total_cpu) + ", CPU = " + str(total_cpu) + ", CPU_TYPE = "+ str(cpu_type) +
                      ", TOT_MEM =  " + str(total_mem) + ", MEM = " + str(total_mem) + " WHERE DCID = " + str(dc_cnt))

  def insertIntoDCTable(self, rowid):  #corrected

    exec_str = "INSERT INTO DataCenter VALUES (" + str(rowid) + ", " + "NULL" + ", " + "NULL" + ", " + "NULL" + ", " + "NULL" + ", " +"NULL" + ")" 
    self.c.execute(exec_str)


  def insertIntoNodeTable(self, rowid, dcid, tot_cpu, cpu, cpu_type, tot_mem, mem):  #corrected
    exec_str = "INSERT INTO Node VALUES (" + str(rowid) + ", " +str(dcid) + ", " +str(cpu) + ", " +str(cpu) + " ," + str(cpu_type) + ", " + str(mem) + ", " + str(mem)+ ")"  
    #print(exec_str)
    self.c.execute(exec_str)

  def printDCTable(self):
    for row in self.c.execute("SELECT * FROM DataCenter"):
      print(row)

  def printNodeTable(self):
    for row in self.c.execute("SELECT * FROM Node"):
     print(row)

  def getPossibleDCs(self, cpu, cpu_type, mem):
    return (self.c.execute("SELECT DCID FROM DataCenter WHERE CPU >= " + str(cpu) + " AND CPU_TYPE =" + str(cpu_type) +  " AND MEM >= " + str(mem)))

  def getPossibleNodes(self, possible_dcs, cpu, mem):
  
    str1 = "("
    flag = False
    for row in possible_dcs:
      if(flag == False) :
        str1 += str(row[0])
        flag = True
      else:
        str1 += " ," + str(row[0]) 
      
    str1 += ")"
  
    return (self.c.execute("SELECT * FROM Node WHERE FDCID IN " + str1 + " AND CPU >= " + str(cpu) + " AND MEM >= " + str(mem)))


  def getDCsToPreempt(self, cpu, cpu_type, mem):
    return (self.c.execute("SELECT DCID FROM DataCenter WHERE TOT_CPU >= " + str(cpu) + " AND CPU_TYPE =" + str(cpu_type) +  " AND TOT_MEM >= " + str(mem)))

  def getNodesToPreempt(self, possible_dcs, cpu, mem):
   
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
 
    return (self.c.execute("SELECT * FROM Node WHERE FDCID IN " + str1 + " AND TOT_CPU >= " + str(cpu) + " AND TOT_MEM >= " + str(mem)))

  def findBestNode(self, possible_nodes, cpu, mem):
    min_node = 0
    min_dc = 0
    min_cpu = float("inf")
    min_mem = float("inf")

    for row in possible_nodes:
      cpu_diff = row[3] - min_cpu
      mem_diff = row[6] - min_mem
      if(cpu_diff <= 0 and mem_diff <= 0):
        # TODO
        min_node = row[0]
        min_dc = row[1]

    print(min_node, min_dc)
    return(min_node, min_dc)

  def preemptIfPossible(self, cpu, cpu_type, mem, priority, myQueue):
    print("____________PREEMPT___________________")
    possible_dcs = self.getDCsToPreempt(cpu, cpu_type, mem)
    possible_nodes = self.getNodesToPreempt(possible_dcs, cpu, mem)
    print("needed cpu = ", cpu)
    print("needed mem = ", mem)
    dcid = 0
    NodeID = 0
    
    for row in (possible_nodes) :
      
      NodeID = row[0]
      dcid = row[1]
      tasks = self.c.execute("SELECT * FROM Task WHERE NODEID = " + str(NodeID) + " AND PRIORITY > " + str(priority))
      print("node = " + str(NodeID)+ " DCID = " + str(dcid) + " total_cpu = " + str(row[2]) + " cpu = " + str(row[3]) + " total_mem = " + str(row[5]) + " mem = "+ str(row[6]))
      tasks_cpu = 0
      tasks_mem = 0

      current_cpu = row[3]
      current_mem = row[6]
      
      print("node = ", NodeID, "dcid = ", dcid)
      to_preempt = list()
      to_enqueue = list()

      for x in tasks : 
        tasks_cpu += x[3]
        tasks_mem += x[5]
        to_preempt.append(x[0])
        to_enqueue.append(x)

        if(current_mem + tasks_mem >= mem and current_cpu + tasks_cpu >= cpu):
          print("%%%%%%%%************PREEMPRION POSSIBLE%%%%%%%%%%%%%************")
          self.preemptTasks(to_preempt)       ### preempt tasks on this node
          myQueue.enqueuelist(to_enqueue)       ### enqueue preempted tasks
          self.updateDCTable(dcid,  cpu - tasks_cpu, mem - tasks_mem)     ## update cpu and mem values on Datacenter Table
          self.updateNodeTable(x[0], cpu - tasks_cpu, mem - tasks_mem)   ## update cpu and mem values on Node Table
          return(dcid, NodeID)

    return(0, 0)

    

  def preemptTasks(self, to_preempt):

    for i in to_preempt:
      print("preempting task no." + str(i))
      self.c.execute("UPDATE Task SET DCID = NULL, NODEID = NULL WHERE TASKID = " + str(i))


  def updateDCTable(self, dcid, cpu, mem):
    #print("updating dctable dc with id  " + str(dcid))
    #for row in (self.c.execute("SELECT * FROM DataCenter WHERE DCID = " + str(dcid))):
      #print(row)
    self.c.execute("UPDATE DataCenter SET CPU = CPU - " + str(cpu) + ", MEM = MEM - " + str(mem) + " WHERE DCID = " + str(dcid))
    #for row in self.c.execute("SELECT * FROM DataCenter WHERE DCID = " + str(dcid)):
      #print(row)

  def updateNodeTable(self, node, cpu, mem):

    print("updating nodetable with node id  " + str(node))
    #for row in (self.c.execute("SELECT * FROM Node WHERE NodeID= " + str(node))):
      #print(row)
    self.c.execute("UPDATE Node SET CPU = CPU - " + str(cpu) + ", MEM = MEM - " + str(mem) + " WHERE NodeID = " + str(node))
    #for row in (self.c.execute("SELECT * FROM Node WHERE NodeID= " + str(node))):
      #print(row)

  def updateTaskTable(self, task_id, dc, node, cpu, cpu_type, mem, priority):
    print("upadtind task no. " + str(task_id))
    print("update task table", "node = ", node, "dc = ", dc)
    self.c.execute("UPDATE Task SET DCID = " + str(dc) + ", NODEID = " + str(node) + ", CPU = " + str(cpu)
                    + ",MEM = " + str(mem) + ", PRIORITY = " + str(priority) + ", CPU_TYPE = " + str(cpu_type) + " WHERE TASKID = " + str(task_id))

  def printStatus(self, file):
    file.write(" -------------------------------------- DataCenters' Status ------------------------------------------\n\n")
    for x in (self.c.execute("SELECT * FROM DataCenter")):
      DCID = x[0]
      TOT_CPU = x[1]
      CPU = x[2]
      TYPE = x[3]
      TOT_MEM = x[4]
      MEM = x[5]
      file.write("DataCenter no." + str(DCID) + " total number of processors = " + str(TOT_CPU) + " of type " + str(TYPE) + ", " + str(CPU) + " processors remaining")
      file.write(" total mem = " + str(TOT_MEM) + ", " + str(MEM) + " remaining\n")

    file.write(" ---------------------------------------- Nodes' Status -----------------------------------------------\n\n")
    for row in (self.c.execute("SELECT * FROM Node")):
      NODE_ID = row[0]
      FDCID = row[1]
      TOT_CPU = row[2]
      CPU = row[3]
      TYPE = row[4]
      TOT_MEM = row[5]
      MEM = row[6]
      file.write("Node no." + str(NODE_ID) + " on DC No." + str(FDCID) + " total number of processors = " + str(TOT_CPU) + " of type " + str(TYPE) + ", " + str(CPU) + " processors remaining")
      file.write(" total mem = " + str(TOT_MEM) + ", " + str(MEM) + " remaining\n")


      file.write(" ---------------------------------------- Tasks' Status -----------------------------------------------\n\n")
    for row in (self.c.execute("SELECT * FROM Task")):
      TASK_ID = row[0]
      DCID = row[1]
      NODE_ID = row[2]
      CPU = row[3]
      MEM = row[4]
      PRIORITY= row[5]
      file.write("Task no." + str(TASK_ID) + ", On Node no." + str(NODE_ID) + ", on DC No." + str(DCID) + "\n")
      


  def commitDB(self):
    self.conn.commit()


def distributeTasksBF(myQueue, myDB, file):

  
  file.write("--------------------------------- LOG FILE ---------------------------------\n\n")
  while(True):
    request = myQueue.dequeue()
    if(request != (-1, -1, -1, -1, -1)):
      task_no = request[0]
      cpu = request[1]
      cpu_type = request[2]
      mem = request[3]
      priority = request[4]
      file.write("Task No." + str(task_no) + " needs cpu = " + str(cpu) + " type = " + str(cpu_type) + " and mem = " + str(mem) + "\n")
      possible_dcs = myDB.getPossibleDCs(cpu, cpu_type, mem)
      possible_nodes = myDB.getPossibleNodes(possible_dcs, cpu, mem)
      (BestNode, BestDC) = (myDB.findBestNode(possible_nodes, cpu, mem))
      if(BestNode == 0 or BestDC == 0): 
        print("TRYING TO PUT TASK NO." + str(task_no) + "BY PREEMPTION")
        (PDC, PNode) = myDB.preemptIfPossible(cpu, cpu_type, mem, priority, myQueue)
        if(PNode == 0 or PDC == 0):
          file.write("***Task no." + str(task_no) +  " could not be allocated to any node.\n")
        else:
          file.write("$$$Task no." + str(task_no) + " allocated to datacenter " + str(PDC) + " and node " + str(PNode) + "\n")
          myDB.updateTaskTable(task_no, PDC, PNode, cpu, cpu_type, mem, priority)
          
      else:
        myDB.updateDCTable( BestDC, cpu, mem)
        myDB.updateNodeTable( BestNode, cpu, mem)
        myDB.updateTaskTable(task_no, BestDC, BestNode, cpu, cpu_type, mem, priority)
        file.write("%%%Task no." + str(task_no) + " allocated to datacenter " + str(BestDC) + " and node " + str(BestNode) + "\n")
      
    else:
      print("request", request)
      file.write("\n--------------------------------------Finished distributing Tasks--------------------------------------\n\n")
      return

    file.write("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n\n")


def main():

  myQueue = Queue()
  myQueue.initiateQueue()

  myDB = Database()
  myDB.createDataCenterTable()
  myDB.createNodeTable()
  myDB.createTaskTable()
  num_of_dcs = myDB.generateRandomDCs()
  myDB.generateRandomNodes(num_of_dcs)
  # myDB.printDCTable()
  myDB.printNodeTable()

  file = open("log.txt", "w")
  distributeTasksBF(myQueue, myDB, file)
  myDB.printStatus(file)
  #myDB.commitDB()


if __name__== "__main__":
  main()
