# usage: python client.py <nodes.txt> <ipconfig.txt> <processID>
import socket
import sys, os, time
from threading import Thread
import Queue
import pickle
import shlex


#global CREATED = 0
#global DELETED = 1
#global REQUESTED = 2

	# list of HolderObj objects that exist in the connect graph
g_holderobjlist = []
	# list of Token objects 
g_tokenobjlist = []
	# this node's id number;  currently defaulted at -1
g_idnum = -1
	# This list has all neighbor node ids (not including itself)
g_neighbornodes = []

# unnecessary
class MyThread (Thread):
	def __init__(self, PORT, adj_list, my_idnum, adj_idnum, socketQueue):
		Thread.__init__(self)
		self.PORT = PORT
		self.adj_list = adj_list
		self.my_idnum = my_idnum
		self.adj_idnum = adj_idnum
		self.socketQueue = socketQueue

	def run(self):
		#print "Connecting..."
		if self.my_idnum < self.adj_idnum:
			self.socketQueue.put([self.adj_idnum, connect(self.PORT, self.adj_list, self.my_idnum, self.adj_idnum)])
		else:
			self.socketQueue.put([self.adj_idnum, listen(self.PORT, self.adj_list, self.my_idnum, self.adj_idnum)])
		#print "Finished"

class Thefiletoken():
    def __init__(self, name, content):
        self.name = name
        self.content = content

    def info(self):
        return ("Filename: " + self.name + "\n"
                "Content: " + "\n"
                + self.content)

    def filename(self):
        return "%s" %(self.name)

    def filecontents(self):
        return "%s" %(self.content)

    def appendfile(self, msgline):
    	if (self.content == ""):
    		self.content = "%s" %(msgline)
    	else:
    		self.content = "%s\n%s" %(self.content, msgline)


#nodeid is an integer
#asked is an integer
#use is an integer
#OUTPUT of nid() and asked() and using() is str (Must convert to int)
class Holderalg():
    def __init__(self, filename, nodeid, ask, use, requeue, commandq):
        self.filename = filename
        self.nodeid = nodeid
        self.ask = ask
        self.use = use
        self.requeue = requeue
        self.commandq = commandq

    def fname(self):
        return "%s" %(self.filename)

    def nid(self):
        return int(self.nodeid)

    def edit_nid(self, newval):
        self.nodeid = newval

    #ASKED BOOLEAN variable (When THIS nonpriviledged node X sent REQUEST msg to current holder)
    def asked(self):
        return int(self.ask)

    def edit_asked(self, newval):
        self.ask = newval


    def using(self):
        return int(self.use)

    def edit_using(self, newval):
        self.use = newval

        
    def reqQsize(self):
        return int(self.requeue.qsize())

    def reqQput(self, val):
        self.requeue.put(val)

    def reqQget(self):
        return self.requeue.get()

    # Whenenver self is put into reqQ, the corresponding command is put into this queue
    def commandQput(self, val):
        self.commandq.put(val)

    def commandQget(self):
        return self.commandq.get()

# read tree.txt & ipconfig.txt to determine the nodes to connect to
def connect(PORT, adj_list, my_idnum, adj_idnum):
	'''
	this function will try to connect a socket connection between the 
	current id and another idnum designated with the parameter adj_idnum

	'''
	if my_idnum < adj_idnum:
		IP = adj_list[adj_idnum]
		MESSAGE = 'PID: %d' % my_idnum
		s = socket.socket()
	
		while True:
			try:
				s.connect((IP, PORT))
				break
			except:
				#print 'Waiting for connection. Trying again in 3 seconds...'
				time.sleep(1)
				continue
		#s.send(MESSAGE)
		#data = s.recv(1024)
		#print 'received: ', data
		return s

def listen(PORT, adj_list, my_idnum, adj_idnum):
	
	if my_idnum > adj_idnum:
		IP = adj_list[adj_idnum]
		MESSAGE = 'PID: %d' % my_idnum
		s = socket.socket()
		conn = socket.socket()
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		try:
			s.bind(('', PORT))
		except:
			print "ERROR: Cannot bind ports yet, wait a little bit..."
			os._exit(1)
		s.listen(5)

		#while True:
			#print 'Waiting for connection...'
		conn, addr = s.accept()
			#print 'got connection from ', addr
			#data = conn.recv(1024)
			#conn.send(MESSAGE)
		#if conn: break
		#print 'received: ', data
		s.close()
		return conn
	
def initializeNodes(treefile, ipfile):
	
	id_num = int(sys.argv[3])
	
	def parseFiles(id_num, treefile, ipfile):

		adj_list = {}
		with open(treefile) as f:
			for line in f:
				# if theres a # at beginning of line, its a comment
				# this code can be removed for final product
				if line[0] == '#':
					continue
				if int(line[1]) == id_num:
					adj_list[int(line[3])] = ''
				elif int(line[3]) == id_num:
					adj_list[int(line[1])] = ''
		
		with open(ipfile) as f:
			for line in f:
				# if theres a # at beginning of line, its a comment
				# this code can be removed for final product
				if line[0] == '#':
					continue			
				line = line.replace('(', '').replace(')', '').strip().split(',')
				if int(line[0]) in adj_list:
					adj_list[int(line[0])] = line[1]

		return adj_list
	
	def uniquePORT(id_num, id_node):
		str_port = '60'
		if id_num > id_node:
			str_port += str(id_num) + str(id_node)
		else:
			str_port += str(id_node) + str(id_num)

		return int(str_port)


	#### CODE HERE TO PARSE FILES FOR CURRENT PROCESSID #####


	# real stuff
	adj_list = parseFiles(id_num, treefile, ipfile)

	# iterate through nodes
	threads = []

	socketQueue = Queue.Queue()
	sockets = {}

	for node in adj_list:
		# have a unique port for each connection in case of same IP addresses
		PORT = uniquePORT(id_num, node)
		#thread = myThread(PORT, adj_list, id_num, node, socketQueue)
		thread = Thread(target=lambda q, PORT, adj_list, id_num, node: 
			q.put([node, connect(PORT, adj_list, id_num, node)]) if id_num < node else 
			q.put([node, listen(PORT, adj_list, id_num, node)]), 
			args=(socketQueue, PORT, adj_list, id_num, node))
		thread.start()
		threads.append(thread)
		#s = connect(PORT, adj_list, id_num, node)
		#c = listen(PORT, adj_list, id_num, node)

	for thread in threads:
		thread.join()

	while not socketQueue.empty():
		result = socketQueue.get()
		sockets[result[0]] = result[1]


	return id_num, adj_list, sockets

def getReadys(sockets):
	foo=1

def printSockets(sockets):
	for key, value in sockets.iteritems():
		print key, value.getpeername()

def criticalsection(filename, sockets, command):
	if (command[0] == "delete"):
		print "Achieved Critical Section, will delete token %s" % (filename)
		#delete holder, delete token, send msg to all saying token got deleted
		#print "Printing objlist and holderlist Before"
		#for obj in g_holderobjlist:
		#	print obj.fname()
		#for item in g_tokenobjlist:
		#	print item.filename()

		deleteholderobj(filename)
		deletetokenobj(filename)
		#print "Printing objlist and holderlist After"
		#for obj in g_holderobjlist:
		#	print obj.fname()
		#for item in g_tokenobjlist:
		#	print item.filename()

		#2nd parameter 1 means delete notification
		holdermsg = [g_idnum, 1, filename]
		#send holdermsg delete notification to all other neighboring nodes
		print "Now sending message that token has been deleted"
		for nodeids in g_neighbornodes:
			createMessage2(holdermsg, sockets, nodeids)	

	if (command[0] == "read"):
		print "Achieved Critical Section, will read out contents:"
		for item in g_tokenobjlist:
			if (item.filename() == filename):
				print item.filecontents()
				print '\n'

	if (command[0] == "append"):
		print "Achieved Critical Section, will append, then print out updated contents:"
		for item in g_tokenobjlist:
			if (item.filename() == filename):
				item.appendfile(command[1])
				print item.filecontents()
				print '\n'

#command is an array
def assignpriviledge(filename, sockets):
	# ENTER ASSIGN_PRIVILEDGE
	print "Now in Assign Priviledge."

	#print "filename parameter: %s" %(filename)
	for obj in g_holderobjlist:
		if (obj.fname() == filename):
			print "Filename matched: %s" %(filename)
			#checking if holder == self && !using && reqQ not empty
			#print "object's nid holder"
			#print obj.nid()
			#print g_idnum
			if(obj.nid() == g_idnum and obj.using() == 0 and obj.reqQsize() != 0):
				print "Current node has token, Current node not using Token, and Current node has requests in ReqQ"
				#dequeue Head of Queue, and set it to nid value of holderalg object
				obj.edit_nid(obj.reqQget())
				#print "HOLDERid = %d" %(obj.nid())
				#asked = false
				obj.edit_asked(0)
				#if HOLDER == self, then get Head of CommandQ to get command
				if (obj.nid() == g_idnum):
					print "Node has Token and is Head of ReqQ, so it can now enter critical section"
					#only extract commandQ when node is about to go into critical section
					act = obj.commandQget()
					#USING = true
					obj.edit_using(1)
					#entry into CRITICAL SECTION
					criticalsection(filename,sockets,act)

					#Exiting CRITICAL SECTION
					#using = 
					print "Node has exited critical section"
					obj.edit_using(0)
					assignpriviledge(filename,sockets)
					makerequest(filename,sockets)
				else:
					#this means current node not head of request queue, so pass along token to next node
					#Send token to holder noid				
					for toks in g_tokenobjlist:
						if (toks.filename() == obj.fname()):
							#send token info to node with holder nid, then remove token from list and delete
							#second parameter 3 == sending a token
							holdermsg = [g_idnum, 3, toks]
							createMessage2(holdermsg, sockets, obj.nid())
							break
					deletetokenobj(filename)
					
def makerequest(filename, sockets):
	# MAKE_REQUEST TOKEN
	# find this Holderobject from list
	#print "Now in Make Request."
	for obj in g_holderobjlist:
		if (obj.fname() == filename):			
			#checking for !self && reqQ not empty && asked = false
			if (obj.nid() != g_idnum and obj.reqQsize() != 0 and obj.asked() == 0):
				print "Make Request: Node does not have Token, Node has requests in reqQ, and Node has not asked before"
				#Send REQUEST to HOLDER
				#Note on second parameter:  0 = create   1 = delete   2 = request  3 = token
				#print "parts of msg being sent"
				#print g_idnum
				#print filename
				holdermsg = [g_idnum, 2, filename]
				#send holdermsg to the node with holder node id
				for nodeids in g_neighbornodes:
					print nodeids
					print obj.nid()
					if (nodeids == obj.nid()):
						print "About to send to node %d" %(nodeids)
						createMessage2(holdermsg, sockets, nodeids)
						break
				# set ASKED to true
				obj.edit_asked(1)

def deleteholderobj(filename):
	#deleting holderobj from records.
	counter = 0
	for obj in g_holderobjlist:
		if (filename == obj.fname()):
			break
		counter += 1
	del g_holderobjlist[counter]

def deletetokenobj(filename):
	#deleting tokenobj from records.
	counter = 0
	for obj in g_tokenobjlist:
		if (filename == obj.filename()):
			break
		counter += 1
	del g_tokenobjlist[counter]		


def listenSocket(sock, filelist, sockets):
	global g_holderobjlist
	global g_tokenobjlist
	global g_idnum
	global g_neighbornodes

	while True:
		try:
			data = sock.recv(1024)

			unpackinggoods = pickle.loads(data)
			print 'Received message: '
			# received array (unpackinggoods):  [<nodeid>, <0,1,2,3>, <tokenobject or filename>]
			# 0 = create   1 = delete   2 = request  3 = token
			if (int(unpackinggoods[1]) == 0):
				# 0 = create
				#Since a new token has been created, create Holderalg object for that token
				newqueue = Queue.Queue()
				newcq = Queue.Queue()
				# note 2nd argument of Holderalg is set the nodeid where message came from
				newholder = Holderalg(unpackinggoods[2], unpackinggoods[0], 0, 0, newqueue, newcq)
				print "Holderalg Object created--> Filename: " + str(unpackinggoods[2]) + "; and  Pointing to NodeId: " + str(unpackinggoods[0])
				#recording new Holderalg object
				g_holderobjlist.append(newholder)

				#send message to all neighbor nodes other than the one it just received from
				for nids in g_neighbornodes:
					if (nids == int(unpackinggoods[0])):
						continue
					else:
						#notice nodeid is this node's id,... create, filename
						# 0 = create
						holdermsg = [g_idnum, 0, unpackinggoods[2]]
						#assert (unpackinggoods[2] == newholder.filename)
						createMessage2(holdermsg, sockets, nids)

			elif (int(unpackinggoods[1]) == 1):
				# 1 = delete  (token got deleted, so remove that object holderobj from records)
				print "Holderalg Object deleted--> Filename: " + str(unpackinggoods[2]) + "; and Received message from Node: " + str(unpackinggoods[0])
				deleteholderobj(unpackinggoods[2])

				print "Now passing Delete notification to neighboring nodes:"

				#send message to all neighbor nodes other than the one it just received from
				#print "Printing neighboring nodes"
				for nids in g_neighbornodes:
					#print nids
					if (nids == int(unpackinggoods[0])):
						continue
					else:
						#notice nodeid is this node's id,... create, filename
						# 1 = delete 
						print "Sending messgages to node: %d" %(nids)
						holdermsg = [g_idnum, 1, unpackinggoods[2]]
						createMessage2(holdermsg, sockets, nids)

			elif (int(unpackinggoods[1]) == 2):
				# 2 = request (receives request message)
				#print 'ABOUT TO PRINT GOODS[2]'
				#print str(unpackinggoods[2])
				print 'This node got Request Msg from Node: ' + str(unpackinggoods[0])
				#enqueue (REQUEST-Q, X); ASSIGN-PRIVILEGE; MAKE-REQUEST 
				# put Node X into request Q
				#print "Received unpackinggoods filename: %s" %(unpackinggoods[2])
				for obj in g_holderobjlist:
					#find corresponding holderobject of requested token
					if (obj.fname() == unpackinggoods[2]):
						#put the requesting nodeid into Queue
						obj.reqQput(unpackinggoods[0])

				#Assign Priviledge
				assignpriviledge(unpackinggoods[2], sockets)
				#Make Request
				makerequest(unpackinggoods[2], sockets)

			elif (int(unpackinggoods[1]) == 3):
				# 3 = token (receives privieledge message)
				print 'This node got Token Priviledge Msg from Node: ' + str(unpackinggoods[0])
# received array (unpackinggoods):  [<nodeid>, <0,1,2,3>, <tokenobject or filename>]				
				#HOLDER := self; ASSIGN-PRIVILEGE; MAKE-REQUEST
				#record token
				g_tokenobjlist.append(unpackinggoods[2])
				#HOLDER := self
				for item in g_holderobjlist:
					if (item.fname() == unpackinggoods[2].filename()):
						item.edit_nid(g_idnum)

				#Assign Priviledge
				assignpriviledge(unpackinggoods[2].filename(), sockets)
				#Make Request
				makerequest(unpackinggoods[2].filename(), sockets)



			else:
				# error
				print 'Error in receiving message ERR'

			# parse the data (action|filename|node_id)
			#parsed_data = data.split("|")
			#if parsed_data[0] == 'create':
			#	filelist.append(parsed_data[1])

		except:
			break
		if not data: break
	
		#print "Received: ", data
	

def listenSockets(sockets, filelist):
	threads = []
	for key, value in sockets.iteritems():
		thread = Thread(target = listenSocket, args=(value, filelist, sockets))
		thread.daemon = True
		thread.start()
		threads.append(thread)


	return threads
	#for thread in threads:
	#	thread.join()

def sendMessage(message, node_id, sockets):
	# sends a message to a node

	# parse the message ("message")
	#print message
	msg_str = ''
	for i in message:
		msg_str+= i + ' '
	if node_id not in sockets:
		print "No connection with that node number!"
		return
	sockets[node_id].send(msg_str)

def createMessage(action, filename, id_num, sockets):

	# format message (action|filename|node_id)
	message = action + "|" + filename + "|" + str(id_num)
	print message
	# send the message to all adjacent nodes
	for node, socket in sockets.iteritems():
		sockets[node].send(message)

#Sending objects using pickle
def createMessage2(dummy, sockets, id_num):
	# pickle the dummy
	data_string = pickle.dumps(dummy)
	sockets[id_num].send(data_string)
		#f.close()

def UI():
	message = (
		"\n\t****Welcome to the Distributed System!****\n\n"
		"\t\tWhat would you like to do?\n\n"
		"\tOptions:\n"
		"\t\tcreate          <filename>: creates an empty file name <filename>\n"
		"\t\tdelete          <filename>: deletes file name <filename>\n"
		"\t\tread            <filename>: displays the contents of <filename>\n"
		"\t\tappend  <filename>[<line>]: append a <line> to <filename>\n"
		"\t\tsockets                   : prints the sockets that are currently connected\n"
		"\t\tsend    <message><node_id>: sends a message to node\n"
		"\t\tquit                      : quits the program\n\n"

		"enter a command: "
		)

	action = raw_input(message)

	return action


def Main():
	global g_holderobjlist
	global g_tokenobjlist
	global g_idnum
	global g_neighbornodes

	print "Initializing nodes from tree configuration, be patient..."
	id_num, adj_list, sockets = initializeNodes('tree.txt', 'ipconfig.txt')
	print "done! Waiting for rest of nodes to finish...\n"
	# get acknowledgements from all nodes
	#getReadys(sockets)
	print "...done!\n"

	#copying variable adj_list into global variable g_neighbornodes
	for key in adj_list:
		g_neighbornodes.append(int(key))

	# This is the real value of global variable
	g_idnum = id_num

	#this doesn't do anything
	filelist = []
	threads = listenSockets(sockets, filelist)
	#thread = Thread(target = listenSocket, args=(sockets[2],))
	#thread.start()
	#dummy = ['this is a dummy string', 43]
	#f_token = thefiletoken('foo.txt', 'this is the first line')



						
	while True:
		action = shlex.split(UI())
		
		if action[0] == 'create':
			if len(action) != 2:
				print "Incorrect agruments"
				continue
			
			#Creating new token class and respective new Holderalg class
			newqueue = Queue.Queue()
			newcommandq = Queue.Queue()
			newholder = Holderalg(action[1], g_idnum, 0, 0, newqueue, newcommandq)
			newtoken = Thefiletoken(action[1], "")

			#Recording new Holderalg class
			g_holderobjlist.append(newholder)
			#Recording new token class
			g_tokenobjlist.append(newtoken)

			#Send message out to all neighboring nodes that a token has been created
			#Note on second parameter:  0 = create   1 = delete   2 = request  3 = token
			holdermsg = [g_idnum, 0, action[1]]

			#send holdermsg to all other neighboring nodes
			for nodeids in g_neighbornodes:
				createMessage2(holdermsg, sockets, nodeids)
 
		elif action[0] == 'delete':
			if len(action) != 2:
				print "Incorrect agruments"
				continue

			# Since this node wishes to enter critical section

			#made command array for "append" command convenience
			command = []
			command.append(action[0])

			print "Starting Delete command execution"

			print "printing out g_holderlist:"
			for obj in g_holderobjlist:
				print obj.fname()
			# put self into request Q,  (AND PUT command into comandQ)
			for obj in g_holderobjlist:
				if (obj.fname() == action[1]):
					obj.reqQput(g_idnum)
					obj.commandQput(command)
					print "Put self and command into reqQueue"


			#Assign Priviledge
			assignpriviledge(action[1], sockets)
			#Make Request
			makerequest(action[1], sockets)

		elif action[0] == 'read':
			if len(action) != 2:
				print "Incorrect agruments"
				continue
			# Since this node wishes to enter critical section

			command = []
			command.append(action[0])
			# put self into request Q (AND PUT command into comandQ)
			for obj in g_holderobjlist:
				if (obj.fname() == action[1]):
					obj.reqQput(g_idnum)
					obj.commandQput(command)


			#Assign Priviledge
			assignpriviledge(action[1], sockets)
			#Make Request
			makerequest(action[1], sockets)

		elif action[0] == 'append':
			if len(action) != 3:
				print "Incorrect agruments"
				continue
			# Since this node wishes to enter critical section

			command = []
			command.append(action[0])
			command.append(action[2])
			# put self into request Q (AND PUT command into comandQ)
			for obj in g_holderobjlist:
				if (obj.fname() == action[1]):
					obj.reqQput(g_idnum)
					obj.commandQput(command)


			#Assign Priviledge
			assignpriviledge(action[1], sockets)
			#Make Request
			makerequest(action[1], sockets)





		elif action[0] == 'send':

			if len(action) < 3:
				print "Not enough arguments"
				continue
			sendMessage(action[1:-1], int(action[-1]), sockets)

		elif action[0] == 'quit':
			break

		elif action[0] == 'sockets':
			printSockets(sockets)

		elif action[0] == 'filelist':
			print filelist
		else:
			print 'Nonexisting arguments'
			continue

	
	print "exiting..."
	for node, socket in sockets.iteritems():
		socket.close()
	#for thread in threads:
	#	thread.join(1)
	#sys.exit()

if __name__ == '__main__':
	Main()