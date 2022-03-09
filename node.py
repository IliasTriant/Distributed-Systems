#!/usr/bin/env python3

import hashlib 
import socket
import threading
import pickle
import time

from client import Client


class Node (object) : 
	def __init__(self, ip, port):
		#with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
		#	s.connect(("8.8.8.8", 80))
		#	ipp = s.getsockname()[0]
		#	s.close()
		self.port = port
		self.ip = ip
		f = str(ip) + ":" + str(port)
		hashed = hashlib.sha1(f.encode()) 
		self.id = hashed.hexdigest()
		self.predid, self.predport, self.predip = self.id, self.port, self.ip
		self.succid, self.succport, self.succip = self.id, self.port, self.ip
		self.data = {}
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		#self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		
                #print(ipp)
		self.sock.bind((ip, port))

		self.sock.listen(10)
		self.functions = { 'insert' : self.insert, 'inserted' : self.inserted, 'delete' : self.delete, 'deleted' : self.deleted, 'query' : self.query, 'query_found' : self.query_found, 				'update_pred' : self.update_pred, 'update_succ' : self.update_succ, 'join' : self.join, 'find_where' : self.find_where, 'take_data' : self.take_data, 'depart' : 				self.depart, 'send_data' : self.send_data, 'receive_data' : self.receive_data, 'destroy' : self.destroy, 'all_values' : self.all_values, 'overlay' : self.overlay, 'print_data' : self.print_data, 'add_replica' : self.add_replica, 'delete_replica' : self.delete_replica, 'query_replica': self.query_replica}
		self.dep = False
		self.threads = []
		self.message = {}
		self.countmessage = 0
		self.readmessage = 0
		
	def __del__(self):
		self.sock.close()	
	
	def insert (self, message):
		key,value,start, replica, repl, startip = message.split('+')[1], message.split('+')[2], message.split('+')[3], message.split('+')[4], message.split('+')[5], message.split('+')[6]
		replica = int(replica)
		#print (key, value, start)
		hashed_key = hashlib.sha1(key.encode()).hexdigest()
		if (hashed_key > self.predid and hashed_key<= self.id) or (hashed_key<= self.id and self.id <= self.predid) or (self.id<= self.predid and hashed_key >= self.predid):
			#key is for this client
			answer = self.data.pop(hashed_key, (None, None)) #in case that this key exists
			self.data[hashed_key] = (key, value)
			print ("I have inserted the " , key, "-", value)
			#send to start that data has been inserted, id
			if (self.port!=start):
				start = int(start)
				with Client(startip, start) as c : 
					message = "inserted" + '+' + str(self.id) + '+' + str(self.port)
					c.communication(message)
			else : 
				message = "inserted" + '+' + str(self.id) + '+' + str(self.port) 
				self.inserted(message, False)
			if (replica != 1 and repl == 'eventual'):
				with Client(self.succip, self.succport) as c:
					message = "add_replica" + '+' + str(key) + '+' + str(value) + '+' + str(replica-1) + '+' + repl
					c.communication(message, False)
					
			if (replica != 1 and repl == 'linear'):
				with Client(self.succip, self.succport) as c:
					message = "add_replica" + '+' + str(key) + '+' + str(value) + '+' + str(replica-1) + '+' + repl
					c.communication(message)
				
			#self.message[self.countmessage] = "insert"
			#self.countmessage += 1
		else : 
			with Client(self.succip, self.succport) as c:
				message = "insert" + '+' + str(key) + '+' + str(value) + '+' + str(start) + '+' + str(replica) + '+' + repl + '+' + startip
				c.communication(message)
				
		self.message[self.countmessage] = "insert"
		self.countmessage += 1
			
			
	def add_replica (self, message): 
		#time.sleep(1)
		key, value, replica, repl = message.split('+')[1], message.split('+')[2], message.split('+')[3], message.split('+')[4]
		replica = int(replica)
		hashed_key = hashlib.sha1(key.encode()).hexdigest()
		answer = self.data.pop(hashed_key, (None, None)) #in case that this key exists
		self.data[hashed_key] = (key, value)
		if ((replica-1) != 0 and repl == 'eventual'): 
			with Client(self.succip, self.succport) as c:
				message = "add_replica" + '+' + str(key) + '+' + str(value) + '+' + str(replica-1) + '+' + repl
				c.communication(message, False)
		elif ((replica-1) != 0 and repl == 'linear'): 
			with Client(self.succip, self.succport) as c:
				message = "add_replica" + '+' + str(key) + '+' + str(value) + '+' + str(replica-1) + '+' + repl
				c.communication(message)
		if (repl == 'linear') : 
			self.message[self.countmessage] = "linearazability"
			self.countmessage += 1
		print("I am port = ", self.port, "with id = ", self.id, "and I have inserted the key = ", key, "with value = ", value)
		
			
	def inserted (self, message, flag = True): 
		id, port = message.split('+')[1], message.split('+')[2]
		print("I am port = ", self.port, "with id = ", self.id, "and the key has been inserted by node with id = ", id, "and port = ", port)
		if (flag == True): 
			self.message[self.countmessage] = "inserted"
			self.countmessage += 1
			
	def delete (self, message) :
		key, start, replica, repl, startip = message.split('+')[1], message.split('+')[2], message.split('+')[3], message.split('+')[4], message.split('+')[5]
		replica = int(replica)
		hashed_key = hashlib.sha1(key.encode()).hexdigest()
		if (hashed_key > self.predid and hashed_key<= self.id) or (hashed_key<= self.id and self.id <= self.predid) or (self.id<= self.predid and hashed_key >= self.predid):
			#key is here
			answer = self.data.pop(hashed_key, (None, None))
			#print ("answer is ", answer)
			if (answer[0] is None) : 
				print ("key = ", key, " was not found in the ring")
			else : 
				print ("I have deleted the " , key)
				#send to start that data has been deleted, id
				if (self.port!=start):
					start = int(start)
					with Client(startip, start) as c : 
						message = "deleted" + '+' + str(self.id) + '+' + str(self.port) 
						c.communication(message)
				else : 
					message = "deleted" + '+' + str(self.id) + '+' + str(self.port)
					self.deleted(message, False)
				if (replica != 1 and repl == 'eventual'):
					with Client(self.succip, self.succport) as c:
						message = "delete_replica" + '+' + str(key) +  '+' + str(replica-1) + '+' + repl
						c.communication(message, False)
				if (replica != 1 and repl == 'linear'):
					with Client(self.succip, self.succport) as c:
						message = "delete_replica" + '+' + str(key) + '+' + str(replica-1) + '+' + repl
						c.communication(message)
				
		else : 
			#send to succ
			with Client(self.succip, self.succport) as c:
				message = "delete" + '+' + str(key) + '+' + str(start) + '+' + str(replica) + '+' + repl + '+' + startip
				c.communication(message)
		self.message[self.countmessage] = "delete"
		self.countmessage += 1
		
	def delete_replica (self, message): 
		#time.sleep(1)
		key, replica, repl = message.split('+')[1], message.split('+')[2], message.split('+')[3]
		replica = int(replica)
		hashed_key = hashlib.sha1(key.encode()).hexdigest()
		answer = self.data.pop(hashed_key, (None, None)) 
		if ((replica-1) != 0): 
			with Client(self.succip, self.succport) as c:
				message = "delete_replica" + '+' + str(key) + '+' + str(replica-1) + '+' + repl
				c.communication(message, False)
		elif ((replica-1) != 0 and repl == 'linear'): 
			with Client(self.succip, self.succport) as c:
				message = "delete_replica" + '+' + str(key) + '+' + str(replica-1) + '+' + repl
				c.communication(message)
		if (repl == 'linear') : 
			self.message[self.countmessage] = "linearazability"
			self.countmessage += 1
		print("I am port = ", self.port, "with id = ", self.id, "and I have deleted the key = ", key)
	
			
	def deleted (self, message, flag = True): 
		id, port = message.split('+')[1], message.split('+')[2]
		print("I am port = ", self.port, "with id = ", self.id, "and the key has been deleted by node with id = ", id, "and port = ", port)
		if (flag == True):
			self.message[self.countmessage] = "deleted"
			self.countmessage += 1
			
			
	def query (self, message) :
		#print(self.data)
		key, start, counter, replica, repl, startip = message.split('+')[1], message.split('+')[2], message.split('+')[3], message.split('+')[4], message.split('+')[5], message.split('+')[6]
		replica = int(replica)
		start = int(start)
		hashed_key = hashlib.sha1(key.encode()).hexdigest()
		counter = int(counter)
		if (key != "*" and hashed_key in self.data and repl == 'eventual' and replica!=1):
			value = self.data.get(hashed_key, None)
			if (self.port!=start):
				with Client(startip, start) as c : 
					message = "query_found" + '+' + str(self.id) + '+' + str(self.port) + '+' + str(value[1]) + '+' + str(value[0])
					c.communication(message)
			else : 
				message = "query_found" + '+' + str(self.id) + '+' + str(self.port) + '+' + str(value[1]) + '+' + str(value[0])
				self.query_found(message)
		elif (key != "*"):
			if (hashed_key > self.predid and hashed_key<= self.id) or (hashed_key<= self.id and self.id <= self.predid) or (self.id<= self.predid and hashed_key >= self.predid):
				if (hashed_key in self.data and replica != 1 and repl == 'linear'):
					with Client(self.succip, self.succport) as c:
						message = "query_replica" + '+' + str(key) + '+' + str(start) + '+' + str(replica-1) + '+' + repl + '+' + startip
						c.communication(message)
				elif (hashed_key in self.data) : 
					value = self.data.get(hashed_key, None)
					print ("I have found the " , value[0])
					#send to start the value, id
					if (self.port!=start):
						with Client(startip, start) as c : 
							message = "query_found" + '+' + str(self.id) + '+' + str(self.port) + '+' + str(value[1]) + '+' + str(value[0])
							c.communication(message)
					else : 
						message = "query_found" + '+' + str(self.id) + '+' + str(self.port) + '+' + str(value[1]) + '+' + str(value[0])
						self.query_found(message, False)
				else : 
					print ("key doesn't exist")
					
			else : 
				#send to succ
				with Client(self.succip, self.succport) as c:
					message = "query" + '+' + str(key) + '+' + str(start) + '+' + str(counter) + '+' + str(replica) + '+' + repl + '+' + startip
					c.communication(message)
				
		else : 
			#send to succ
			if (counter == 0 and self.port==start) or self.port!=start : #first node or non full cycle
				counter += 1
				if (len(self.data) != 0):
					for keys in self.data:
						value = self.data.get(keys, None)
						if (self.port!=start):
							with Client(startip, start) as c : 
								message = "all_values" + '+' + str(self.id) + '+' + str(self.port) + '+' + str(value[1]) + '+' + str(value[0])
								c.communication(message)
						else : 
							message = "all_values" + '+' + str(self.id) + '+' + str(self.port) + '+' + str(value[1]) + '+' + str(value[0])
							self.all_values(message, False)
				with Client(self.succip, self.succport) as c:
					message = "query" + '+' + str(key) + '+' + str(start) + '+' + str(counter) + '+' + str(replica) + '+' + repl
					c.communication(message)
				
			elif (self.port == start) : #a full cycle
				print ("I have printed all the key-values")
		self.message[self.countmessage] = "query"
		self.countmessage += 1
	
	def query_replica (self, message): 
		#time.sleep(1)
		key, start, replica, repl, startip = message.split('+')[1], message.split('+')[2], message.split('+')[3], message.split('+')[4], message.split('+')[5]
		start = int(start)
		replica = int(replica)
		hashed_key = hashlib.sha1(key.encode()).hexdigest() 
		if ((replica-1) != 0): 
			with Client(self.succip, self.succport) as c:
				message = "query_replica" + '+' + str(key) + '+' + str(start) + '+' + str(replica-1) + '+' + repl + '+' + startip
				c.communication(message)
		elif (hashed_key in self.data) : 
			value = self.data.get(hashed_key, None)
			print ("I have found the " , value[0])
			#send to start the value, id
			if (self.port!=start):
				with Client(startip, start) as c : 
					message = "query_found" + '+' + str(self.id) + '+' + str(self.port) + '+' + str(value[1]) + '+' + str(value[0])
					c.communication(message)
			else : 
				message = "query_found" + '+' + str(self.id) + '+' + str(self.port) + '+' + str(value[1]) + '+' + str(value[0])
				self.query_found(message, False)
		self.message[self.countmessage] = "query_replica"
		self.countmessage += 1
		#print("I am port = ", self.port, "with id = ", self.id, "and I have found the key = ", key)
				
	def query_found(self, message, flag = True):
		id, port, value, key = message.split('+')[1], message.split('+')[2], message.split('+')[3], message.split('+')[4]
		print("I am port = ", self.port, "with id = ", self.id, "and the key = ", key, "has been found by node with id = ", id, "and port = ", port, "and it is = ", value)
		if (flag==True):
			self.message[self.countmessage] = "query_found"
			self.countmessage += 1
		
	def all_values (self, message, flag = True) : 
		id, port, value, key = message.split('+')[1], message.split('+')[2], message.split('+')[3], message.split('+')[4]
		print("I am port = ", self.port, "with id = ", self.id, "and the node with id = ", id, "and port = ", port, "has the key = ", key, "with the value = ", value)
		if (flag == True):
			self.message[self.countmessage] = "all_values"
			self.countmessage += 1
		
	def join(self, masterport) :
		with Client('192.168.0.4', masterport) as c:
			message = "find_where" + '+' + str(self.id) + '+' + str(self.port) + '+' + str(self.ip)
			#print(message.split('+')[1])
			x = c.communication(message)
			x = x.decode()
			x = x.split('+')
			#print(x)
		message = "hh" + '+' + x[0] + '+' + x[1] + '+' + x[4]
		self.update_pred(message, True)
		message = "hh" + '+' + x[2] + '+' + x[3] + '+' + x[5]
		self.update_succ(message, True)
		with Client(self.predip, self.predport) as c:
			message = "update_succ" + '+' + str(self.id) + '+' + str(self.port) + '+' + str(self.ip)
			c.communication(message)
		#print ("ll")
		with Client(self.succip, self.succport) as c:
			message = "update_pred" + '+' + str(self.id) + '+' + str(self.port) + '+' + str(self.ip)
			c.communication(message)
		#with Client(self.succip, self.succport) as c:
			#print("lol")
		#	message = "take_data" + '+' + str(self.id) + '+' + str(self.port) 
		#	data = c.communication(message)
			#print(data)
		#	data = pickle.loads(data)
			#print(data)
		#for key in data : 
		#	self.data[key] = data[key]
		
		
		
	def find_where(self, message) :
		#print(message) 
		id, port, ip = message.split('+')[1], message.split('+')[2], message.split('+')[3]
		#print(id, self.predid, self.id)
		if (id > self.predid and id<= self.id) or (id<= self.id and self.id <= self.predid) or (self.id<= self.predid and id >= self.predid):
			#print("Hi")
			self.message[self.countmessage] = str(self.predid) + '+' + str(self.predport) + '+' + str(self.id) + '+' + str(self.port) + '+' + str(self.predip) + '+' + str(self.ip) 
			self.countmessage += 1
			message = "hh" + '+' + id + '+' + port + '+' + ip
			self.update_pred(message, True)
			#print("hh")
			
			#return (self.predid, self.predport, self.id, self.port)
		else : 
			#print("hI")
			with Client(self.succip, self.succport) as c:
				#print("hi")
				message = "find_where" + '+' + str(id) + '+' + str(port) + '+' + str(ip)
				x = c.communication(message)
				self.message[self.countmessage] = x
				self.countmessage += 1
			
				 
	def take_data (self, message): 
		id, port = message.split('+')[1], message.split('+')[2]
		data = {}
		#print ("I am " , self.port)
		for key in self.data : 
			if (key > self.predid and key<= id) or (key<= id and id <= self.predid) or (id<= self.predid and id >= self.predid):
				data[key]=self.data[key]
				del self.data[key]
		self.message[self.countmessage] = pickle.dumps(data)
		self.countmessage += 1
		#print (self.message)
		
	def depart (self, message) : 
		masterport = message.split('+')[1]
		self.send_data()
		with Client(self.predip, self.predport) as c:
			message = "update_succ" + '+' + str(self.succid) + '+' + str(self.succport) + '+' + str(self.succip) 
			c.communication(message)
		with Client(self.succip, self.succport) as c:
			message = "update_pred" + '+' + str(self.predid) + '+' + str(self.predport) + '+' + str(self.predip) 
			c.communication(message)
		with Client('192.168.0.4', int(masterport)) as c:
			message = "depart" 
			c.communication(message)
		self.dep = True
		self.message[self.countmessage] = "time to depart"
		self.countmessage += 1
		#self.dep = True
	
	def destroy (self, message) : 
		self.depart(message)
		with Client(self.succip, self.succport) as c:	
			c.communication(message, False)		
			
	def update_pred (self, message, flag = False): 
		pred_id, pred_port, pred_ip = message.split('+')[1], message.split('+')[2], message.split('+')[3]
		self.predid = pred_id
		self.predport = int(pred_port)
		self.predip = pred_ip
		print ("I am ", self.port, "and I have predport ", self.predport, "with ip ", self.predip)
		if (flag == False) :
			self.message[self.countmessage] = "I did it"
			self.countmessage += 1
		
	def update_succ (self, message, flag = False): 
		succ_id, succ_port, succ_ip = message.split('+')[1], message.split('+')[2], message.split('+')[3]
		self.succid = succ_id
		self.succport = int(succ_port)
		self.succip = succ_ip
		print ("I am ", self.port, "and I have succport ", self.succport, "with ip ", self.succip)
		if (flag == False) :
			self.message[self.countmessage] = "I did it"
			self.countmessage += 1
		
	def send_data (self): 
		with Client(self.succip, self.succport) as c:
			#data = pickle.dumps(self.data)
			#print(data)
			for key in self.data : 
				value = self.data[key]
				message = "receive_data" + '+' + str(key) + '+' + str(value[0]) + '+' + str(value[1]) + '+' + str(self.port) + '+' + str(self.id)
			#print(message)
				c.communication(message)
		
	def receive_data (self, message) : 
		hashedkey, key, value, port, id = message.split('+')[1], message.split('+')[2], message.split('+')[3], message.split('+')[4], message.split('+')[5]
		#print("have values " , value[0], value[1])
		#data = pickle.loads(bytes(data, encoding='utf8'))
		#print(data)
		if (self.id == id and self.port == port) :
			print ("I am alone in the ring")
		else : 
			if self.data.get(hashedkey, None) != (key, value):
				self.data[hashedkey] = (key, value)
				#print("my data", self.data)
				print ("I am port = ", self.port, "with id = ", self.id, "and I received key = ", key, "with value = ", value, "from node with id = ", id, "and port = ", port)
			else : 
				with Client(self.succip, self.succport) as c:
					message = "receive_data" + '+' + str(hashedkey) + '+' + str(key) + '+' + str(value) + '+' + str(port) + '+' + str(id)
			#print ("I received all the data")
		self.message[self.countmessage] = "received a key-value pair"
		self.countmessage += 1
			
			
	def overlay (self, message) : 
		start, counter, oldmes, startip = message.split('+')[1], message.split('+')[2], message.split('+')[3], message.split('+')[4]
		start = int(start)
		counter = int(counter)
		if (self.port != start or counter == 0) : 
			counter = 1
			if (self.port!=start):
				with Client(startip, start) as c : 
					message = "print_data" + '+' + str(self.port) + '+' + str(self.succport) + '+' + str(oldmes)
					newmes = c.communication(message)
					newmes = newmes.decode()
			else : 
				message = "print_data" + '+' + str(self.port) + '+' + str(self.succport) + '+' + str(oldmes)
				#print(message)
				newmes = self.print_data(message, False)
			#print ("I am ", self.port, "and I have succesor = ", self.succport)
			with Client(self.succip, self.succport) as c:
				message = "overlay" + '+' + str(start) + '+' + str(counter) + '+' + str(newmes) + '+' + startip	
				x = c.communication(message)
				x = x.decode()
				self.message[self.countmessage] = x
				self.countmessage += 1
		else : 
			counter = 2
			#f = open("overlay.txt", "w")
			#f.write("there is no someone else :( \n")
			#f.close()
			print ("there is no someone else :( ")
			#print(oldmes + '/' + "there is no someone else :( ")
			newmes = "there is no someone else :( "
		#if (self.port != start or counter == 2):
			#print ("I am port ", self.port, " and I send a message")
			self.message[self.countmessage] = oldmes + '/' + newmes
			self.countmessage += 1
			
	def print_data(self, message, flag = True) : 
		port, succ, oldmes = message.split('+')[1], message.split('+')[2], message.split('+')[3]
		print ("I am ", port, "and I have succesor = ",succ)
		message = oldmes + '/' + "I am " + port + " and I have succesor = " + succ
		if (flag==True):
			self.message[self.countmessage] = message
			self.countmessage += 1
		else :
			return message
		#f = open("overlay.txt", "w")
		#f.write("I am ", port, "and I have succesor = ",succ, "\n")
		#f.close()
			
	def connection(self):
		print("connect")
		while True:
			connect, _ = self.sock.accept()
			self.threads.append(threading.Thread(target=self.thread_fuc, args=(connect,)))
			self.threads[-1].start()
			if self.dep:
				print ("bye") 
				break
				
	def thread_fuc (self, sock) : 
		while (True) : 
			data = sock.recv(4096)
			if not data: 
				break
			data = data.decode()
			oper = self.functions.get(data.split('+')[0])
			#print(data)
			oper(data)
			if (self.countmessage != self.readmessage): 
				#print (self.message[self.readmessage])
				if isinstance(self.message[self.readmessage], (bytes, bytearray)):
					sock.send(self.message[self.readmessage])
				else : 
					sock.send(self.message[self.readmessage].encode())
				
				if self.message[self.readmessage] == 'time to depart':
					sock.close()
					#print ("hh")
					return
				self.readmessage += 1

	
	
	
	
	
	
########### BOOTSTRAP NODE #####################	
	
	
	
		
		
class bootstrap (Node) : 
	def __init__(self, ip, port):
		self.size = 1
		print("bootstrap")
		super(bootstrap, self).__init__(ip, port)
		
	def find_where(self, message):
		#id, port = message.split('+')[1], message.split('+')[2]
		self.size += 1
		super(bootstrap, self).find_where(message)

	def depart(self, message):
		self.size -= 1
		self.message[self.countmessage] = 'Node departed'
		self.countmessage += 1
'''		
	def destroy (self) : 
		if self.size > 1:
			with Client(self.succport) as c:
				message = "destroy" + '+' + str(self.port) 
				c.communication(message, False)
		else:
			print ("network is destroyed")
			self.close = True
'''
