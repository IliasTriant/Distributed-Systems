#!/usr/bin/env python3

import random
import threading 

from client import Client
from node import Node, bootstrap


class chordring(object): 
	#nodes = {5000 : '192.168.0.4', 5001 : '192.168.0.4', 5002 : '192.168.0.2', 5003 : '192.168.0.2', 5004 : '192.168.0.6', 5005 : '192.168.0.6', 5006 : '192.168.0.5', 5007: '192.168.0.5', 5008 : '192.168.0.3', 5009: '192.168.0.3'}
	#nodes = {5000 : '192.168.0.4', 5002 : '192.168.0.2', 5004 : '192.168.0.6'}
	#nodes[port]=0
	def __init__(self, ip, port, flag = False):
		self.master_port = port
		#self.nodes = {} #dict of port->ip
		#self.nodes[port]=0
		#chordring.nodes[port]=ip
		self.threads = {}
		if (flag == True): 
			self.threads['0'] = threading.Thread(target=self.dht, args=(ip,port,))
			self.threads['0'].start()
		
	def dht(self, ip, port) : 
		print("dht")
		first_node = bootstrap(ip, port)
		first_node.connection()
		
	def join(self, id, ip, port):
		self.threads[id] = threading.Thread(target=self.new_node, args=(ip, port,))
		self.threads[id].start()
		#chordring.nodes[port] = ip
		#print(chordring.nodes)
		
	def new_node (self, ip, port) :
		new_node = Node(ip, port)
		print("ready to join", self.master_port)
		new_node.join(self.master_port)
		print("I joined")
		new_node.connection()
		
	def depart(self, id, ip, port):
		#print (chordring.nodes.keys())
		if port not in chordring.nodes.keys():
			print ("there is no that node in the ring")
		else : 
			with Client(chordring.nodes[port], port) as c:
				message = "depart" + '+' + str(self.master_port) 
				c.communication(message)
			#del self.threads[id]
			del chordring.nodes[port]
			print ("I departed")
			
	def insert(self, key, value, port = None, replica = 0, repl = 'eventual'):
		if (port == None) : 
			start_port = random.choice(list(chordring.nodes.keys()))
		else :
			start_port = port
		print ("insert will start from port = ", start_port)
		with Client(chordring.nodes[start_port], start_port) as c:
			message = "insert" + '+' + str(key) + '+' + str(value) + '+' + str(start_port) + '+' + str(replica) + '+' + repl + '+' + chordring.nodes[start_port] 
			c.communication(message)
		print ("insert completed")
		
	def delete(self, key, port = None, replica = 0, repl = 'eventual'): 
		if (port == None) : 
			start_port = random.choice(list(chordring.nodes.keys()))
		else :
			start_port = port
		print ("delete will start from port = ", start_port)
		with Client(chordring.nodes[start_port], start_port) as c:
			message = "delete" + '+' + str(key) + '+' + str(start_port) + '+' + str(replica) + '+' + repl + '+' + chordring.nodes[start_port] 
			c.communication(message)
		print ("delete completed")
			
	def query(self, key, port = None, replica =0, repl = 'eventual'):
		if (port == None) : 
			start_port = random.choice(list(chordring.nodes.keys()))
		else :
			start_port = port
		print ("query will start from port = ", start_port)
		with Client(chordring.nodes[start_port], start_port) as c:
			message = "query" + '+' + str(key) + '+' + str(start_port) + '+' + str(0) + '+' + str(replica) + '+' + repl + '+' + chordring.nodes[start_port] 
			c.communication(message)
		print ("query completed")
			
	def destroy (self, port): 
		if port not in chordring.nodes.keys():
			print ("there is no that node in the ring")
		with Client(chordring.nodes[port], port) as c:
			message = "destroy" + '+' + str(self.master_port)
			c.communication(message, False)
			
			
	def overlay (self, port): 
		with Client(chordring.nodes[port], port) as c:
			message = "overlay" + '+' + str(port) + '+' + str(0) + '+' + '' + '+' + chordring.nodes[port]
			x = c.communication(message)
			x = x.decode()
		x = x.split('/')
		for i in range (len(x)):
			print(x[i])
			
		#f = open("overlay.txt", "r")
		#print (f.read())
Ring = chordring("192.168.0.4", 5000)
