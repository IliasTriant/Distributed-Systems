#!/usr/bin/env python3

import hashlib 
import socket

class Client(object):
	def __init__(self, ip, port):
		self.port=port
		self.answer = ''
		self.client_socket=socket.socket()
		self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		#self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.ip = ip
		self.client_socket.connect((self.ip, self.port))

        
	def __exit__(self, exc_type, exc_val, exc_tb):
		self.client_socket.close()

	def __enter__(self):
		return self
        
	def communication(self,message, flag = True):
		#print(message)
		self.client_socket.send(message.encode())
		if (flag==True): 
			self.answer = self.client_socket.recv(4096)
		#self.answer = self.answer.decode()
		#print("answer = ", self.answer)
			return self.answer

