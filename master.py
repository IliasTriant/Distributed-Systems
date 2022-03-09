#!/usr/bin/env python3

from chordring import Ring, chordring
import time
import socket
import threading

def main() : 

	#threads = threading.Thread(target=Ring.dht, args=("localhost", 5000,))
	#threads.start()
	Ring.dht("192.168.0.4", 5000)
'''	
	time.sleep(10)
	print("hi")
	f = open('transactions/insert.txt', 'r')
	start = time.time()
	for line in f : 
		line = line.rstrip()
		line = line.split(',')
		Ring.insert( line[0], line[1])
	f.close()
'''	
'''
	ring = chordring("localhost", 5000, True)
	time.sleep(2)
	port = 5001
	for i in range (9) : 
		ring.join(i+1, "localhost", port+i)
		time.sleep(2)
	
	f = open('transactions/insert.txt', 'r')
	start = time.time()
	for line in f : 
		line = line.rstrip()
		line = line.split(',')
		ring.insert( line[0], line[1])
	f.close()
	
	f = open('transactions/query.txt', 'r')
	for line in f : 
		line = line.rstrip()
		#line = line.split(',')
		ring.query(line)
		
	print ("total time for insert with k=5 - linear : ", time.time() - start)
	
'''	
'''	
	f = open('transactions/requests.txt', 'r')
	for line in f :
		line = line.rstrip()
		if line.startswith('query'):
			line = line.split(',')
			ring.query(line[1])
		elif line.startswith('insert'):
			line = line.split(',')
			ring.insert( line[1], line[2])
	f.close()
		
	
	f = open('transactions/insert.txt', 'r')
	start = time.time()
	for line in f : 
		line = line.rstrip()
		line = line.split(',')
		ring.insert( line[0], line[1])
	f.close()
	print ("total time for insert with k=3 - linearizability : ", time.time() - start)
	

	f = open('transactions/query.txt', 'r')
	for line in f : 
		line = line.rstrip()
		#line = line.split(',')
		ring.query(line)
		
'''


'''
	ring = chordring("localhost", 5000, True)
	time.sleep(2)
	#ring.dht("localhost", 5000)
	#time.sleep(2)
	ring.join(1, "localhost", 5004)
	time.sleep(2)
	ring.join(2, "localhost", 5002)
	time.sleep(2)
	ring.join(3, "localhost", 5001)
	time.sleep(2)
	ring.join(4, "localhost", 5003)
	time.sleep(2)
	#ring.depart(2, "localhost", 5002)
	#time.sleep(2)
	ring.join(4, "localhost", 5006)
	time.sleep(2)
	ring.insert( 55, "rrrr", replica = 4)
	#time.sleep(2)
	ring.query("*")
	ring.query( 55, replica = 4)
	ring.query("*")
	#time.sleep(2)
	ring.insert( 42, "ilias")
	ring.query("*")
	#time.sleep(2)
	ring.insert( 1112, "giorgos")
	#time.sleep(2)
	ring.insert( 4, "erica")
	#time.sleep(2)
	ring.insert( 17, "omadara")
	#time.sleep(2)
	ring.insert( 56, "lol")
	time.sleep(2)
	ring.delete(4)
	time.sleep(2)
	ring.delete(5)
	time.sleep(2)
	ring.query(1112)
	#time.sleep(2)
	ring.query(4)
	#time.sleep(2)
	ring.query("*")
	time.sleep(2)
	ring.depart(4, "localhost", 5006)
	time.sleep(2)
	ring.query("*")
	#time.sleep(2)
	
	
'''
	


	

	
if __name__ == '__main__':
	main()
