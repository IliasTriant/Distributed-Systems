#!/usr/bin/env python3

from chordring import Ring
import time
import socket

def main() : 
	Ring.join(3, "192.168.0.2", 5001)
	time.sleep(2)
	
	

	
if __name__ == '__main__':
	main()
