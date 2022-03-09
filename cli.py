#!/usr/bin/env python3

import click 
import requests
import pprint
import json
import os
from os.path import expanduser
import re
import urllib3
urllib3.disable_warnings()
#from click_option_group import optgroup
from chordring import Ring

@click.group()
def main():
	pass



@main.command(name='insert', help = 'insert a pair key-value in the chordring')
@click.option('--port', default=5000, help =  'port')
@click.option('--key', type=str, help =  'the key')
@click.option('--value', type=str, help =  'the value')
def insert(key, value, port):
	Ring.insert(key, value, port, replica = 2, repl = 'linear')


@main.command(name='delete', help = 'delete a key in the chordring')
@click.option('--port', default=5000, help =  'port')
@click.option('--key', type=str, help =  'the key')
def delete(key, port):
	Ring.delete(key, port, replica = 2, repl = 'linear')


@main.command(name='query', help = 'query a key in the chordring')
@click.option('--port', default=5000, help =  'port')
@click.option('--key', type=str, help =  'the key')
def query(key, port):
	Ring.query(key, port, replica = 2, repl = 'linear')


@main.command(name='depart', help = 'depart node')
@click.option('--id', default=0, help =  'the id of the node')
@click.option('--ip', default='localhost', help =  'ip')
@click.option('--port', default=5000, help =  'port')
def depart(id, ip, port):
	Ring.depart(id, ip, port)


@main.command(name='overlay', help = 'print the topology of the network')
@click.option('--port', default=5000, help =  'port')
def overlay(port):
	Ring.overlay(port)




if __name__ == '__main__':
	main()
                  
