# coding=utf-8
import json
import logging
import sys
import os
from kazoo.client import KazooClient

# @TODO: Need the zookeeper address from env
def get_brokers():
	zk = KazooClient(hosts=os.environ['ZOOKEEPER'], read_only=True)
	zk.start()

	broker_list = ""
	children = zk.get_children( '/brokers/ids' )
	for i in children:
		data, stat = zk.get( '/brokers/ids/'+i )
		data = json.loads( data )
		if broker_list != "":
			broker_list += ","
		broker_list += data['host'].encode('utf8') + ":" + str(data['port'])

	data, stat = zk.get( '/brokers/ids/0' )
	zk.stop()
	data = json.loads( data )
	return broker_list

def setup_logging():
	root = logging.getLogger()
	root.setLevel(logging.INFO)
	ch = logging.StreamHandler(sys.stdout)
	ch.setLevel(logging.INFO)
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	ch.setFormatter(formatter)
	root.addHandler(ch)

# Example of how to use a hash function to pick partition
#def hashFunc(partitions,key):
#	which = json.loads(key)['count'] % len(partitions)
#	return  partitions[which]
#producer = topic.get_producer( partitioner=hashFunc )
