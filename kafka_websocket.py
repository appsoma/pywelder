import json
import time
import threading
import signal
import sys
from autobahn.twisted.websocket import WebSocketServerProtocol, WebSocketServerFactory
from twisted.internet import reactor
sys.path.append( "./pywelder" )
import kafka_util
sys.path.insert(0, "./pykafka")
from pykafka import KafkaClient
import pykafka.protocol

kafka_util.setup_logging()
brokers = kafka_util.get_brokers()
kafka = KafkaClient( hosts=brokers )


kafka_threads = []

class KafkaThread(threading.Thread):
	def __init__(self, args):
		print "CREATE THREAD"
		global kafka_threads
		kafka_threads.append( self )
		super(KafkaThread, self).__init__(args=args)
		self.stop_request = False
		self.topic_name = args[1]
		self.protocol = args[0]

		self.topic = kafka.topics[self.topic_name]
		self.consumer = self.topic.get_simple_consumer( "kafka_websocket" )

		# ACB: pykafka fails miserably if you try to reset offsets to head and there is no offset
		offsets = set([ res.offset[0] for p, res in self.topic.latest_available_offsets().items() ])
		if not (len(offsets) == 0 and offsets[0] == -1):
			# SKIP all data up to now.
			self.consumer.reset_offsets( [ (v, -1) for k,v in self.topic.partitions.items() ] )

	def run(self):
		print "THREAD RUN"
		while not self.stop_request:
			try:
				message = self.consumer.consume(block=True)
				if message:
					ret_message = {
						'topic': self.topic_name,
						'message': json.loads(message.value)
					}
					self.protocol.sendMessage( json.dumps(ret_message) )
			except Exception as e:
				print "SLEEP ON EXECPTION", e
				time.sleep(0.1)

		print "THREAD STOPPED"

	def stop(self):
		print "THREAD STOP REQUEST"
		self.stop_request = True

class MyServerProtocol(WebSocketServerProtocol):
	def __init__(self):
		print "ON INIT"
		self.kafka_threads = {}

	def onConnect(self, request):
		print "ON CONNECT", request

	def onOpen(self):
		print "ON OPEN"
		self.path = self.http_request_path
		self.query = self.http_request_params

	def onMessage(self, payload, isBinary):
		comm = json.loads(payload)
		if comm["command"] == "start_follow":
			self.start_follow( comm["topic"] )
		if comm["command"] == "stop_follow":
			self.stop_follow( comm["topic"] )
		if comm["command"] == "history":
			self.history( comm["topic"], comm["offset"], comm["count"] )
		if comm["command"] == "produce":
			topic = kafka.topics[comm["topic"]]
			producer = topic.get_producer()
			producer.produce( [ json.dumps(comm["message"]) ] )

	def onClose(self, wasClean, code, reason):
		print "ON CLOSE", wasClean, code, reason
		for k,v in self.kafka_threads.items():
			v.stop()

	def start_follow(self,topic_name):
		self.kafka_threads[topic_name] = KafkaThread( args=(self,topic_name) )
		self.kafka_threads[topic_name].start()
	
	def stop_follow(self,topic_name):
		self.kafka_threads[topic_name].stop()
		
	def history(self,topic_name,offset,count):
		topic = kafka.topics[topic_name]
		consumer = topic.get_simple_consumer( "group1" )
		consumer.seek(offset,1)
		while True:
			message = consumer.consume(block=False)
			if message:
				self.sendMessage(message.value)
			else:
				break

if __name__ == '__main__':
	print "MAIN1"
	def signal_handler(signal, frame):
		global kafka_threads
		for i in kafka_threads:
			i.stop_request = True
		reactor.stop()

	signal.signal(signal.SIGINT, signal_handler)

	headers = {
		"Access-Control-Allow-Origin": "*"
	}
	factory = WebSocketServerFactory("ws://*:80",debug=True,headers=headers)
	factory.protocol = MyServerProtocol
	reactor.listenTCP(80,factory)
	print "Listening on 80"
	reactor.run()
	print "ENDED"
