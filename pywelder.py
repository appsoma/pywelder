import os
import urlparse
import urllib
import httplib
import ssl
import json
import inspect
import re
from urllib2 import HTTPError
from httplib import BadStatusLine

# This will become the welder api.
def expand_vars(s, level=1):
	state = "str"
	n = []
	cv = ''
	stack = inspect.currentframe()
	for i in range(level):
		stack = stack.f_back
	var = dict(globals().items() + stack.f_locals.items())
	i = 0
	while i < len(s):
		if state == "str":
			if i + 1 < len(s) and s[i] == "$" and s[i + 1] == "{" :
				state = "var"
				cv = ''
				i += 1
			else:
				n.append(s[i])
		elif state == "var":
			if(s[i] == "}"):
				parts = cv.split(".")
				co = None
				for part in parts:
					if not co:
						co = var[part]
					else:
						co = getattr(co, part)
				n.append(str(co))
				state = "str"
			else:
				cv += s[i]
		i+=1
	return "".join( n )


def http(url, params={}, data="", action="GET", headers={}, progressCallback=None, toFilename=None, retrys=0, retryDelay=5, returnHeaders=False):
	attempts = 0
	while attempts <= retrys:
		try:
			parsed = urlparse.urlparse(url)
			if url.startswith("https://"):
				# WARNING: This does not do any checking of the SSL certificate
				h = httplib.HTTPSConnection(parsed.netloc)
			else:
				h = httplib.HTTPConnection(parsed.netloc)

			# The URL may have a query string which is combined with the params
			if parsed.query:
				inlineQueryAsDict = urlparse.parse_qs(parsed.query)

				# FLATTEN because parse_qs annoyingly returns a list for each value
				for k in inlineQueryAsDict:
					inlineQueryAsDict[k] = inlineQueryAsDict[k][0]

				# UNION the sent in params with the query string
				params = dict(params.items() + inlineQueryAsDict.items())

			headers["Content-Length"] = str(len(data))
			headers["User-Agent"] = "fake browser"
			if data and data != "" and ("Content-Type" not in headers) and not(action == "GET" or action == "DELETE"):
				raise HTTPError(url, 0, "Trying to send data without Content-Type to url:" + url, None, None)

			paramString = ""
			if params:
				paramString = "?"+urllib.urlencode(params).encode('utf-8')

			try:
				escapePath = urllib.quote(parsed.path).encode('utf-8')
				h.request(action, escapePath + paramString, data, headers)
				resp = h.getresponse()
			except ssl.SSLError as e:
				raise HTTPError(url, 406, "SSL failed: " + url + ", " + str(e), None, None)
			except BadStatusLine as e:
				raise HTTPError(url, 404, "URL returned bad status:" + url + ", " + str(e), None, None)
			except Exception as e:
				raise HTTPError(url, 0, "Can not reach url: " + url + ", " + str(e), None, None)

#			if resp.status != 200:
#				raise HTTPError(url, resp.status, "Return code not 200", None, None)

			readData = ""
			f = None
			try:
				if toFilename and str(resp.status)[0] == '2':
					try:
						os.makedirs('/'.join(toFilename.split('/')[:-1]))
					except:
						pass
					f = open(toFilename, "wb")

				totalSize = int( resp.getheader("content-length", 0))
				blockSize = 1024 * 1024
				count = 0
				lengthRead = 0
				while True:
					chunk = resp.read(blockSize)
					if chunk and progressCallback:
						lengthRead += len(chunk)
						progressCallback(url, lengthRead, totalSize)
					if not chunk:
						break
					if f:
						f.write(chunk)
					else:
						readData += chunk

					count += 1
			except Exception as e:

				raise HTTPError(url, resp.status, "Bad read or write: " + url + " " + str(e), None, None)
			finally:
				if f is not None:
					f.close()

			if str(resp.status)[0] != '2':
				raise HTTPError(url, resp.status, readData, None, None)

			if returnHeaders:
				replyHeaders = {}
				for k, v in resp.getheaders():
					replyHeaders[k.lower()] = v
				return readData, replyHeaders
			else:
				return readData

		except Exception as e:
			if attempts >= retrys:
				raise e
			attempts += 1
			if retryDelay:
				time.sleep(retryDelay)

	# We should never get here, but in case we throw an exception
	raise HTTPError(url, 0, "http fatal: " + url, None, None)

def welder_run_task_add( task_object ):
	key = os.environ['WELDER_KEY']
	run_id = os.environ['WELDER_RUN_ID']
	project = os.environ['WELDER_PROJECT']
	url = os.environ['WELDER_URL'] + "/api/v1/projects/"+project+"/runs/"+run_id+"/tasks"
	ret = http( url, data=json.dumps(task_object), action="POST", headers={'Content-Type':'application/json','welder_key':key} )
	return json.loads(ret)['taskFolder']

def welder_run( run_object ):
	key = os.environ['WELDER_KEY']
	project = os.environ['WELDER_PROJECT']
	url = os.environ['WELDER_URL'] + "/api/v1/projects/"+project+"/runs"
	ret = http( url, data=json.dumps(run_object), action="POST", headers={'Content-Type':'application/json','welder_key':key} )
	return json.loads(ret)

def service_address( external_or_internal="external" ):
	m = re.match( r'(^[^\.]+)\.', os.environ['MESOS_TASK_ID'] )
	if m:
		service = m.group(1)
	else:
		raise Exception("MESOS_TASK_ID doesn't match service pattern")
	value = http( "http://"+os.environ['HOST']+":2379/v2/keys/"+external_or_internal+"s/"+service )
	return json.loads( value )['node']['value']

