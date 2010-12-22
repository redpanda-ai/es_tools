import urllib2, os, re, pyodbc, sys, time, urllib, Queue, threading, difflib
import signal, httplib, locale, smtplib

locale.setlocale(locale.LC_ALL, 'en_US.utf8')

colors = ["\033[90m", "\033[91m", "\033[92m", "\033[93m", "\033[94m", \
	"\033[95m", "\033[96m", "\033[89m"]

def write_email (subj, body) :
	fromaddr="newton@comparenetworks.com"
	toaddrs= ("jkey@comparenetworks.com zmarji@comparenetworks.com" + \
	" charu@comparenetworks.com").split()
	msg = ("From: %s\r\nTo: %s\r\n" \
	% (fromaddr, ", ".join(toaddrs))) \
	+ "Subject: " + subj + "\r\n\r\n" \
	+ subj + "\r\n" + body

	server = smtplib.SMTP('10.10.10.144')
	server.set_debuglevel(1)
	server.sendmail(fromaddr, toaddrs, msg)
	server.quit()

def clean_up_dirty_text (string):
	result = ""
	for c in string: 
		if ord(c) < 0x80: result +=c
		elif ord(c) < 0xC0: result += ('\xC2' + c)
		else: result += ('\xC3' + chr(ord(c) - 64))
	return result

class ThreadConsumer(threading.Thread):
	def __init__(self,thread_id,data_queue,colors,nodes):
		threading.Thread.__init__(self)
		self.thread_id = thread_id
		self.data_queue = data_queue
		self.uploaded_queue = uploaded_queue
		self.color = colors[self.thread_id % len(colors)]
		self.endc = "\033[0m"
		self.node_number = self.thread_id % len(nodes)
		self.cloud_node = nodes[self.node_number]
		self.bulk = []
		self.unbulked = ""
		self.log("Consumer Started")

	def log(self,s):
		sys.stdout.write(self.color + \
		time.strftime("[%Y-%m-%dT%H:%M:%S] ", time.localtime()) + \
		"c" + str(self.thread_id) + \
		":n" + str(self.node_number) + \
		":q" + str(self.data_queue.qsize()) + \
		str(s) + "\n" + self.endc)
		sys.stdout.flush()


	def run(self):
		while True:
			count = 0
			for i in range(bulk_size):
				try:
					#for loop to grab bulk items for self.bulk
					self.bulk.append(self.data_queue.get())
					self.data_queue.task_done()
					count = count+1
				except Queue.Empty, e:
					if count > 0:
						self.put_data(bulk)
						self.uploaded_queue.put(count)
						count = 0
			if count > 0:
				self.put_data(self.bulk)
				self.uploaded_queue.put(count)
			elif id_queue_consumed == 1:
				#nothing to write, no more will be written
				self.log("complete")
				return 0

	def put_data(self,bulk):
		url = "http://" + self.cloud_node + ":9200/_bulk"
		opener = urllib2.build_opener(urllib2.HTTPHandler)

		self.unbulked = ""
		for i in range(bulk_size):
			row = self.bulk.pop(0)
			self.unbulked += clean_up_dirty_text (row.json_command)

		request = urllib2.Request(url, self.unbulked)
		request.add_header('Content-Type', 'text/html')
		try:
			response = opener.open(request).read()
		except urllib2.HTTPError, e:
			print "Error of some sort"
			print request.get_full_url()
			print len(request.get_data())
			#print url
			#print self.unbulked
			#self.data_queue.put(row)
			return

		reponse_fail_pattern = re.compile(r'^.*Exception.*$')
		if reponse_fail_pattern.search(response):
			self.log(" " + str(row.item_id) + " [FAILURE] " + \
			response)
			#self.log("foo")
		else :
			self.log(" " + str(row.item_id) + " [OK]")

p_colors = ["\033[105m"]

class ThreadProducer(threading.Thread):
	def __init__(self,thread_id,id_queue,data_queue,colors,nodes):
		threading.Thread.__init__(self)
		self.thread_id = thread_id
		self.id_queue = id_queue
		self.data_queue = data_queue
		self.color = colors[self.thread_id % len(colors)]
		self.endc = "\033[0m"
		self.node_number = self.thread_id % len(nodes)
		self.cloud_node = nodes[self.node_number]

	def log(self,s):
		sys.stdout.write(self.color + \
		time.strftime("[%Y-%m-%dT%H:%M:%S] ", time.localtime()) + \
		"p" + str(self.thread_id) + \
		":n" + str(self.node_number) + \
		":q" + str(self.data_queue.qsize()) + \
		str(s) + "\n" + self.endc)
		sys.stdout.flush()


	def run(self):
		#fetch a batch
		while ( self.id_queue.qsize() > 0 ):
			start_id = self.id_queue.get()
			#self.delete_items(start_id)
			rows = self.get_data(start_id)
			for row in rows:
				self.data_queue.put(row)
			self.id_queue.task_done()
		if (self.id_queue.qsize() == 0):
			self.log(" id_queue consumed")
			id_queue_consumed = 1
			return 0	

	def get_data (self, last_id ) :
		#fetch product data in JSON notation from the database
		command = "EXEC LogShipping.dba_tools.insert_to_elastic_search_cloud" + \
		" @starting_item_id='" + str(last_id) + "'" + \
		", @ending_item_id='" + str(last_id + batch_size) + "'" + \
		", @fetch_records='" + str(batch_size) + "'" + \
		", @category='" + db_category + "'" + \
		", @es_index='" + es_index + "'" + \
		", @es_type = '" + es_type + "'"
		#sys.stdout.write(command)
		cnxn = pyodbc.connect('DSN='+dsn+';UID='+uid+';PWD='+pwd)
		cursor = cnxn.cursor()
		cursor.execute(command)
		self.log(" [" + str(last_id) + ":" + \
		str(last_id + batch_size) +  "]") 
		rows = cursor.fetchall()
		cnxn.close()
		return rows

def get_elastic_search_cloud_nodes ( ):
	#create a list of elastic search cloud nodes and place in nodes[]
	pattern = re.compile(r'^.*ami-b646b3df.*(ec2-[^\t]+).*$')
	results = os.popen("ec2-describe-instances")
	for line in results:
		if pattern.search(line):
			nodes.append(pattern.search(line).groups()[0])
	sys.stdout.write("There are " + str(len(nodes)) + " nodes.\n")

def get_known_es_nodes ():
	nodes.extend(["cloud1","cloud2","cloud3"])
	#nodes.extend(["localhost"])

def start_producers ( id_queue ):
	for i in range(producers):
		p = ThreadProducer(i,id_queue,data_queue,p_colors,nodes)
		p.setDaemon(True)
		p.start()

def start_consumers ( data_queue ) :
	#create all of your consumers
	for i in range(consumers):
		c = ThreadConsumer(i,data_queue,colors,nodes)
		c.setDaemon(True)
		c.start()

def populate_id_queue ( id_queue ) :
	#create a start id for each batch and add it to the id_queue
	i = start_id
	while (i <= end_id):
		id_queue.put(i)
		i += batch_size

def signal_handler( signal, frame) :
	print 'You pressed Ctrl+C!'
	sys.exit(0)


def ensure_group_options_are_ready():
	command = "SELECT COUNT(0) c FROM LogShipping.dba_tools.item_group_options"
	cnxn = pyodbc.connect('DSN='+dsn+';UID='+uid+';PWD='+pwd)
	cursor = cnxn.cursor()
	cursor.execute(command)
	row = cursor.fetchone()
	cnxn.close()
	if row.c == 0:
		write_email("New Indexer Report", \
		"Group options table was empty :( Aborting the update")
		sys.exit(0)
	else:
		write_email("New Indexer Report", str(row.c) + \
		" group options found :)")

def confirm_index():
	#fetch product data in JSON notation from the database
	command = "EXEC LogShipping.dba_tools.Update_crud_items_rundate " + \
	"@category_id=" + category_id + ", @flag='I'"
	print command
	cnxn = pyodbc.connect('DSN='+dsn+';UID='+uid+';PWD='+pwd)
	cursor = cnxn.cursor()
	cursor.execute(command)
	cnxn.commit()

def send_report():
	uqs = uploaded_queue.qsize()
	sys.stdout.write("uploaded queue size: " +str(uqs) + "\n")
	total = 0
	#loop through uploaded_queue add numbers
	while ( uploaded_queue.qsize() > 0 ):
		total += uploaded_queue.get()
	write_email("Uploaded " + str(total) + " documents", ":)")

#Exit with informative message if there are not exactly 3 parameters
if len(sys.argv) != 13:
	print "usage: python " + sys.argv[0] + " <dsn> <uid> <pwd> <batch_size>" + \
	" <bulk_size> <consumers> <producers> <start_id> <end_id>" + \
	" <db_category> <es_index> <es_type>"
	sys.exit(0)

#assign command line arguments to named variables
#does not appear to work with types that have spaces

signal.signal(signal.SIGINT, signal_handler)

dsn,uid,pwd = sys.argv[1], sys.argv[2], sys.argv[3]
batch_size,bulk_size = int(sys.argv[4]),int(sys.argv[5])
consumers,producers = int(sys.argv[6]),int(sys.argv[7])
start_id,end_id = int(sys.argv[8]),int(sys.argv[9])
db_category,es_index,es_type = sys.argv[10],sys.argv[11],sys.argv[12]

category_id = '3194'

#set variables
nodes = []
get_known_es_nodes()
#get_elastic_search_cloud_nodes()
ensure_group_options_are_ready()
data_queue, id_queue = Queue.Queue(), Queue.Queue()
uploaded_queue = Queue.Queue()
id_queue_consumed = 0
populate_id_queue(id_queue)
start_consumers(data_queue)
start_producers(id_queue)
id_queue.join()
data_queue.join()
sys.stdout.write("Confirming Index")
confirm_index()
sys.stdout.write("Finished, sending mail")
send_report()
#write_email("New Indexer Report","The indexer ran, you can review the logs")

sys.exit(0)
