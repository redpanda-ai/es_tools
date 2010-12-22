import os, re, pyodbc, sys, time, Queue, threading, httplib, smtplib

colors = ["\033[90m", "\033[91m", "\033[92m", "\033[93m", "\033[94m", \
	"\033[95m", "\033[96m", "\033[89m"]

def write_email (subj, body) :
        fromaddr="newton@comparenetworks.com"
        toaddrs="jkey@comparenetworks.com zmarji@comparenetworks.com".split()
        msg = ("From: %s\r\nTo: %s\r\n" \
        % (fromaddr, ", ".join(toaddrs))) \
        + "Subject: " + subj + "\r\n\r\n" \
        + subj + "\r\n" + body

        server = smtplib.SMTP('10.10.10.144')
        server.set_debuglevel(1)
        server.sendmail(fromaddr, toaddrs, msg)
        server.quit()

class ThreadConsumer(threading.Thread):
	def __init__(self,thread_id,data_queue,colors,nodes):
		threading.Thread.__init__(self)
		self.thread_id = thread_id
		self.data_queue = data_queue
		self.color = colors[self.thread_id % len(colors)]
		self.endc = "\033[0m"
		self.node_number = self.thread_id % len(nodes)
		self.cloud_node = nodes[self.node_number]

	def run(self):
		while True:
			row = self.data_queue.get()
			self.delete(row)
			self.data_queue.task_done()

	def log(self,s):
		sys.stdout.write(self.color + \
		time.strftime("[%Y-%m-%dT%H:%M:%S] ", time.localtime()) + \
		"t" + str(self.thread_id) + \
		":n" + str(self.node_number) + \
		":q" + str(self.data_queue.qsize()) + \
		str(s) + \
		self.endc)
		sys.stdout.flush()

	def delete(self,row):
		#make a request to delete a single item
		host = self.cloud_node 
		url = "/" + es_index + "/" + es_type + "/" + str(row.itemid)
		try:
			c = httplib.HTTPConnection(host,9200)
			c.request('DELETE',url)
			response = c.getresponse().read()
		except httplib.HTTPException, e:
			print "Exception on " + str(row.itemid)
			self.data_queue.put(row)
			return
	
		#regex to see if response indicates success
		response_ok_pattern = re.compile(r'^{"ok":true,.*$')
		if response_ok_pattern.search(response):
			self.log(" " + str(row.itemid) + " [OK]\n")
			#self.log("foo")
		else :
			self.log(" " + str(row.itemid) + " [FAILURE]\n")

def get_rows_to_delete(data_queue):
	#fetch a batch
	rows = get_data()
	for row in rows:
		data_queue.put(row)

def confirm_delete():
	#fetch product data in JSON notation from the database
	command = "EXEC LogShipping.dba_tools.Update_crud_items_rundate " + \
	"@category_id=" + category_id + ", @flag='D'"
	print command
	cnxn = pyodbc.connect('DSN='+dsn+';UID=jkey_sa;PWD=Mus3Musculus!')
	cursor = cnxn.cursor()
	cursor.execute(command)
	cnxn.commit()

def get_data () :
	#fetch product data in JSON notation from the database
	command = "EXEC LogShipping.dba_tools.crud_items_delete " + \
	"@category_id='" + category_id + "'"
	cnxn = pyodbc.connect('DSN='+dsn+';UID=jkey_sa;PWD=Mus3Musculus!')
	cursor = cnxn.cursor()
	cursor.execute(command)
	rows = cursor.fetchall()
	return rows

def get_elastic_search_cloud_nodes ( ):
	#create a list of elastic search cloud nodes and place in nodes[]
	nodes.extend(["cloud1","cloud2","cloud3"])
	#pattern = re.compile(r'^.*ami-b646b3df.*(ec2-[^\t]+).*$')
	#results = os.popen("/usr/bin/ec2-describe-instances")
	#for line in results:
	#	if pattern.search(line):
	#		nodes.append(pattern.search(line).groups()[0])
	sys.stdout.write(time.strftime("[%Y-%m-%dT%H:%M:%S] ", \
	time.localtime()) + "There are " + str(len(nodes)) + " nodes.\n")

def get_known_es_nodes ():
	#nodes.extend(["cloud101","cloud102","cloud103"])
	nodes.extend(["localhost"])

def start_consumers ( data_queue ) :
	#create all of your consumers
	for i in range(consumers):
		c = ThreadConsumer(i,data_queue,colors,nodes)
		c.setDaemon(True)
		c.start()

#Exit with informative message if there are not exactly 3 parameters
if len(sys.argv) != 7:
	print "usage: python " + sys.argv[0] + " <consumers> <dsn> " + \
	"<category_id> <es_index> <es_type> <localhost_or_cloud>"
	sys.exit(0)

#assign command line arguments to named variables
#does not appear to work with types that have spaces
consumers = int(sys.argv[1])
dsn,category_id = sys.argv[2], sys.argv[3]
es_index,es_type = sys.argv[4],sys.argv[5]
localhost_or_cloud = sys.argv[6]

#set variables
nodes = []

if localhost_or_cloud == 'localhost':
	get_known_es_nodes()
else:
	get_elastic_search_cloud_nodes()

data_queue = Queue.Queue()
get_rows_to_delete(data_queue)
start_consumers(data_queue)
data_queue.join()
confirm_delete()
write_email("Deleter Report","The deleter ran, you can review the logs")
sys.exit(0)
# a different idea would be to have a queue of errors that you can send into
# the stored procedure, also logic to determine whether something should be confirmed
