#!c:\python27x64\python

# ^---- the above path needs to be changed, it should be your path to python



'''
	Copyright 2012 Joshua S. Giardino
    
	This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''

# loading some nifty helpers from the stdlib

from __future__ import division
try:
	import logging, traceback, json, sys
except Exception, ex:
	print 'Standard Library Import Error'
	print sys.exit(1)

# activating a logger for exception handling, it dumps to the console by default
try:
	logger = logging.getLogger('moz')
	s = logging.StreamHandler()
	logger.setLevel(logging.INFO)
	logger.addHandler(s)
except:
	print 'Logging Exception: '
	print traceback.format_exc()
	sys.exit(1)

# gevent adds concurrency, it's wonderful
try:
	import gevent
	from gevent.queue import Queue, Empty
	
	from gevent import monkey; monkey.patch_socket()

except:
	logger.exception(traceback.format_exc())
	sys.exit(1)
	
# requests is people friendly url fetching, it's also quite wonderful
try:
	import requests
except:
	logger.exception(traceback.format_exc())
	sys.exit(1)

# PyQuery = jQuery selectors in Python
# lxml is a dependency for PyQuery

try:
	import lxml
	from pyquery import PyQuery as pq
except:
	logger.exception(traceback.format_exc())
	sys.exit(1)


# this lets you know the logging is working, and that the app loaded okay
logger.info('SEOMoz User Directory Crawler Initialized')


# for each profile request, init all potential fields with a value of "none" based on a str.lower of the field's table heading. On scrape, iterate through the values, updating as necessary. This ensures a complete model of the profile data while still allowing consistency for the CSV format.

def userDataModel():
	# could do an object, but DICTS are easy
	# plus the keys handle whitespace well. only gotta trim ":" and str.lower to match things up nicely
	
	model = dict()
	
	model[u'full name'] = u'none'
	model[u'nickname'] = u'none'
	model[u'email'] = u'none'
	model[u'title'] = u'none'
	model[u'company'] = u'none'
	model[u'type of work'] = u'none'
	model[u'location'] = u'none'
	model[u'favorite thing about seo'] = u'none'
	model[u'time spent on seo'] = u'none'
	model[u'bio'] = u'none'
	model[u'blog bio'] = u'none'
	model[u'additional contact info'] = u'none'
	model[u'favorite topics'] = u'none'
	
	return model


# same general idea as userDataModel, but for the vital stats table
def vitalStatsModel():
	model = dict()
	
	model[u'mozpoints'] = u'none'
	model[u'level'] = u'none'
	model[u'membership type'] = u'none'
	model[u'community rank'] = u'none'
	model[u'comments & responses'] = u'none'
	model[u'thumbs up'] = u'none'
	model[u'thumbs down'] = u'none'
	model[u'last activity'] = u'none'
	model[u'member since'] = u'none'
	
	return model


# same idea as above. grabs data for user from 
def userDirectoryStatsModel():
	model = dict()
	
	model[u'profileURL'] = u'none'
	model[u'blog posts'] = u'none'
	model[u'youmoz posts'] = u'none'
	
	return model
	

def companyDataModel():
	model = dict()
	model[u'category_code'] = u'none'
	model[u'description'] = u'none'
	model[u'ipo'] = u'none'
	model[u'inCrunchbase'] = u'false'
	
	return model

	
def fetchPage(url=None):
	try:
		url = url.strip().rstrip().lower()
	except:
		url = None
		logger.critical('Error in fetchPage()')
		
		
	if url is not None:
		try:
			r = requests.get(url,timeout=60.0)
			if r.status_code == requests.codes.ok:
				r.encoding = 'utf-8'
				source = r.text
			else:
				r.raise_for_status()
		except:
			logger.critical(traceback.format_exc())
			source = None
	else:
		source = None
		
	return source
	

def fetchJSON(url=None):
	try:
		url = url.strip().rstrip().lower()
	except:
		url = None
		logger.critical('Error in fetchPage()')
		
		
	if url is not None:
		try:
			r = requests.get(url,timeout=60.0)
			if r.status_code == requests.codes.ok:
				r.encoding = 'utf-8'
				source = r.json
			else:
				r.raise_for_status()
		except:
			logger.critical(traceback.format_exc())
			source = None
	else:
		source = None
		
	return source

	
def processDirectoryPage(pageNum=None):
	if pageNum is not None:
		url = 'http://www.seomoz.org/users/index?page=' + pageNum
		source = fetchPage(url)

		if source is not None:
			try:
				dom = lxml.html.fromstring(source)
				dom = pq(dom)
				
				memberTable = dom('table')
				memberTable = pq(memberTable)
				
				rows = memberTable('tr')
				rows = rows[1:]

				for row in rows:
					row = pq(row)
					data = userDirectoryStatsModel()
					href = row('div.usersPhoto')
					href = href('a').attr('href')
					data[u'profileURL'] = 'http://www.seomoz.org' + href
					logger.info('Directory Worker is queuing profile: ' + data[u'profileURL'])
					
					tds = row('td')
					
					data[u'blog posts'] = int(pq(tds[3]).text())
					data[u'youmoz posts'] = int(pq(tds[4]).text())
					
					profileQueue.put_nowait(data)
				
				
				for i in range(maxWorkers):
					profileWorkers.append(gevent.spawn(profileWorker,str(i)))
				gevent.joinall(profileWorkers)
					
				
			except:
				failed = stats.updateDirectoryFailed()
				success = stats.returnDirectorySuccess()
				logger.info('directory fetch failure - page ' + pageNum)
				logger.exception(traceback.format_exc())
				if success <= 0:
					success = 5
				
				if ((failed / success) * 100) >= 30:
					logger.critical('Unacceptably High Failure Rate on Directory URLs. Terminating Now!')
					gevent.killall(profileWorkers)
					gevent.killall(directoryWorkers)
					gevent.killall(outputWorkers)
					sys.exit(1)
				else:
					directoryQueue.put_nowait(pageNum)
					logger.info('directoryWorker requeueing page' + pageNum)
		else:
			failed = stats.updateDirectoryFailed()
			success = stats.returnDirectorySuccess()
			logger.info('directory fetch failure - page' + pageNum)
			
			if success <= 0:
				success = 5
			if ((failed / success) * 100) >= 30:
				logger.critical('Unacceptably High Failure Rate on Directory URLs. Terminating Now!')
				gevent.killall(profileWorkers)
				gevent.killall(directoryWorkers)
				gevent.killall(outputWorkers)
				sys.exit(1)
			else:
				directoryQueue.put_nowait(pageNum)
				logger.info('directoryWorker requeueing page' + pageNum)

				
def processProfilePage(data):
	if data is not None:
		
		try:
			if data[u'profileURL'] is not u'none':
				logger.info('fetching profile page: ' + data[u'profileURL'])
				source = fetchPage(data[u'profileURL'])
				if source is not None:
					userData = userDataModel()
					vitalStats = vitalStatsModel()
					
					try:
						dom = lxml.html.fromstring(source)
						dom = pq(dom)
						
						dataTable = dom('#data_table')
						tableRows = dataTable('tr')
						tableRows = tableRows[0:len(tableRows)-1]
						
						for row in tableRows:
							row = pq(row)
							
							rowTitle = row('th')
							rowTitle = pq(rowTitle).text()
							rowTitle = rowTitle.replace(':','').lower()
							
							rowData = row('td')
							rowData = pq(rowData).text()
							rowData = rowData.replace(',','').lower()
							
							userData[rowTitle] = rowData
						
						dataTable = dom('#profile_stats table')
						tableRows = dataTable('tr')
						
						for row in tableRows:
							row = pq(row)
							rowTitle = row('th')
							rowTitle = pq(rowTitle).text()
							rowTitle = rowTitle.replace(':','').lower()
							
							rowData = row('td')
							rowData = pq(rowData).text()
							rowData = rowData.replace(',','').lower()
							
							vitalStats[rowTitle] = rowData
							
						mozPoints = dom('#UserCurrentMozpoints')
						mozPoints = pq(mozPoints).text()
						vitalStats[u'mozpoints'] = mozPoints
						
						companyData = companyDataModel()
						if userData[u'company'] is not 'none':
							try:
								companyData = processCompany(userData[u'company'],companyData)
							except:
								companyData = companyData
						
						#start of record
						csvLine = ''
						
						#profile url
						csvLine = csvLine + unicode(data[u'profileURL']) + ','
						
						#start of profile page data
						csvLine = csvLine + unicode(userData[u'full name']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(userData[u'nickname']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(userData[u'email']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(userData[u'title']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(userData[u'company']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						
						# companyData from CrunchBase
						csvLine = csvLine + unicode(companyData[u'inCrunchbase']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(companyData[u'description']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(companyData[u'category_code']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(companyData[u'ipo']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						
						#resume profile page data
						csvLine = csvLine + unicode(userData[u'type of work']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(userData[u'location']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(userData[u'favorite thing about seo']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(userData[u'time spent on seo']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(userData[u'bio']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(userData[u'blog bio']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(userData[u'additional contact info']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(userData[u'favorite topics']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						#end of profile page data
						
						#start of vital stats data
						csvLine = csvLine + unicode(vitalStats[u'mozpoints']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(vitalStats[u'level']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(vitalStats[u'membership type']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(vitalStats[u'community rank']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(vitalStats[u'comments & responses']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(vitalStats[u'thumbs up']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(vitalStats[u'thumbs down']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(vitalStats[u'last activity']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(vitalStats[u'member since']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						
						#end of data from queue
						csvLine = csvLine + unicode(data[u'blog posts']).replace(',','').replace('\r','').replace('\n','').replace('"','') + ','
						csvLine = csvLine + unicode(data[u'youmoz posts']).replace(',','').replace('\r','').replace('\n','').replace('"','')
						
						#end of record
						csvLine = csvLine + '\n'
						return csvLine
						
					except:
						failed = stats.updateProfileFailed()
						success = stats.returnProfileSuccess()
						logger.info('profile fetch failure - page ' + data[u'profileURL'])
						logger.exception(traceback.format_exc())
						
						if success <= 0:
							success = 5
						
						if ((failed / success) * 100) >= 30:
							logger.critical('Unacceptably High Failure Rate on Profile URLs. Terminating Now!')
							#gevent.killall(profileWorkers)
							gevent.killall(directoryWorkers)
							gevent.killall(outputWorkers)
							sys.exit(1)
						else:
							profileQueue.put_nowait(data)
							logger.info('profileWorker requeueing page' + data[u'profileURL'])
							
				else:
					failed = stats.updateProfileFailed()
					success = stats.returnProfileSuccess()
					logger.info('profile fetch failure - page ' + data[u'profileURL'])
					logger.exception(traceback.format_exc())
					
					if success <= 0:
						success = 5
					
					if ((failed / success) * 100) >= 30:
						logger.critical('Unacceptably High Failure Rate on Profile URLs. Terminating Now!')
						#gevent.killall(profileWorkers)
						gevent.killall(directoryWorkers)
						gevent.killall(outputWorkers)
						sys.exit(1)
					else:
						profileQueue.put_nowait(data)
						logger.info('profileWorker requeueing page' + data[u'profileURL'])
					
		except:
			logger.exception(traceback.format_exc())
		
		return None
		
def processCompanyData(company,companyDataModel):
	company = company.lower().replace(' ','-')
	companyData = fetchJSON('http://api.crunchbase.com/v/1/company/' + company +'.js?app=pft&api_key=krb4686q9mdbjawqe949sbmj')
	
	if companyData is not None:
			companyData[u'inCrunchbase'] = u'true'
			if companyData[u'ipo'] == None:
				companyDataModel[u'ipo'] = str(False).lower()
			else:
				companyDataModel[u'ipo'] = str(True).lower()
			
			
			if companyData[u'category_code'] != u'':
				companyDataModel[u'category_code'] = companyData[u'category_code']
				
			if companyData[u'description'] != u'':
				companyDataModel[u'description'] = companyData[u'description']
			
			return companyDataModel
	else:
		return companyDataModel


def directoryWorker(workerID):
	while not directoryQueue.empty():
		pageNum = directoryQueue.get()
		pageNum = str(pageNum)
		try:
			logger.info('DirectoryWorker ' + str(workerID) + ' processing ' + pageNum)
			processDirectoryPage(pageNum)
			directoryCount = stats.updateDirectorySuccess()
			logger.info(str(directoryCount) + ' directory pages crawled successfully')
		except:
			logger.exception(traceback.format_exc())
	
		
		gevent.sleep(0)
	logger.info('Directory Queue Empty, Worker: ' + str(workerID) + ' terminating now.')

	
def profileWorker(workerID):
	while not profileQueue.empty():
		data = profileQueue.get()
		try:
			csvData = processProfilePage(data)
			if csvData is not None:
				outputQueue.put(csvData)
				outputWorkers.append(gevent.spawn(outputWorker,0))
				gevent.joinall(outputWorkers)
			else:
				logger.info('Profile Worker Failed for ' + data[u'profileURL'])
		except:
			logger.exception(traceback.format_exc())

		gevent.sleep(0)
		
	logger.info('Profile Queue Empty, Worker: ' + str(workerID) + ' terminating now.')
	

def outputWorker(workerID):
	while not outputQueue.empty():
		data = outputQueue.get()
		saveFile = 'F:/from-home/mozDirectoryData.csv'

		#append mode
		try:
			logger.info('Output Worker ' + str(workerID) + ' is writing data')
			
			fh = open(saveFile,'a')
			fh.write(data.decode('utf-8'))
			fh.close()
			
			profileCount = stats.updateProfileSuccess()
			logger.info(str(profileCount) + ' profile pages crawled successfully')
			return True
		except:
			logger.exception(traceback.format_exc())
			return False

	logger.info('Output Queue Empty, Worker: ' + str(workerID) + ' terminating now.')
# how many workers should we use to clear a queue? 
#this means 6 concurrent requests to moz server at any time... 3 for each queue
maxWorkers = 3

# lists of worker greenlets for processing queues
directoryWorkers = []
profileWorkers = []
outputWorkers = []
	
#directoryQueue is a list of member directory pages to fetch
directoryQueue = Queue()

#profileQueue is a list of member profile pages to fetch
profileQueue = Queue()

#outputQueue is a collection of DICTs containing the data to be written to a CSV
outputQueue = Queue()




# the crawler is not necessarily polite, but it's not evil.

# counts for different workers. checked to prevent unusually high failures which may suggest we're impacting the performance of SEOMoz's web server(s). If lots of failures occur, the workers will shut down & system will exit

class stats():
	def __init__(self):

		self.directoryFailed = 0 
		self.directorySuccess = 0

		self.profileFailed = 0
		self.profileSuccess = 0
		
	def updateDirectorySuccess(self):
		self.directorySuccess = self.directorySuccess + 1
		return self.directorySuccess
	
	def updateDirectoryFailed(self):
		self.directoryFailed = self.directoryFailed + 1
		return self.directoryFailed
	
	def updateProfileSuccess(self):
		self.profileSuccess = self.profileSuccess + 1
		return self.profileSuccess
	
	def updateProfileFailed(self):
		self.profileFailed = self.profileFailed + 1
		return self.profileFailed
		
	def returnDirectorySuccess(self):
		return self.directorySuccess
	
	def returnDirectoryFailed(self):
		return self.directoryFailed
	
	def returnProfileSuccess(self):
		return self.profileSuccess
	
	def returnProfileFailed(self):
		return self.profileFailed
		

# an object for tracking our successes and failures. necessary for accessibility within functions
stats = stats()
		

for i in range(298):
	if i > 0:
		page = 'http://www.seomoz.org/users/index?page=' + str(i)
		directoryQueue.put(str(i))
		logger.info('Added ' + page + ' to the Directory Worker Queue')




for i in range(maxWorkers):
	directoryWorkers.append(gevent.spawn(directoryWorker,str(i)))
	


gevent.joinall(directoryWorkers)



	
	
