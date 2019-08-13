import elasticsearch
from elasticsearch_dsl import Search, A, Q
#import logging
import operator
import sys
import os
import dateutil.parser
import pprint
import datetime

#logging.basicConfig(level=logging.WARN)
es = elasticsearch.Elasticsearch(
        ['https://gracc.opensciencegrid.org/q'],
        timeout=300, use_ssl=True, verify_certs=False)

osg_raw_index = 'gracc.osg.raw-*'
osg_summary_index = 'gracc.osg.summary'

#res = es.search(index=osg_raw_index,body={
#	'query':{
#		'match':{
#			'ResourceType':"Payload"
#		}
#	}
#})

#print("Got %d Hits" % res['hits']['total'])

#Access ElasticSearch data
res = es.search(index=osg_raw_index,body={
	'from': 0, 'size': 10000,
	'query':{
		'bool': {
			'must': [
				{'match':{'Host': "mikan09.farm.particle.cz"}},
				{'match':{'ReportableVOName':"dune"}},
#				{'match':{'StartTime':"2019-06-01"}},
#				{'match':{'EndTime':"2019-06-01"}},
			{
				'range': {
					'StartTime': {
						'gte': '2019-05-06',
						'lte': '2019-05-14'
					}
				},
				'range': {
					'EndTime':{
						'gte': '2019-05-06',
						'lte': '2019-05-14'
					}
				}
			}
			]
		}
	}
})

batches = []
payloads = []

#Store data into lists of batches and payloads
bcount = pcount = btimesum = ptimesum = 0;
for hit in res['hits']['hits']:
	if hit['_source']['ResourceType'] == 'Batch':
		tempDict = dict(	LocalJobId = hit['_source']['LocalJobId'],
							WallDuration = hit['_source']['WallDuration'],
							ST = dateutil.parser.parse(hit['_source']['StartTime']),
							ET = dateutil.parser.parse(hit['_source']['EndTime']),
							payloads = [],
							pLastET = dateutil.parser.parse(hit['_source']['StartTime']) )
		batches.append(tempDict)
		bcount += 1
		btimesum += hit['_source']['WallDuration']

	else:	
		tempDict = dict(	LocalJobId = hit['_source']['LocalJobId'],
        					isActive = True,
							WallDuration = hit['_source']['WallDuration'],
							ST = dateutil.parser.parse(hit['_source']['StartTime']),
							ET = dateutil.parser.parse(hit['_source']['EndTime']),
							batch = "" )
		payloads.append(tempDict)
		pcount += 1
		ptimesum += hit['_source']['WallDuration']

#	print hit['_source']['ResourceType']
#	print hit['_source']['StartTime']
#	print hit['_source']['EndTime']
#	print ' '

#Sort lists by startTime
sBatches = sorted(batches, key=lambda k: k['ST'])
sPayloads = sorted(payloads, key=lambda k: k['ST'])
pp = pprint.PrettyPrinter(indent=4)

threshold = datetime.timedelta(minutes= 3)
threshold2 = datetime.timedelta(minutes= 5)
matchedPL = idleTime = totalWallTime = 0.0

totalWallTime = btimesum
idleTime = btimesum

hasMatch = True
#PHASE1: match payloads to batches: 10m after batch ST, 3m after payload ST
while hasMatch == True:
	hasMatch = False
	for payload in sPayloads:
		if payload['isActive']:
			for batch in sBatches:
				if batch['ST'] == batch['pLastET']:
					if payload['ST'] - batch['pLastET'] <= threshold2 and payload['ET'] <= batch['ET'] and payload['ST'] >= batch['pLastET']:
						batch['payloads'].append(payload['LocalJobId'])
						payload['batch'] = batch['LocalJobId']
						batch['pLastET'] = payload['ET']
						payload['isActive'] = False
						matchedPL += 1
						idleTime = idleTime - payload['WallDuration']
						hasMatch = True
						break
				else:
					if payload['ST'] - batch['pLastET'] <= threshold2 and payload['ET'] <= batch['ET'] and payload['ST'] >= batch['pLastET']:
						batch['payloads'].append(payload['LocalJobId'])
						payload['batch'] = batch['LocalJobId']
						batch['pLastET'] = payload['ET']
						payload['isActive'] = False
						matchedPL += 1
						idleTime = idleTime - payload['WallDuration']
						hasMatch = True
						break

#PHASE2: match payloads to batches: 10m 
pp = pprint.PrettyPrinter(indent=4)
pp.pprint(sBatches)
pp.pprint(sPayloads)

percPL = 100*matchedPL/len(sPayloads)
percWT = 100*idleTime/totalWallTime

print "Batches:"
pp.pprint(sBatches)
print ""
print "Payloads:"
pp.pprint(sPayloads)



print "%d/%d payloads matched (%d%%)" %(matchedPL,len(sPayloads),percPL)
print "%d/%d batch wall time used (%d%%)" %(idleTime,totalWallTime,percWT)
print("Got %d total hits" % res['hits']['total'])
print "Got %d batchs" %bcount
print "Got %d payloads" %pcount
print "batchs total CPU time = %d" %btimesum
print "payloads total CPU time = %d" %ptimesum
#print "Query took %i milliseconds" % s.took 

#print "Query got %i hits" % s.hits.total
