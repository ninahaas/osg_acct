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
						'gte': '2019-06-03',
						'lte': '2019-06-09'
					}
				},
				'range': {
					'EndTime':{
						'gte': '2019-06-03',
                                        	'lte': '2019-06-09'
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
		tempDict = dict(LocalJobId = hit['_source']['LocalJobId'],
				WallDuration = hit['_source']['WallDuration'],
				ST = dateutil.parser.parse(hit['_source']['StartTime']),
				ET = dateutil.parser.parse(hit['_source']['EndTime']),
				payloads = [] )
		batches.append(tempDict)
		bcount += 1
		btimesum += hit['_source']['WallDuration']
	else:	
		tempDict = dict(LocalJobId = hit['_source']['LocalJobId'],
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
#print "Batches:"
#pp.pprint(sBatches)
#print ""
#print "Payloads:"
#pp.pprint(sPayloads)

threshold = datetime.timedelta(minutes=2)
matchedPL = idleTime = totalWallTime = 0.0

#match payloads to batches by startTime threshold
#need to add second phase for payload chains that begin in the middle of batches
for batch in sBatches:
	currentST = batch['ST']
	totalWallTime += batch['WallDuration']
	idleTime += batch['WallDuration']
	for payload in sPayloads:
		if payload['isActive']:
			if payload['ST'] - currentST <= threshold and payload['ET'] <= batch['ET'] and payload['ST'] >= currentST:
				batch['payloads'].append(payload['LocalJobId'])
				payload['batch'] = batch['LocalJobId']
				currentST = payload['ET']
				payload['isActive'] = False
				matchedPL += 1
				idleTime = idleTime - payload['WallDuration']
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
