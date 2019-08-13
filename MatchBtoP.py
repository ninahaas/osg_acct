import elasticsearch
from elasticsearch_dsl import Search, A, Q
from elasticsearch_dsl.query import Range, Bool
import operator
import sys
import os
import dateutil.parser
import pprint
import datetime

es = elasticsearch.Elasticsearch(
        ['https://gracc.opensciencegrid.org/q'],
        timeout=300, use_ssl=True, verify_certs=False)

osg_raw_index = 'gracc.osg.raw-*'
osg_summary_index = 'gracc.osg.summary'

qB = Q('wildcard', SiteName = "UCSD*")
qP = Q('bool',
	should=[	Q('match', Host_description = "UCSDT2"),
				Q('match', Host_description = "Comet") ] )

#qVO = Q('bool',
#	must=[Q('wildcard', ReportableVOName = "*cms*") ] )

rtB = Q('bool', must=[Q('match', ResourceType = "Batch")])
rtP = Q('bool', must=[Q('match', ResourceType = "Payload")])

r = Bool(
	must=[	Range(StartTime = {'gte': '2019-06-01','lte': '2019-06-07'}),
				Range(EndTime = {'gte': '2019-06-01','lte': '2019-06-07'}) ] )

sB = Search(using=es, index=osg_raw_index)
sP = Search(using=es, index=osg_raw_index)
sB = sB.query(qB).query(r).query(rtB)
sP = sP.query(qB).query(r).query(rtP)
resB = sB.execute()
resP = sP.execute()

bcount = pcount = btimesum = ptimesum = 0
batches = []
payloads = []

#Store data into lists of batches and payloads
bcount = pcount = btimesum = ptimesum = 0;
for hit in sB.scan():
	tempDict = dict(	LocalJobId = hit.LocalJobId,
							WallDuration = hit.WallDuration,
							ST = dateutil.parser.parse(hit.StartTime),
							ET = dateutil.parser.parse(hit.EndTime),
							payloads = [],
							pLastET = dateutil.parser.parse(hit.StartTime) )
	batches.append(tempDict)
	bcount += 1
	btimesum += hit.WallDuration

for hit in sP.scan():
	tempDict = dict(	LocalJobId = hit.LocalJobId,
							isActive = True,
							WallDuration = hit.WallDuration,
     						ST = dateutil.parser.parse(hit.StartTime),
     						ET = dateutil.parser.parse(hit.EndTime),
     						batch = "" )
	payloads.append(tempDict)
	pcount += 1
	ptimesum += hit.WallDuration

totalJobs = resB.hits.total + resP.hits.total
print "Got %d total hits" %totalJobs
print "Got %d batchs" %bcount
#print "with %d batches less than 25 min (Walltime = %d)" %(bShortNum,bShortTime)
print "Got %d payloads" %pcount
print "batchs total Core hours = %d" %btimesum
print "payloads total Core hours = %d" %ptimesum
