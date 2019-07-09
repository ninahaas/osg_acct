import elasticsearch
from elasticsearch_dsl import Search, A, Q
from elasticsearch_dsl.query import Range, Bool

es = elasticsearch.Elasticsearch(
        ['https://gracc.opensciencegrid.org/q'],
        timeout=300, use_ssl=True, verify_certs=False)

osg_raw_index = 'gracc.osg.raw-*'
osg_summary_index = 'gracc.osg.summary'

qB = Q('wildcard', SiteName = "UCSD*")
qP = Q('bool',
	should=[Q('match', Host_description = "UCSDT2"),
		Q('match', Host_description = "Comet") ] )

qVO = Q('bool',
	must_not=[Q('wildcard', ReportableVOName = "*cms*") ] )

rtB = Q('bool', must=[Q('match', ResourceType = "Batch")])
rtP = Q('bool', must=[Q('match', ResourceType = "Payload")])

r = Bool(
	must=[Range(StartTime = {'gte': '2019-06-09','lte': '2019-06-15'}),
		Range(EndTime = {'gte': '2019-06-09','lte': '2019-06-15'}) ] )

sB = Search(using=es, index=osg_raw_index)
sP = Search(using=es, index=osg_raw_index)
sB = sB.query(qB).query(qVO).query(r).query(rtB)
sP = sP.query(qP).query(qVO).query(r).query(rtP)
resB = sB.execute()
resP = sP.execute()

bcount = pcount = btimesum = ptimesum = 0;
print "Batches:"
for hit in sB.scan():
	print hit.StartTime
	print hit.EndTime
	print hit.SiteName
        bcount += 1
        btimesum += hit.WallDuration
print "Payloads:"
for hit in sP.scan():
	print hit.StartTime
	print hit.EndTime
	print hit.Host_description
        pcount += 1
        ptimesum += hit.WallDuration
	
totalJobs = resB.hits.total+resP.hits.total
print "Got %d total hits" %totalJobs
print "Got %d batchs" %bcount
print "Got %d payloads" %pcount
print "batchs total CPU time = %d" %btimesum
print "payloads total CPU time = %d" %ptimesum
