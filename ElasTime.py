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
	should=[	Q('match', Host_description = "UCSDT2"),
				Q('match', Host_description = "Comet") ] )

qVO = Q('bool',
	must=[Q('wildcard', ReportableVOName = "*cms*") ] )

rtB = Q('bool', must=[Q('match', ResourceType = "Batch")])
rtP = Q('bool', must=[Q('match', ResourceType = "Payload")])

r = Bool(
	must=[	#Range(StartTime = {'gte': '2019-06-01','lte': '2019-06-25'}),
		Range(EndTime = {'gte': '2019-03-01','lte': '2019-04-25'}) 
		] )

sB = Search(using=es, index=osg_raw_index)
sP = Search(using=es, index=osg_summary_index)
sB = sB.query(qB).query(r).query(rtB)
sP = sP.query(qB).query(r).query(rtP)
resB = sB.execute()
resP = sP.execute()

VOs = RVOs = ProjName = HostDescrip = list()
countVOs = countRVOs = countProj = countHosts = 0
bcount = pcount = bShortNum = bShortTime = btimesum = ptimesum = 0
#print "Batches:"
for hit in sB.scan():
	bcount += 1
	btimesum += hit.WallDuration*hit.Processors/3600
#	if hit.VOName not in VOs:
#		countVOs += 1
#		VOs.append(hit.VOName)
#	if hit.ReportableVOName not in RVOs:
#		countRVOs += 1
#		RVOs.append(hit.VOName)
#	if hit.ProjectName not in ProjName:
#		countProj += 1
#		ProjName.append(hit.ProjectName)
#	if hit.Host_description not in HostDescrip:
#		countHosts +=1
#		HostDescrip.append(hit.Host_description)
	if hit.WallDuration < 25*60:
		print hit.StartTime
		print hit.EndTime
		print hit.WallDuration
		bShortTime += hit.WallDuration*hit.Processors/3600
		bShortNum += 1
#print "Payloads:"
#for hit in sP.scan():
#	print hit.StartTime
#	print hit.EndTime
#	print hit.Host_description
#	pcount += 1
#	ptimesum += hit.WallDuration*hit.Processors/3600

#percShortJobs = bShortTime/btimesum*100.0

totalJobs = resB.hits.total + resP.hits.total
print "Got %d total hits" %totalJobs
print "Got %d batchs" %bcount
print "with %d batches less than 25 min (Walltime = %d)" %(bShortNum,bShortTime)
#print "Got %d payloads" %pcount
#print "Got %d VONames" %countVOs
#print VOs
#print "Got %d ReportableVONames" %countRVOs
#print RVOs
#print "Got %d ProjectName" %countProj
#print ProjName
#print "Got %d Host_description" %countHosts
#print HostDescrip
print "batchs total Core hours = %d" %btimesum
#print "payloads total Core hours = %d" %ptimesum
