import elasticsearch
from elasticsearch_dsl import Search, A, Q
from elasticsearch_dsl.query import Range, Bool

es = elasticsearch.Elasticsearch(
	['https://gracc.opensciencegrid.org/q'],
	timeout=300, use_ssl=True, verify_certs=False)

osg_raw_index = 'gracc.osg.raw-*'
osg_summary_index = 'gracc.osg.summary'

qP = Q('bool',
	should=[	Q('match', ProjectName = "osg.Ceser") ] )

rtP = Q('bool', must=[Q('match', ResourceType = "Payload")])

sP = Search(using=es, index=osg_summary_index)
sP = sP.query(qP).query(rtP)
resP = sP.execute()

count = 0
for hit in sP.scan():
	count+=1
	print hit.ProbeName

print count
