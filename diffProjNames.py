import elasticsearch
from elasticsearch_dsl import Search, A, Q
from elasticsearch_dsl.query import Range, Bool
import pprint
import re
import csv
import os

### project in topology
from os import listdir
from os.path import isfile, join, splitext

mypath = "/home/users/nhaas/osgclone/topology/projects"
files = [f for f in listdir(mypath) if isfile(join(mypath, f))]

OSGpn = list()
for name in files:
	OSGpn.append(splitext(name)[0].lower())
print OSGpn

### projects in elasticsearch
es = elasticsearch.Elasticsearch(
        ['https://gracc.opensciencegrid.org/q'],
        timeout=300, use_ssl=True, verify_certs=False)

osg_raw_index = 'gracc.osg.raw-*'
osg_summary_index = 'gracc.osg.summary'

r = Bool(
        must=[  Range(EndTime = {'gte': '2019-06-12','lte': '2019-08-12'}) ] )

rtP = Q('bool', must=[Q('match', ResourceType = "Payload")])

s = Search(using=es, index=osg_summary_index)
s = s.query(rtP).query(r)

GRACCpn = dict()
pnNotIncluded = list()
for hit in s.scan():
	GRACCpn[str(hit.ProjectName).strip()] = 1
#	if str(hit.ProjectName).strip() not in GRACCpn:
#		GRACCpn[str(hit.ProjectName).strip()] = 1
#	else:
#		GRACCpn[str(hit.ProjectName).strip()] += 1

for project in GRACCpn:
	if project.lower() not in OSGpn:
		pnNotIncluded.append(project)

#print sorted(pnNotIncluded)

for project in sorted(pnNotIncluded):
	print project
