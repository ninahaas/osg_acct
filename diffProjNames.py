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

OSGpn = set()
for name in files:
	OSGpn.add(splitext(name)[0].lower())
print OSGpn

### projects in elasticsearch
es = elasticsearch.Elasticsearch(
        ['https://gracc.opensciencegrid.org/q'],
        timeout=300, use_ssl=True, verify_certs=False)

osg_raw_index = 'gracc.osg.raw-*'
osg_summary_index = 'gracc.osg.summary'

r = Bool(
        must=[  Range(EndTime = {'gte': '2019-02-28','lte': '2019-08-28'}) ] )

rtP = Q('bool', must=[Q('match', ResourceType = "Payload")])

s = Search(using=es, index=osg_summary_index)
s = s.query(rtP).query(r)

GRACCpn = dict()
pnNotIncluded = dict()
for hit in s.scan():
	project = str(hit.ProjectName).strip()
	probe = str(hit.ProbeName).strip()
	if project not in GRACCpn:
		GRACCpn[project] = {probe}
	else:
		GRACCpn[project].add(probe)

#	GRACCpn[(str(hit.ProjectName).strip(),str(hit.ProbeName).strip())] = 1
#	if str(hit.ProjectName).strip() not in GRACCpn:
#		GRACCpn[str(hit.ProjectName).strip()] = 1
#	else:
#		GRACCpn[str(hit.ProjectName).strip()] += 1

for project in GRACCpn:
	if project.lower() not in OSGpn:
		pnNotIncluded[project] = GRACCpn[project]

#print sorted(pnNotIncluded)

### find projects in GRACC not in topology

for project in sorted(pnNotIncluded):
	print project + ":	" + str(list(pnNotIncluded[project]))

with open('unregisteredProjNames_6m.csv', 'wb') as writeFile:
	writeFile.write("ProjectName,ProbeName\n")
	for key in sorted(pnNotIncluded):
		writeFile.write(key + ",")
		writeFile.write([str(list(pnNotIncluded[key]))]+"\n")
