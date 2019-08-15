import elasticsearch
from elasticsearch_dsl import Search, A, Q
from elasticsearch_dsl.query import Range, Bool
import pprint
import re
import csv
import os
import pprint

### project in topology
from os import listdir
from os.path import isfile, join, splitext

mypath = "/home/users/nhaas/osgclone/topology/projects"
files = [f for f in listdir(mypath) if isfile(join(mypath, f))]

OSGpn = set()
for name in files:
	OSGpn.add(splitext(name)[0].lower())
#print OSGpn

### projects in elasticsearch
es = elasticsearch.Elasticsearch(
        ['https://gracc.opensciencegrid.org/q'],
        timeout=300, use_ssl=True, verify_certs=False)

osg_raw_index = 'gracc.osg.raw-*'
osg_summary_index = 'gracc.osg.summary'

r = Bool(
        must=[  Range(EndTime = {'gte': '2019-07-12','lte': '2019-08-12'}) ] )

rtP = Q('bool', must=[Q('match', ResourceType = "Payload")])

s = Search(using=es, index=osg_summary_index)
s = s.query(rtP).query(r)

GRACCpn = dict()
pnNotIncluded = list()
for hit in s.scan():
        GRACCpn[str(hit.ProjectName).strip()] = 1

### find projects in GRACC not in topology
for project in GRACCpn:
        if project.lower() not in OSGpn:
                pnNotIncluded.append(project)

#print sorted(pnNotIncluded)

pnNotIncluded = sorted(pnNotIncluded)
for project in pnNotIncluded:
        print project

### find raw records for unregistered project names

### synthesizes python code to query the list of project names
def write_query(projList, field, variable):
	code_lines = []
	code_lines.append(variable + ' = Bool(\n	should=[')
	for project in projList:
		if re.search("^'.*'$", project) != None:
			code_lines.append('Q(\'match\',' + field + ' = \'\\\'' + project.strip('\'') + '\\\'\'),\n')
		else:
			code_lines.append('Q(\'match\',' + field + ' = \'' + project + '\'),\n')
	code_lines.append('] )')

	function_code = ''.join(code_lines)
	print('--- function_code:\n' + function_code)
	return function_code

### create seperate queries for records with and without ProjectName field

function_code = write_query(pnNotIncluded, 'ProjectName', 'sPN')
exec(function_code)
function_code = write_query(pnNotIncluded, 'VOName', 'sVO')
exec(function_code)

ne = Bool(
	must=[Q('exists', field = 'ProjectName')],
	must_not=[Q('match', ProjectName = 'GLOW')] ) 

nd = Bool(
	must_not=[Q('exists', field = 'ProjectName')] )

rtP = Q('bool', must=[Q('match', ResourceType = "Payload")])

#s = Search(using=es, index=osg_raw_index)
#s = s.query(rtP).query(r)

se = Search(using=es, index=osg_raw_index)
sd = Search(using=es, index=osg_raw_index)
se = se.query(rtP).query(r).query(ne).query(sPN)
sd = sd.query(rtP).query(r).query(nd).query(sVO)

pnData = dict()
voData = dict()

###find all unique (Id, Probe) tuples

for hit in se.scan():
	project = str(hit.ProjectName).strip()
	Id = str(hit.LocalUserId).strip()
	probe = str(hit.ProbeName).strip()
	if project not in pnData:
		pnData[project] = {(Id, probe)}
	else:
		pnData[project].add((Id, probe))
	
for hit in sd.scan():
	project = str(hit.VOName).strip()
	Id = str(hit.LocalUserId).strip()
	probe = str(hit.ProbeName).strip()
	if project not in pnData:
		voData[project] = {(Id, probe)}
	else:
		voData[project].add((Id, probe))

pp = pprint.PrettyPrinter(indent=4)
print "ProjectName that are assigned"
pp.pprint(pnData)
print "ProjectName that = VONames"
pp.pprint(voData)

