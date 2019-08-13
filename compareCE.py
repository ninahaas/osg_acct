import elasticsearch
from elasticsearch_dsl import Search, A, Q
from elasticsearch_dsl.query import Range, Bool
import pprint
import re
import csv
import os
import xml.etree.ElementTree as ET

topCEs = list()
tree = ET.fromstring(os.popen("curl 'https://topology.opensciencegrid.org/rgsummary/xml?service&service_1=on'").read())
for x in tree.findall("./ResourceGroup/Resources/Resource/Name"):
	topCEs.append(x.text)

print topCEs

es = elasticsearch.Elasticsearch(
        ['https://gracc.opensciencegrid.org/q'],
        timeout=300, use_ssl=True, verify_certs=False)

osg_raw_index = 'gracc.osg.raw-*'
osg_summary_index = 'gracc.osg.summary'

#qB = Q('wildcard', SiteName = "UCSD*")
qP = Q('bool', 
	should=[ Q('match', ReportableVOName = "")])

rtB = Q('bool', must=[Q('match', ResourceType = "Batch")])
rtP = Q('bool', must=[Q('match', ResourceType = "Payload")])

r = Bool(
	must=[	Range(EndTime = {'gte': '2019-05-01','lte': '2019-06-01'}) ] )

sP = Search(using=es, index=osg_summary_index)
sB = Search(using=es, index=osg_summary_index)
sP = sP.query(r).query(rtP)
sB = sB.query(r).query(rtB)
resB = sB.execute()
resP = sP.execute()

unique1 = dict()
unique3 = dict()

unique_Hosts = dict()

countVOs = countRVOs = countProj = countHosts = 0
num1 = num3  = 0
#print "Batches:"
for hit in sB.scan():
	if str(hit.OIM_Site).strip() not in unique1:
		unique1[str(hit.OIM_Site).strip()] = 1
	else:
		unique1[str(hit.OIM_Site).strip()] += 1

for hit in sP.scan():
        if str(hit.OIM_Site).strip() not in unique3:
		unique3[str(hit.OIM_Site).strip()] = 1
        else:
		unique3[str(hit.OIM_Site).strip()] += 1

pp = pprint.PrettyPrinter(indent=4)

totalJobs = resP.hits.total
print "Got %d total hits" %totalJobs

print "Got %d (1)" %len(unique1)
pp.pprint(unique1)
print "Got %d (3)" %len(unique3)
pp.pprint(unique3)

with open('comp1_2CEs.csv', 'wb') as writeFile:
	writeFile.write("(1) Facilities (" + str(len(unique1)) + "), Count\n")
	for key in sorted(unique1):
		writeFile.write( key + "," + str(unique1[key])+"\n")
	writeFile.write("\n" + "(3) Facilities + (" + str(len(unique3)) + "), Count\n")
	for key in sorted(unique3):
		writeFile.write( key + "," + str(unique3[key]) + "\n")

diff3 = list()
diff1 = list()

print "diff3:"
for key in unique1:
	if key not in unique3:
		print key
		diff1.append(key)

print "diff1:"
for key in unique3:
	if key not in unique1:
		print key
		diff3.append(key)

#print "diff3:"
#print diff3
#print "diff1:"
#print diff1

#with open('jobTriples.csv', 'wb') as writeFile:
#	writeFile.write("ReportableVOName,VOName,ProjectName,Count\n")
#	for key in sorted(countedVOs, key=str.lower):
#		writeFile.write(key + "," + str(countedVOs[key])+"\n")

