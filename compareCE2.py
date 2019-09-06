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
rtP = Q('bool', must=[Q('match', ResourceType = "Payload")], 
		must_not=[Q('exists', field = 'OIM_Match')])
rtP_intop = Q('bool', 	must=[	Q('match', ResourceType = "Payload"),
				Q('exists', field = 'OIM_Match')]) 

r = Bool(
	must=[	Range(EndTime = {'gte': '2019-05-01','lte': '2019-06-01'}) ] )

sP_intop = Search(using=es, index=osg_summary_index)
sP = Search(using=es, index=osg_summary_index)
sB = Search(using=es, index=osg_summary_index)
sP_intop = sP.query(r).query(rtP_intop)
sP = sP.query(r).query(rtP)
sB = sB.query(r).query(rtB)
resB = sB.execute()
resP = sP.execute()

unique1 = dict()
unique3 = dict()
unique3_intop = dict()


unique_Hosts = dict()

countVOs = countRVOs = countProj = countHosts = 0
num1 = num3  = 0
#print "Batches:"
for hit in sB.scan():
	name = str(hit.OIM_Site).strip()
	time = float(str(hit.WallDuration).strip())/3600
	if name not in unique1:
		unique1[name] = time
	else:
		unique1[name] += time

for hit in sP.scan():
        name = str(hit.OIM_Site).strip()
        time = float(str(hit.WallDuration).strip())/3600
        if name not in unique3:
                unique3[name] = time
        else:
                unique3[name] += time
for hit in sP_intop.scan():
	name = str(hit.OIM_Site).strip()
	time = float(str(hit.WallDuration).strip())/3600	
	if name not in unique3_intop:
		unique3_intop[name] = time
	else:
		unique3_intop[name] += time
pp = pprint.PrettyPrinter(indent=4)

print "Got %d (1)" %len(unique1)
pp.pprint(unique1)
print "Got %d (3)" %len(unique3)
pp.pprint(unique3)
print "Got %d (3) in top" %len(unique3_intop)
pp.pprint(unique3_intop)

num1 = num3 = 0
for key in unique1:
	num1 += unique1[key]
for key in unique3:
	num3 += unique3[key]

with open('comp1_3CEs.csv', 'wb') as writeFile:
	writeFile.write("(1) Facilities (" + str(len(unique1)) + "),Core Hours (" + str(int(num1)) + " total)\n")
	for key in sorted(unique1):
		writeFile.write( key + "," + str(int(unique1[key])) + "\n")
	writeFile.write("\n" + "(3) Facilities + (" + str(len(unique3)) + "),Core Hours (" + str(int(num3)) + " total)\n")
	for key in sorted(unique3):
		writeFile.write( key + "," + str(int(unique3[key])) + "\n")

diff3 = list()
diff1 = list()

# CE in GRACC payload but not in topology 

for key in unique1:
	if key not in unique3:
		diff1.append(key)

# CE in GRACC pilot 

for key in unique3:
	if key not in unique1:
		diff3.append(key)

print "Got %d in diff3:" %len(diff3)
for name in sorted(diff3):
	print name
print "\nGot %d in diff1:" %len(diff1)
for name in sorted(diff1):
	print name

#with open('jobTriples.csv', 'wb') as writeFile:
#	writeFile.write("ReportableVOName,VOName,ProjectName,Count\n")
#	for key in sorted(countedVOs, key=str.lower):
#		writeFile.write(key + "," + str(countedVOs[key])+"\n")

