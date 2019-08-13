import elasticsearch
from elasticsearch_dsl import Search, A, Q
from elasticsearch_dsl.query import Range, Bool
import pprint
import re
import csv

es = elasticsearch.Elasticsearch(
        ['https://gracc.opensciencegrid.org/q'],
        timeout=300, use_ssl=True, verify_certs=False)

osg_raw_index = 'gracc.osg.raw-*'
osg_summary_index = 'gracc.osg.summary'

qB = Q('wildcard', SiteName = "UCSD*")
qP = Q('bool', 
	should=[ Q('match', ReportableVOName = "")])
qP = Q('bool',
	should=[ Q('match', VOName = "")])

rtB = Q('bool', must=[Q('match', ResourceType = "Batch")])
rtP = Q('bool', must=[Q('match', ResourceType = "Payload")])

r = Bool(
	must=[	Range(EndTime = {'gte': '2019-03-01','lte': '2019-06-01'}) ] )

sP = Search(using=es, index=osg_summary_index)
sP = sP.query(r).query(rtP)
#sP = sP.query(r).query(rtP)
#resB = sB.execute()
resP = sP.execute()

VOs = dict()
RVOs = dict()
ProjName = dict()
uncountedVOs = list()
countedVOs = dict()

unique_Hosts = dict()

countVOs = countRVOs = countProj = countHosts = 0
bcount = pcount = bShortNum = bShortTime = btimesum = ptimesum = 0
#print "Batches:"
for hit in sP.scan():
	bcount += 1
	btimesum += hit.WallDuration*hit.Processors/3600
	originalVO = str(hit.VOName).strip().split('/')
	correctedVO = list()
	originalPN = str(hit.ProjectName).strip().split('/')
	correctedPN = list()
	RVO = str(hit.ReportableVOName).strip()

	for string in originalVO:
		if re.search('=', string) == None and string != "":
			correctedVO.append(string)
	for string in originalPN:
		if re.search('=', string) == None and string != "":
			correctedPN.append(string)

	uncountedVOs.append(','.join([RVO,'/'.join(correctedVO),'/'.join(correctedPN)]))			

#	if originalVO == "N/A":
#		print hit.ProbeName
	
#	if re.search('LocalGroup', originalVO) != None:
#		correctedVO = re.findall('^\/([^/]+)\/', originalVO)[0]
#		correctedPN = re.findall('^\/([^/]+)\/', originalVO)[0]

#	if re.search('^\/fermilab\/([^/]+)\/', originalVO) != None: 
#		correctedVO = re.findall('^\/([^/]+)\/', originalVO)[0]
#		correctedPN = re.findall('^\/fermilab\/([^/]+)\/', originalVO)[0]
#		if re.search('^\/fermilab/Role=Production\/', originalVO) != None:
#			print correctedPN 
		
	if str(hit.OIM_Site).strip() not in unique_Hosts:
		countHosts += 1
		unique_Hosts[str(hit.OIM_Site).strip()] = 1
	else:
		unique_Hosts[str(hit.OIM_Site).strip()] += 1

#	if correctedVO not in VOs:
#		countVOs += 1
#		VOs[correctedVO] = 1
#	if str(hit.ReportableVOName).strip() not in RVOs:
#		countRVOs += 1
#		RVOs[str(hit.ReportableVOName).strip()] = 1
#	if correctedPN not in ProjName:
#		countProj += 1
#		ProjName[correctedPN] = 1
#	else:
#		VOs[correctedVO] += 1
#		RVOs[str(hit.ReportableVOName).strip()] += 1
#		ProjName[correctedPN] += 1	

#	if hit.WallDuration > 25*60:
#		print hit.StartTime
#		print hit.EndTime
#		print hit.WallDuration
#		bShortTime += hit.WallDuration*hit.Processors/3600
#		bShortNum += 1

for job in uncountedVOs:
	if job not in countedVOs:
		countedVOs[job] = 1
	else:
		countedVOs[job] +=1

#print "Payloads:"
#or hit in sP.scan():
#	print hit.StartTime
#	print hit.EndTime
#	print hit.Host_description
#	pcount += 1
#	ptimesum += hit.WallDuration*hit.Processors/3600

percShortJobs = bShortTime/btimesum*100.0
pp = pprint.PrettyPrinter(indent=4)

totalJobs = resP.hits.total
print "Got %d total hits" %totalJobs
print "Got %d payloads" %bcount
#print "with %d batches less than 25 min (Walltime = %d)" %(bShortNum,bShortTime)
#print "Got %d payloads" %pcount
#print "Got %d VONames" %len(VOs)
#pp.pprint(VOs)
#print "Got %d ReportableVONames" %len(RVOs)
#pp.pprint(RVOs)
#print "Got %d ProjectName" %len(ProjName)
#pp.pprint(ProjName)

#pp.pprint(countedVOs)
pp.pprint(unique_Hosts)

with open('uniqueFacilities.csv', 'wb') as writeFile:
	writeFile.write("Facility (Host_description), Count\n")
	for key in sorted(unique_Hosts):
		writeFile.write(key + "," + str(unique_Hosts[key])+"\n")

#with open('jobTriples.csv', 'wb') as writeFile:
#	writeFile.write("ReportableVOName,VOName,ProjectName,Count\n")
#	for key in sorted(countedVOs, key=str.lower):
#		writeFile.write(key + "," + str(countedVOs[key])+"\n")

