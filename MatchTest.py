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

#qB = Q('wildcard', SiteName = "UCSD*")
#qP = Q('bool',
#	should=[	Q('match', Host_description = "UCSDT2"),
#				Q('match', Host_description = "Comet") ] )
q = Q('bool', must=[	Q('match', Host = 'mikan09.farm.particle.cz'),
							Q('match', ReportableVOName = 'dune') ] )

rtB = Q('bool', must=[Q('match', ResourceType = "Batch")])
rtP = Q('bool', must=[Q('match', ResourceType = "Payload")])

r = Bool(	must=[	Range(StartTime = {'gte': '2019-06-07','lte': '2019-06-14'}),
							Range(EndTime = {'gte': '2019-06-07','lte': '2019-06-14'}) ] )

sB = Search(using=es, index=osg_raw_index)
sP = Search(using=es, index=osg_raw_index)
sB = sB.query(q).query(r).query(rtB)
sP = sP.query(q).query(r).query(rtP)
resB = sB.execute()
resP = sP.execute()

bcount = pcount = btimesum = ptimesum = 0
batches = []
p_loads = []

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
	p_loads.append(tempDict)
	pcount += 1
	ptimesum += hit.WallDuration

pp = pprint.PrettyPrinter(indent=4)
pp.pprint(batches)
pp.pprint(p_loads)

totalJobs = resB.hits.total + resP.hits.total
print "Got %d total hits" %totalJobs
print "Got %d batchs" %bcount
#print "with %d batches less than 25 min (Walltime = %d)" %(bShortNum,bShortTime)
print "Got %d payloads" %pcount
print "batchs total Core hours = %d" %btimesum
print "payloads total Core hours = %d" %ptimesum

#batches = ['b1','b2','b3']
#p_loads = ['p1','p2','p3','p4']

def write_for_loops( batches, p_load ):
	### Synthesize code like this:
	###
	### for b_0 in batches:
	###	associate( p_loads[0], b_0 )
	###	for b_1 in batches:
	###		assoicate( p_loads[1], b_1 )
	###		for b_2 in batches:
	###			associate( p_loads[2], b_2 )

	code_lines = []
	code_lines.append('def ' + 'depth_first_search' + '( batches, p_loads ):\n')
	indent = ''
	for p_load_no in range ( 0, len(p_loads) ):
		code_lines.append(indent + '	for b_' + str(p_load_no) + ' in batches:\n')
		code_lines.append(indent + '		associate(p_loads[' + str(p_load_no) + '], b_' + str(p_load_no) + ' )\n')
		if (p_load_no == (len(p_loads) - 1)):
			code_lines.append(indent + '	evaluate()\n')
		indent = indent + '	'
	function_code = ''.join( code_lines )
	print('--- function_code:\n' + function_code)
	return function_code

def associate( p_load, batch):
	if fits( p_load, batch ):
		print('	--- associating p_load ' + p_load + ' with batch ' + batch)
		assign_dict[ p_load ] = batch

###to check if start/end times of payload are consistent with batch and payloads already assigned to batch
def fits( p_load, batch ):
	return_val = False
	if p_load[ST] >= batch[ST] and p_load[ET] <= batch[ET]:		###check time constraints here
		return_val = True
	return return_val

###to compute score for this configuration -- save the best and the corresponding configuration
def evaluate():
	global best_score
	global best_configuration
	print('	--- evaluate configuration: assign_dict: ' + str(assign_dict) + '	best_score: ' + str(best_score))
	score = 17		###compute score here:
	score = compute_score( assign_dict )
	if (score > best_score):
		best_score = score
		best_configuration = assign_dict.copy()		###Use copy() or deep_copy() to make sure best_configuration is not modified when assign_dict is modified

#======== Main ========

function_code = write_for_loops( batches, p_loads ) ### generate the code for 'depth_first_search' (nested for loops)

exec( function_code )	#implement the function 'depth_first_search'

depth_first_search( batches, p_loads )	### execute the function 'depth_first_search'
