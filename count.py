"""
Join dim and hash partitioned fact.

Campaign meta:
['campaign_00', 'video campaign "euhvnlkkhy"', 'metadata 1']
['campaign_01', 'video campaign "vuozqifebt"', 'metadata 1']
['campaign_02', 'video campaign "mbtfddmblb"', 'metadata 1']
...

Impressions.
['50000', 'campaign_00', 'video_0', '0.5']
['50018', 'campaign_08', 'video_0', '0.3333333333333333']
['50020', 'campaign_00', 'video_0', '0.25']
['50030', 'campaign_00', 'video_0', '0.2']
...

"""


import os, sys, csv, gzip
import io, time
import tempfile
import asyncio
import aioboto3

import boto3, shutil, string, random
from pprint import pprint as pp
from os.path import isdir, isfile, join

from os import linesep



try:
	import cStringIO
except ImportError:
	import io as cStringIO

		
e=sys.exit	

		
s3			= boto3.client('s3')
bucket_name	= 'crossix-test'
fact_name 	= 'campaign_2k_c10.csv.gz'
ufn			= 'unique_cn.csv'

sql_stmt 	= """SELECT * FROM s3object S """  

cdim_fn		= 'cdim_10_upd.csv'

cdim_s3fn 	= 'cdim_1000.csv.gz'
cfact_s3fn	= 'campaign_100k_c1000.csv.gz'

dim_key_col_pos=0
fact_key_col_pos=1

colsep	= ','	
key_pos_in_fact_fn = 2
part_pos_in_fact_fn = 1
s3	= boto3.client('s3')
dim_cols_to_append = [2]
dim_to_fact_cols = {2:4}
s3_updated_fact_tn = 'FACT_IMPRESSIONS_UPDATED'
if 0:

	

	try:
		import __builtin__ as builtins
	except:
		import builtins
		
	builtins.bucket_name=bucket_name
	builtins.fact_name=fact_name
	builtins.dim_key_col_pos=dim_key_col_pos
	builtins.fact_key_col_pos=fact_key_col_pos
	builtins.key_pos_in_fact_fn=key_pos_in_fact_fn
	builtins.part_pos_in_fact_fn=part_pos_in_fact_fn
	builtins.s3=s3
	builtins.colsep=colsep
	builtins.dim_to_fact_cols=dim_to_fact_cols
	builtins.s3_updated_fact_tn=s3_updated_fact_tn
	from include.update import  get_files_to_update, update_and_upload


if 0: #Create updated dim
	with open(cdim_fn, mode='w') as fh:
		
		csvw = csv.writer(fh, delimiter = ',', quotechar = '"', lineterminator = '\n',  quoting=csv.QUOTE_MINIMAL)
		for i in range(10):
			row=['campaign_%02d' % (i), 	'video campaign "%s"' % \
			(''.join(random.choice(string.ascii_lowercase) for i in range(100))), 'metadata 2']
			print (row)
			csvw.writerow(row)
			
	e()
	
	
if 0: #Create fact
	with open('campaign_100k_c1000.csv', mode='w') as fh:
		
		csvw = csv.writer(fh, delimiter = ',', quotechar = '"', lineterminator = '\n',  quoting=csv.QUOTE_MINIMAL)
		for i in range(100000):		
			row=[50000+i,'campaign_%04d' % (int(i)%1000), 	'video_%s' % (int(i)//100), 	1/(i//10+2)]
			#print(row)
			csvw.writerow(row)
			
	e()
	
	
	
s = time.perf_counter()

DIM_COLCNT=1
dim=[]
s3_prefix = 'tables'
s3_dimtn = 'DIM_CAMPAIGNS_UPD'
s3_dimfn = 'cdim_10_upd.csv.gz'	
if 1: #count dim from s3
	sql_stmt 	= """SELECT count(*) cnt FROM s3object S"""  
	req_dim = s3.select_object_content(
		Bucket	= bucket_name,
		Key		= '%s/%s/%s' % (s3_prefix, s3_dimtn, s3_dimfn),
		ExpressionType	= 'SQL',
		Expression		= sql_stmt,
		InputSerialization 	= { 'CompressionType':'GZIP', 
		'CSV': {'FileHeaderInfo': 'None'}},
		OutputSerialization = {'CSV': {
					'RecordDelimiter': linesep,
					'FieldDelimiter': ','}},
		#ScanRange = {'Start':0, 'End':40000 }
	)


	def count_dim(req, dim):

		for event in req['Payload']:
			if 'Records' in event:
				rr=event['Records']['Payload'].decode('utf-8')
				for i, rec in enumerate(rr.split(linesep)):
					if rec:
						row=rec.split(colsep)
						if row:
							dim.append(row)
							print(row)
						else:
							raise

	count_dim(req_dim, dim)
	pp(dim)
	#e()







if 1:
	FACT_COLCNT=4
	fact={}
	s3_prefix = 'tables'
	s3_fact_tn = 'FACT_IMPRESSIONS_UPDATED'
	part_name = 'B_0'
	#s3_fact_fn = '%s.%s.campaign_00.campaign.csv.gz' % (s3_fact_tn, part_name)
	
	
	
	s3r		= boto3.resource('s3')
	print(bucket_name)
	mybucket = s3r.Bucket(bucket_name)
	
	bucket_prefix="%s/%s/" % (s3_prefix, s3_fact_tn)
	print(bucket_prefix)
	objs = mybucket.objects.filter(
	Prefix = bucket_prefix)

	
		
	def get_key_from_fn(fn):
		
		return fn.split(".")[key_pos_in_fact_fn]
	def get_part_from_fn(fn):		
		return fn.split(".")[part_pos_in_fact_fn]

	async def get_files_to_scan(objs, queue):

		for obj in objs:
			#print(obj.key)
			
			#path, filename = os.path.split(obj.key)
			#part_name	= get_part_from_fn(filename)
			if obj.key:
				#print(filename)
				await queue.put(obj.key)
			else:
				raise Exception('filename is not set')
		await queue.put(None)
		

	async def count_rows_in_file(counts_queue, s3_key):
		if 1: #Fact partitioner

			sql_stmt 	= """SELECT count(*) FROM s3object S"""  
			req_fact = s3.select_object_content(
				Bucket	= bucket_name,
				Key		= s3_key,
				ExpressionType	= 'SQL',
				Expression		= sql_stmt,
				InputSerialization 	= { 'CompressionType':'GZIP', 
				'CSV': {'FileHeaderInfo': 'None'}},
				OutputSerialization = {'CSV': {
							'RecordDelimiter': linesep,
							'FieldDelimiter': ','}},
				
			)
	
		for event in req_fact['Payload']:
			if 'Records' in event:
				rr=event['Records']['Payload'].decode('utf-8')
				for i, rec in enumerate(rr.split(linesep)):
					if rec:
						row=rec.split(colsep)
						if row:
							print('File line count:', row[0])
							await counts_queue.put(row)

		#await counts_queue.put(None)
		
	
	async def count_rows_in_table(queue, counts_queue):
		
		readers=[]
		qz=[]
		while True: 
			row = await queue.get()
			queue.task_done()
			if row:
				s3_key = row
				print('S3 key: ',s3_key)
				if 1:
					readers.append(asyncio.create_task(count_rows_in_file(counts_queue, s3_key)))

			else:
				break
		await asyncio.gather(*readers)
		await counts_queue.put(None)
		
	async def merge_counts(counts_queue):
		total = 0
		while True: 
			row = await counts_queue.get()
			counts_queue.task_done()
			if row:
				cnt = int(row[0])
				total +=cnt

			else:
				break
		print('Total rows:', total)
	async def s3_count_fact():
		queue = asyncio.Queue()
		counts_queue = asyncio.Queue()
		
		
		await asyncio.gather( get_files_to_scan(objs, queue), count_rows_in_table(queue, counts_queue), merge_counts(counts_queue))	
		await queue.join()
		
		

	if 1:
		asyncio.run(s3_count_fact())
		elapsed = time.perf_counter() - s
		print(f"{__file__} executed in {elapsed:0.2f} seconds.")

	e()

elapsed = time.perf_counter() - s
print(f"{__file__} executed in {elapsed:0.2f} seconds.")
