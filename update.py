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
if 1:

	s3_joined_fact_tn = 'FACT_IMPRESSIONS_JOINED'

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
builtins.dim_cols_to_append=dim_cols_to_append
builtins.s3_joined_fact_tn=s3_joined_fact_tn
#from include.join import  producer,consumer


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

if 0: #Fact partitioner
	FACT_COLCNT=4
	fact={}
	s3_prefix = 'tables'
	s3_fact_tn = 'FACT_IMPRESSIONS'

	req_fact = s3.select_object_content(
		Bucket	= bucket_name,
		Key		= fact_name,
		ExpressionType	= 'SQL',
		Expression		= sql_stmt,
		InputSerialization 	= { 'CompressionType':'GZIP', 
		'CSV': {'FileHeaderInfo': 'None'}},
		OutputSerialization = {'CSV': {
					'RecordDelimiter': '\n',
					'FieldDelimiter': ','}},
		#ScanRange = {'Start':0, 'End':40000 }
	)



		
	async def partition_fact():
		queue = asyncio.Queue()
		cid_queue = asyncio.Queue()
		coro= {}
		
		await asyncio.gather( producer(req_fact,queue), consumer(queue, coro,cid_queue),\
			upload_gzipped(coro,cid_queue, bucket_name, s3_prefix, s3_fact_tn ))	
		await queue.join()
		await cid_queue.join()


	
	if 1:
		asyncio.run(partition_fact())
	e()

DIM_COLCNT=3
dim={}
s3_prefix = 'tables'
s3_dimtn = 'DIM_CAMPAIGNS_UPD'
s3_dimfn = 'cdim_10_upd.csv.gz'	
if 1: #read dim from s3
	sql_stmt 	= """SELECT * FROM s3object S LIMIT 1"""  
	req_dim = s3.select_object_content(
		Bucket	= bucket_name,
		Key		= '%s/%s/%s' % (s3_prefix, s3_dimtn, s3_dimfn),
		ExpressionType	= 'SQL',
		Expression		= sql_stmt,
		InputSerialization 	= { 'CompressionType':'GZIP', 
		'CSV': {'FileHeaderInfo': 'None'}},
		OutputSerialization = {'CSV': {
					'RecordDelimiter': '\n',
					'FieldDelimiter': ','}},
		#ScanRange = {'Start':0, 'End':40000 }
	)


	def read_dim(req, dim, colid):

		leftover=''
		for event in req['Payload']:
			if 'Records' in event:
				rr=event['Records']['Payload'].decode('utf-8')
				for i, rec in enumerate(rr.split(os.linesep)):
					if rec:
						if leftover:
							rec = leftover+rec
							leftover = ''
						row=rec.split(colsep)
						if row:
							if len(row) == DIM_COLCNT:
								key=row[colid]
								assert key not in dim
								dim[key] = row
								print(row)
							else:
								leftover = rec

	read_dim(req_dim, dim,0)
	pp(dim)
	#e()







FACT_COLCNT=1
fact={}
s3_prefix = 'tables'
s3_fact_tn = 'FACT_IMPRESSIONS'
s3_fact_pn = 'B_0'
s3_fact_fn = 'FACT_IMPRESSIONS.B_0.campaign_00.campaign.csv.gz'
key='%s/%s/%s/%s' % (s3_prefix, s3_fact_tn,s3_fact_pn, s3_fact_fn)
print(key)
if 0: #read dim from s3
	sql_stmt 	= """SELECT count(*) cnt FROM s3object S"""  
	req_dim = s3.select_object_content(
		Bucket	= bucket_name,
		Key		= key,
		ExpressionType	= 'SQL',
		Expression		= sql_stmt,
		InputSerialization 	= { 'CompressionType':'GZIP', 
		'CSV': {'FileHeaderInfo': 'None'}},
		OutputSerialization = {'CSV': {
					'RecordDelimiter': '\n',
					'FieldDelimiter': ','}},
		#ScanRange = {'Start':0, 'End':40000 }
	)


	def read_fact(req,  colid):

		leftover=''
		for event in req['Payload']:
			if 'Records' in event:
				rr=event['Records']['Payload'].decode('utf-8')
				for i, rec in enumerate(rr.split(os.linesep)):
					if rec:
						if leftover:
							rec = leftover+rec
							leftover = ''
						row=rec.split(colsep)
						if row:
							if len(row) == FACT_COLCNT:
								#key=row[colid]
								#assert key not in dim
								#dim[key] = row
								print(row)
							else:
								leftover = rec

	read_fact(req_dim, 1)
	#pp(dim)
	e()

if 1:
	FACT_COLCNT=4
	fact={}
	s3_prefix = 'tables'
	s3_fact_tn = 'FACT_IMPRESSIONS_JOINED'
	part_name = 'B_0'
	s3_fact_fn = '%s.%s.campaign_00.campaign.csv.gz' % (s3_fact_tn, part_name)
	
	s3_updated_fact_tn = 'FACT_IMPRESSIONS_UPDATED'
	dim_to_fact_cols = {2:4}
	s3r		= boto3.resource('s3')
	mybucket = s3r.Bucket(bucket_name)
	
	bucket_prefix="%s/%s/" % (s3_prefix, s3_fact_tn)
	objs = mybucket.objects.filter(
	Prefix = bucket_prefix)
	
	def get_key_from_fn(fn):
		
		return fn.split(".")[key_pos_in_fact_fn]
	def get_part_from_fn(fn):		
		return fn.split(".")[part_pos_in_fact_fn]
	async def get_files_to_update(objs, queue,dim):

		for obj in objs:
			#print(obj.key)
			
			path, filename = os.path.split(obj.key)
			dim_key		= get_key_from_fn(filename)
			part_name	= get_part_from_fn(filename)
			if dim_key in dim:
				#print(dim_key, filename)
				await queue.put((path, filename, dim_key, part_name))
			else:
				if 0:
					raise Exception('Campaign key "%s" does not exist in table %s' % (key,s3_fact_tn))
		await queue.put(None)
	async def read_fact_file(queue, s3_prefix, s3_fn):
		if 1: #Fact partitioner
			FACT_COLCNT=5
			s3_prefix = 'tables'
			s3_fact_tn = 'FACT_IMPRESSIONS_APPENDED'
			s3_fact_pn = 'B_0'
			s3_fact_fn = '%s.%s.campaign_00.campaign.csv.gz' % (s3_fact_tn, s3_fact_pn)
			key='%s/%s/%s/%s' % (s3_prefix, s3_fact_tn,s3_fact_pn, s3_fact_fn)
			print(33333,key)
			sql_stmt 	= """SELECT * FROM s3object S LIMIT 2"""  
			req_fact = s3.select_object_content(
				Bucket	= bucket_name,
				Key		= key,
				ExpressionType	= 'SQL',
				Expression		= sql_stmt,
				InputSerialization 	= { 'CompressionType':'GZIP', 
				'CSV': {'FileHeaderInfo': 'None'}},
				OutputSerialization = {'CSV': {
							'RecordDelimiter': '\n',
							'FieldDelimiter': ','}},
				#ScanRange = {'Start':0, 'End':40000 }
			)
		leftover=''
		for event in req_fact['Payload']:
			if 'Records' in event:
				rr=event['Records']['Payload'].decode('utf-8')
				for i, rec in enumerate(rr.split('\n')):
					if rec:
						if leftover:
							rec = leftover+rec
							leftover = ''
						#pp(rec)
						row=rec.split(colsep)
						if row:
							#print(999,row)
							if len(row) == FACT_COLCNT:
								#key=row[fact_key_col_pos]
								#assert key not in fact
								#fact[key] = row
								#print(key)
								await queue.put(row)
							else:
								leftover = rec
		await queue.put(None)
		#print(1234, s3_prefix, s3_fn)
			

	async def update_fact_from_dim(queue, dim_to_fact_cols, dim, updated_q):
		
		readers=[]
		while True: 
			fact = await queue.get()
			queue.task_done()
			if fact:
				key= fact[fact_key_col_pos]
				
				for did, fid in dim_to_fact_cols.items():
					assert fid!=fact_key_col_pos
					fact[fid] = dim[key][did]

				await updated_q.put(fact)
			else:
				break
		await updated_q.put(None)
	
	async def compress_and_upload_to_s3(outq ,bid,cid, blob_s3_key,		bucket):
		if 0:
			row = await outq.get()
			outq.task_done()
			print (7777777777777,blob_s3_key)
			return

		csize=1014*500
		content_type='text/plain'
		stream = io.BytesIO()
		lid=1
		part_info = {'Parts':[]}
		next_chunk=''
		async def get_chunk(outq):
			out = b''
			while outq.qsize():
				payload = await outq.get()
				outq.task_done()
				if payload:
					row = payload
					if not row: break
					out = out + b','.join([x.encode() for x in row])+ b'\n'
					if len(out) >=csize:
						break
				else:
					break
			print('get_chunk',  len(out), outq.qsize())
			return out
		async with aioboto3.client("s3") as s3:
			mpu = await s3.create_multipart_upload(Bucket=bucket, Key=blob_s3_key)
			chunk = await get_chunk(outq)
			while chunk:
				
				next_chunk = await get_chunk(outq)
				if len(next_chunk)<csize: 
					print('merging',len(chunk), len(next_chunk) )
					chunk = chunk+next_chunk;
					next_chunk=''		
				with gzip.GzipFile(fileobj=stream, mode='wb') as gz:
					
					gz.write(chunk)
				
				stream.seek(0)
				
				part = await s3.upload_part(Bucket=bucket, Key=blob_s3_key, PartNumber=lid,\
								UploadId=mpu['UploadId'], Body=stream)
				stream.seek(0)
				stream.truncate()							
					
				part_info['Parts'].append(
						{
							'PartNumber': lid,
							'ETag': part['ETag']
						} )
				lid +=1
				chunk = next_chunk
				#if not chunk: break
				
			try:
				await s3.complete_multipart_upload(Bucket=bucket, Key=blob_s3_key, UploadId=mpu['UploadId'],
									MultipartUpload=part_info)	
			except:
				raise Exception(bid)
				pass	
	async def update_and_upload(queue, dim):
		
		readers=[]
		qz=[]
		while True: 
			row = await queue.get()
			queue.task_done()
			if row:
				(s3_prefix, s3_fn, dim_key, part_name) = row
				print(88888,row)
				if 1:
					fact_queue = asyncio.Queue()
					updated_q = asyncio.Queue()
					readers.append(asyncio.create_task(read_fact_file(fact_queue, s3_prefix, s3_fn)))
					if 1:
						
						readers.append(asyncio.create_task(update_fact_from_dim(fact_queue, dim_to_fact_cols, dim,updated_q)))
					if 1:
						blob_s3_key= '%s/%s/%s/%s.%s.%s.updated_fact_from_dim.csv.gz' % ('tables',s3_updated_fact_tn, part_name, s3_updated_fact_tn,part_name,dim_key)
						#print(blob_s3_key)
						if 1:
							readers.append(asyncio.create_task(compress_and_upload_to_s3(updated_q,\
							part_name, dim_key, blob_s3_key, bucket_name\
							)))
					
					
					qz.append(fact_queue)
					qz.append(updated_q)
			else:
				break
		#await queue.join()
		await asyncio.gather(*readers)
		for q in qz:
			await q.join()
				
	async def update_fact_from_dim_on_s3():
		queue = asyncio.Queue()
		coro= {}
		
		await asyncio.gather( get_files_to_update(objs, queue,dim), update_and_upload(queue, dim))	
		await queue.join()

	if 1:
		asyncio.run(update_fact_from_dim_on_s3())
		elapsed = time.perf_counter() - s
		print(f"{__file__} executed in {elapsed:0.2f} seconds.")

	e()

elapsed = time.perf_counter() - s
print(f"{__file__} executed in {elapsed:0.2f} seconds.")
