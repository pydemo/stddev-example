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

from include.upload import read_fact_from_s3, producer,consumer, upload_gzipped, linesep, colsep, get_pid


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

cdim_fn		= 'cdim_1000.csv'

cdim_s3fn 	= 'cdim_1000.csv.gz'
cfact_s3fn	= 'campaign_100k_c1000.csv.gz'
if 0: #Create dim
	with open(cdim_fn, mode='w') as fh:
		
		csvw = csv.writer(fh, delimiter = ',', quotechar = '"', lineterminator = '\n',  quoting=csv.QUOTE_MINIMAL)
		for i in range(1000):	
			row=['campaign_%04d' % (i), 	'video campaign "%s"' % \
			(''.join(random.choice(string.ascii_lowercase) for i in range(100))), 'metadata 1']
			#print (row)
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
s3_dimtn = 'DIM_CAMPAIGNS'
s3_dimfn = 'cdim_10.csv.gz'	
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



if 1:
	FACT_COLCNT=4
	fact={}
	s3_prefix = 'tables'
	s3_fact_tn = 'FACT_IMPRESSIONS'
	s3_fact_fn = 'B_0.campaign_00.campaign.csv.gz'
	dim_key_col_pos=0
	fact_key_col_pos=1
	part_pos_in_fact_fn = 1
	key_pos_in_fact_fn = 2
	s3r		= boto3.resource('s3')
	mybucket = s3r.Bucket(bucket_name)
	
	bucket_prefix="%s/%s/" % (s3_prefix, s3_fact_tn)
	objs = mybucket.objects.filter(
	Prefix = bucket_prefix)
	dim_cols_to_append = [2]
	if 1:

		s3_joined_fact_tn = 'FACT_IMPRESSIONS_JOINED'
	
	def get_key_from_fn(fn):
		
		return fn.split(".")[key_pos_in_fact_fn]
	def get_part_from_fn(fn):		
		return fn.split(".")[part_pos_in_fact_fn]
	async def producer(objs, queue,dim):

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
			
			
	async def consumer(queue, dim):
		
		readers=[]
		qz=[]
		while True: 
			row = await queue.get()
			queue.task_done()
			if row:
				(s3_prefix, s3_fn, dim_key, part_name) = row
				fact_queue = asyncio.Queue()
				appended_q = asyncio.Queue()
				readers.append(asyncio.create_task(read_fact_file(fact_queue, s3_prefix, s3_fn)))
				
				data_to_append= [x for i, x in enumerate(dim[dim_key]) if i in dim_cols_to_append]
				if 1:
					readers.append(asyncio.create_task(append_dim_cols_to_fact(fact_queue, data_to_append,appended_q)))
				if 1:
					blob_s3_key= '%s/%s/%s/%s.%s.%s.joined_fact_and_dim.csv.gz' % ('tables',s3_joined_fact_tn, part_name, s3_joined_fact_tn,part_name,dim_key)
					#print(blob_s3_key)
					if 1:
						readers.append(asyncio.create_task(compress_and_upload_to_s3(appended_q,\
						part_name, dim_key, blob_s3_key, bucket_name\
						)))
				
				
				qz.append(fact_queue)
				qz.append(appended_q)
			else:
				break
		#await queue.join()
		await asyncio.gather(*readers)
		for q in qz:
			await q.join()

	async def append_dim_cols_to_fact(queue, append, appended_q):
		
		readers=[]
		while True: 
			row = await queue.get()
			queue.task_done()
			if row:
				#print('merge----',row, append)
				await appended_q.put(row +append)
			else:
				break
		await appended_q.put(None)
		
	async def read_fact_file(queue, s3_prefix, s3_fn):
		if 1: #Fact partitioner
			FACT_COLCNT=4
			#fact={}
			s3_prefix = 'tables'
			s3_fact_tn = 'FACT_IMPRESSIONS'
			sql_stmt 	= """SELECT * FROM s3object S LIMIT 2"""  
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
		leftover=''
		for event in req_fact['Payload']:
			if 'Records' in event:
				rr=event['Records']['Payload'].decode('utf-8')
				for i, rec in enumerate(rr.split(os.linesep)):
					if rec:
						if leftover:
							rec = leftover+rec
							leftover = ''
						#pp(rec)
						row=rec.split(colsep)
						if row:
							#print(row)
							if len(row) == FACT_COLCNT:
								key=row[fact_key_col_pos]
								#assert key not in fact
								#fact[key] = row
								#print(key)
								await queue.put(row)
							else:
								leftover = rec
		await queue.put(None)
		#print(1234, s3_prefix, s3_fn)
				
				
	async def get_table_partition_files():
		queue = asyncio.Queue()
		cid_queue = asyncio.Queue()
		coro= {}
		
		await asyncio.gather( producer(objs, queue,dim), consumer(queue, dim))	
		await queue.join()
		#await cid_queue.join()


	
	if 1:
		asyncio.run(get_table_partition_files())
		elapsed = time.perf_counter() - s
		print(f"{__file__} executed in {elapsed:0.2f} seconds.")
		
		
	e()
	
FACT_COLCNT=4
fact={}
s3_prefix = 'tables'
s3_fact_tn = 'FACT_IMPRESSIONS_APPENDED'
s3_fact_fn = 'B_0.campaign_00.campaign.csv.gz'


sql_stmt 	= """SELECT * FROM s3object S"""  


if 0: #Fact partitioner
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


	async def split_fact_to_queues_by_cid(queue, coro, cid_queue):
		

		while True: 
			row = await queue.get()
			queue.task_done()
			if row:
				cid=row[1]
				
				part_id=next(get_pid(cid))
				if 0:
					if part_id not in buckets:
						buckets[part_id] = {}
					if cid not in buckets[part_id]:
						buckets[part_id][cid] = []
					buckets[part_id][cid].append(row)
				if 1: #part_id == 'B_4':
					if part_id not in coro:
						coro[part_id]={}
					if cid not in coro[part_id]:
					
						coro[part_id][cid]= outq = asyncio.Queue()
						await cid_queue.put((part_id, cid))
					outq = coro[part_id][cid]
					pp(row)
					await outq.put((part_id,cid, row))
			else:
				break
		await outq.put(None)
		await cid_queue.put(None)
		
	async def append_upload_gzipped(coro,bid,cid, blob_s3_key,		bucket, append):

		outq = coro.get(bid).get(cid)
		assert outq
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
					bid, cid, row = payload
					if not row: break
					out = out + b','.join([x.encode() for x in (row+append)])+ b'\n'
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
				
	async def append_and_upload_gzipped(coro,cid_queue,bucket, prefix, dim):
		uploaders =[]
		
		while True: 
			payload = await cid_queue.get()
			cid_queue.task_done()
			if payload:
				pid, cid = payload
				print('got pid, cid: ', pid, cid)
				
				if not pid: break
				pp(dim[cid])
				dim_cols_to_append = [2]
				data_to_append= [x for i, x in enumerate(dim[cid]) if i in dim_cols_to_append]
				if 1:		
					uploaders.append(asyncio.create_task(append_upload_gzipped(coro,pid,cid,'%s/%s/%s.%s.%s.campaign.csv.gz' % (prefix, pid, s3_fact_tn,pid, cid), bucket, append=data_to_append)))
			else:
				break
		if uploaders:
			await asyncio.gather(*uploaders)
		print('---- done uploading')
	
	async def append_dim_cols_to_fact():
		queue = asyncio.Queue()
		cid_queue = asyncio.Queue()
		coro= {}

		await asyncio.gather( read_fact_from_s3(req_fact,queue), split_fact_to_queues_by_cid(queue, coro,cid_queue),\
			append_and_upload_gzipped(coro,cid_queue, bucket_name, prefix='%s/%s' % (s3_prefix,s3_fact_tn), dim=dim))	
		await queue.join()
		await cid_queue.join()


	
	if 1:
		asyncio.run(append_dim_cols_to_fact())
	e()
	
if 0: #read fact from s3
	req_fact = s3.select_object_content(
		Bucket	= bucket_name,
		Key		= '%s/%s/%s' % (s3_prefix, s3_fact_tn, s3_fact_fn),
		ExpressionType	= 'SQL',
		Expression		= sql_stmt,
		InputSerialization 	= { 'CompressionType':'GZIP', 
		'CSV': {'FileHeaderInfo': 'None'}},
		OutputSerialization = {'CSV': {
					'RecordDelimiter': '\n',
					'FieldDelimiter': ','}},
		#ScanRange = {'Start':0, 'End':40000 }
	)

	def read_fact(req, fact, colid):
		leftover=''
		for event in req['Payload']:
			if 'Records' in event:
				rr=event['Records']['Payload'].decode('utf-8')
				for i, rec in enumerate(rr.split(os.linesep)):
					if rec:
						if leftover:
							rec = leftover+rec
							leftover = ''
						pp(rec)
						row=rec.split(colsep)
						if row:
							#print(row)
							if len(row) == FACT_COLCNT:
								key=row[colid]
								#assert key not in fact
								fact[key] = row
								print(row)
							else:
								leftover = rec

	read_fact(req_fact, fact,1)
	pp(fact)
	
if 0:
	sql_stmt 	= """SELECT * FROM s3object S"""  
	
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



		
	async def upload_fact():
		queue = asyncio.Queue()
		cid_queue = asyncio.Queue()
		coro= {}
		
		await asyncio.gather( producer(req_fact,queue), consumer(queue, coro,cid_queue),\
			upload_gzipped(coro,cid_queue, bucket_name))	
		await queue.join()
		await cid_queue.join()

		
elapsed = time.perf_counter() - s
print(f"{__file__} executed in {elapsed:0.2f} seconds.")
