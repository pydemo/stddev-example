"""
crossix-test/cust_list2.csv:

Transaction_id,Campaign_id, product_name, view_percentage
50000,campaign_0,video_0,0.5
50001,campaign_1,video_0,0.5
50002,campaign_2,video_0,0.5
50003,campaign_3,video_0,0.5
50004,campaign_4,video_0,0.5


"""
import os, sys, csv, gzip
import io, time
import tempfile
import asyncio
import aioboto3

import boto3, shutil
from pprint import pprint as pp
from os.path import isdir, isfile, join



try:
	import cStringIO
except ImportError:
	import io as cStringIO

		
e=sys.exit	
linesep	= '\n'
colsep	= ','	
		
s3			= boto3.client('s3')
bucket_name	= 'crossix-test'
file_name 	= 'campaign_1k.csv.gz'
ufn			= 'unique_cn.csv'

sql_stmt 	= """SELECT * FROM s3object S """  



def cycle(seq):
    i = 0
    while True:
        yield seq[i]
        i = (i + 1) % len(seq)
		
NR_OF_PARTITIONS = 8

cycles={}
buckets={}
def get_pid(cid):
	global cycles
	if cid not in cycles:
		#print(cid)
		cycles[cid]=cycle(['B_%d' % x for x in range(NR_OF_PARTITIONS)])

	yield next(cycles[cid])


req = s3.select_object_content(
    Bucket	= bucket_name,
    Key		= file_name,
    ExpressionType	= 'SQL',
    Expression		= sql_stmt,
    InputSerialization 	= { 'CompressionType':'GZIP', 
	'CSV': {'FileHeaderInfo': 'None'}},
    OutputSerialization = {'CSV': {
				'RecordDelimiter': '\n',
                'FieldDelimiter': ','}},
	#ScanRange = {'Start':0, 'End':40000 }
)

COLCNT = 4		

coro = {}

async def upload_gzipped_queue(coro,bid, blob_s3_key,		bucket):

	outq = coro.get(bid)
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
				out = out + b','.join([x.encode() for x in row])
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
			

async def producer(req, queue):


	leftover=''
	for event in req['Payload']:
		if 'Records' in event:
			rr=event['Records']['Payload'].decode('utf-8')
			for i, rec in enumerate(rr.split(linesep)):
				if rec:
					if leftover:
						rec = leftover+rec
						leftover = ''
					row=rec.split(colsep)
					if row:
						if len(row) == COLCNT:
							
							await queue.put(row)
						else:
							leftover = rec

	await queue.put(None)



async def consumer(queue, coro, bid_queue):
	

	while True: 
		row = await queue.get()
		queue.task_done()
		if row:
			cid=row[1]
			bucket_id=next(get_pid(cid))
			if 0:
				if bucket_id not in buckets:
					buckets[bucket_id] = {}
				if cid not in buckets[bucket_id]:
					buckets[bucket_id][cid] = []
				buckets[bucket_id][cid].append(row)
			if 1: #bucket_id == 'B_4':
				if bucket_id not in coro:
					coro[bucket_id]= outq = asyncio.Queue()
					await bid_queue.put(bucket_id)
				outq = coro[bucket_id]
				await outq.put((bucket_id,cid, row))
		else:
			break
	await outq.put(None)
	await bid_queue.put(None)
	
async def upload_gzipped(coro,bid_queue,	bucket):
	uploaders =[]
	while True: 
		bid = await bid_queue.get()
		print('got bid: ', bid)
		bid_queue.task_done()
		if not bid: break

		if 1:		
			uploaders.append( upload_gzipped_queue(coro,bid, 'test_load/%s.aio_campaign_1.csv.gz' % bid, bucket))
	await asyncio.gather(*uploaders)
	print('---- done uploading')


	await bid_queue.join()

	
async def main():
	queue = asyncio.Queue()
	bid_queue = asyncio.Queue()
	coro= {}
	
	await asyncio.gather( producer(req,queue), consumer(queue, coro,bid_queue),\
		upload_gzipped(coro,bid_queue, bucket_name))	
	await queue.join()



s = time.perf_counter()
if 1:
	asyncio.run(main())
	
	

elapsed = time.perf_counter() - s
print(f"{__file__} executed in {elapsed:0.2f} seconds.")
