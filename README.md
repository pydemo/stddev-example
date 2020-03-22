# stddev-example
Standard deviation calculation example for impressions fact and campaigns slowly changing  dimension using Python

### Prereqs:
1.	We will need valid job queue and compute environment for AWS Batch.
2.	Also we need working Docker environment to complete this task (Amazon Linux +Docker).
3.	AWS CLI
### Build steps:
1.	Build Docker images for partition, join, and update processes.
2.	Create an Amazon ECR or Docker Hub repo for the images.
3.	Push the built image to ECR or DH.
4.	Create job script and upload it to S3.
5.	Create IAM role to be used by jobs to access S3.
6.	Create a job definition that uses the built images.
7.	Submit and run a job that executes the job script from S3.

## Workflow

1.	Partition large impressions file by date (list partition) and campaign id (hash partition). Upload results to S3 as FACT_PARTITONED.
2.	Join FACT_PARTITONED file (from step 1) with smaller campaign dimension file. Upload results to S3 as FACT_JOINED.
3.	Update FACT_JOINED file (from step 2) with data from changed campaign dimension. Upload results to S3 as FACT_UPDATED.

### Run_job.sh
```shell
#!/bin/bash
date
echo "Args: $@"
env
echo "Partition Fact."
echo "jobId: $AWS_BATCH_JOB_ID"
echo "jobQueue: $AWS_BATCH_JQ_NAME"
echo "computeEnvironment: $AWS_BATCH_CE_NAME"

python3 partition.py bucket_name/fact_file.gz tables/FACT_PARTITIONED

python3 join.py bucket_name/campaign_meta_file.gz tables/ FACT_PARTITIONED tables/FACT_JOINED

python3 update.py bucket_name/updated_campaign_meta_file.gz tables/ CT_JOINED tables/FACT_UPDATED

date
echo "Done.
```

### Python scripts:
      partition.py – partitions impressions files by date (list) and campaign_id (hash) 
      join.py – joins impressions partitioned file with campaign metadata file by date and campaign_id.
      update.py – updates impressions partitioned files with data from updated metadata file by date and campaign_id.
      count.py - total line count across all files in S3 bucket. 

## Metrics.
### You can fetch row counts from:
1.	CloudWatch logs (Write logs from your Python scripts into CloudWatch log group. Then use CW Logs Insights query language to create dashboard)
2.	S3 file metadata tags (assumed you updated metadata with row counts from your python script)
3.	S3 bucket tags (assumed you created those tags with row counts from your python script)
4.	Brute–force recount all rows in a bucket FACT_UPDATED using Python.
### Job status:
1.	CloudWatch logs (Write job leg status from your Python scripts into CloudWatch log group. Then use CW Logs Insights query language to create dashboard)

### Standard deviation of the column “viewing_percentage” over the past 200 days.
1.	FACT_UPDATED
```
   [‘01/01/2019’, '50000', 'campaign_00', 'video_0', '0.5']
   [‘02/02/2020’, '50018', 'campaign_08', 'video_0', '0.33']
   [‘03/03/2020’, '50020', 'campaign_00', 'video_0', '0.25']
   [‘04/04/2019’, '50030', 'campaign_00', 'video_0', '0.2']
```   
Last column is viewing_percentage = VP

### Calculate standard deviation (using S3 Select)
   Mean (run S3 Select query on FACT_UPDATED to calculate sum of all values in VP, then calculate number of all rows. 
   `Mean = sum_of_val_VP/total_row_count`
```SQL
SELECT sum(S._4) VP_sum FROM s3object S where date> ‘06/01/2019’
SELECT count(*) VP_cnt FROM s3object S where date> ‘06/01/2019’
```
`VP_mean = VP_sum/VP_cnt`

   Now calculate std deviation.. (Run S3 Select query on FACT_UPDATED to do it)
```SQL
SELECT sum((S._4- VP_mean)* (S._4- VP_mean)) VP_mean_sum FROM s3object S where date> ‘06/01/2019’
```
`VP_std_dev = sqrt(VP_mean_sum/VP_count)`



