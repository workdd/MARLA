'''
Python reducer function

* Copyright 2016, Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Amazon Software License (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* http://aws.amazon.com/asl/
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License. 

'''

import boto3
import json
import random
import resource
import time

# S3 session 생성
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

# Mapper의 결과가 저장된 S3 Bucket
TASK_MAPPER_PREFIX = "task/mapper/"
# Reducer의 결과를 저장할 S3 Bucket
TASK_REDUCER_PREFIX = "task/reducer/"


# 주어진 bucket 위치 경로에 파일 이름이 key인 object와 data를 저장합니다.
def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)


def lambda_handler(event, context):
    start_time = time.time()

    job_bucket = event['jobBucket']
    job_id = event['jobId']
    r_id = event['reducerId']

    reduce_files = 'bl-release'

    results = {}
    line_count = 0

    # 입력 CSV => 츌력 JSON 포멧

    # 모든 key를 다운로드하고 Reduce를 처리합니다.
    # Reducer는 Mapper의 output 개수에 따라 1/2씩 처리가 되며 Reducer의 step 개수가 결정됩니다.
    # Mapper의 output 개수가 64개라면 (step:output개수/1:32/2:16/3:12.8/4:4/5:2/6:1) 총 6단계 reduce 발생

    files = s3_client.list_objects(Bucket=job_bucket, Prefix=reduce_files)['Contents']
    for mf in files:
        if "task/mapper/" + str(r_id) in mf["Key"]:
            key = mf["Key"]
            print('key: ', key)
            response = s3_client.get_object(Bucket=job_bucket, Key=key)
            contents = response['Body'].read()
            print('contents: ', contents)
            print('str(contents)', str(contents))
            try:
                for srcIp, val in json.loads(contents).items():
                    line_count += 1
                    if srcIp not in results:
                        results[srcIp] = 0
                    results[srcIp] += float(val)
            except Exception as e:
                print(e)

    time_in_secs = (time.time() - start_time)
    pret = [len(files), line_count, time_in_secs]
    print("Reducer output", pret)

    metadata = {
        "linecount": '%s' % line_count,
        "processingtime": '%s' % time_in_secs,
        "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    }
    fname = "%s/%s%s" % (job_id, TASK_REDUCER_PREFIX, r_id)
    write_to_s3(job_bucket, fname, json.dumps(results), metadata)
    return pret
