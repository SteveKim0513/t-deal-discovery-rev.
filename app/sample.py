"""
T-DEAL AIRFLOW 배치
"""
import socket
from datetime import datetime
import argparse
from util.util import CreateLogger
from config.config import TargetConfig
import os
import boto3
import requests, json

def run(logger):
    logger.info('======== Start sample =========')
    try:
        # aws configure 설정 필수
        client = boto3.client('s3')
        resource = boto3.resource('s3')
        bucket = 'sample'
        prefix = 'sample/'
        downloadFile = 'data/download/sample.csv'

        currentGMTtime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%Z")
        # JSON 값 예제 -> "missingValue": str(currentGMTtime),
        # JSON 값 예제 -> "invalidValue": str(currentGMTtime),

        ingestionSpec = {}

        # Find Name Of File
        paginator = client.get_paginator('list_objects')
        data = paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=prefix)
        for item in data.search('Contents'):
            nameOfFile = item.get('Key')
        # DRUID END POINT
        ingestionUrl = TargetConfig.DRUID_INGESTION_URL
        deleteUrl = TargetConfig.DRUID_DELETE_URL
        # Download File
        resource.Bucket(bucket).download_file(nameOfFile, downloadFile)

        ingestionSpec = {}

        URL = deleteUrl + '/druid/coordinator/v1/datasources/' + datasourceName
        headers = {'charset' : 'utf-8'}
        response = requests.delete(URL, headers = headers)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('====== DELETE sample =====')

        URL = ingestionUrl + 'druid/indexer/v1/task'
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        jsonString = json.dumps(ingestionSpec)
        logger.debug('=== Ingestion Spec ===')
        logger.debug(jsonString)
        logger.debug('=== Ingestion Spec ===')
        response = requests.post(URL, headers = headers, data = jsonString)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('Response Data : ' + str(response.json()))
        logger.info('====== Finish - sample =====')
    except:
        logger.exception("Got exception on sample")
