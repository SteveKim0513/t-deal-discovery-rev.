"""
T-DEAL AIRFLOW 배치
"""
import boto3
import json
import requests
from config.config import TargetConfig


def run(logger):
    logger.info('======== Start tmsdata2 =========')
    global nameOfFile
    try:
        # aws configure 설정 필수
        client = boto3.client('s3')
        resource = boto3.resource('s3')
        bucket = 'tdeal-dashboard-bucket'
        prefix = 'tmsdata2/'
        downloadFile = 'data/download/tmsdata2.csv'
        filterFile = 'tmsdata2.csv'
        datasourceName = 'finaltmsdata2'
        intervalValue = '1900-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z'

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

        ingestionSpec = {
            "type": "index",
            "spec": {
                "dataSchema": {
                    "dataSource": datasourceName,
                    "parser": {
                        "type": "csv.stream",
                        "timestampSpec": {
                            "column": "발송일자",
                            "format": "yy-MM-dd",
                            "replaceWrongColumn": "true",
                            "timeZone": "UTC",
                            "locale": "en"
                        },
                        "dimensionsSpec": {
                            "dimensions": ["shop", "발송형태", "고유번호", "campaign_id", "시간", "base_date", "decrypt_procode"],
                            "dimensionExclusions": [],
                            "spatialDimensions": []
                        },
                        "columns": ["shop", "발송일자", "발송량", "발송형태", "발송단가", "광고비용", "발송결과실패", "발송결과성공", "수신결과완료", "고유번호",
                                    "campaign_id", "시간", "base_date", "decrypt_procode"],
                        "delimiter": ",",
                        "recordSeparator": "\n",
                        "skipHeaderRecord": True,
                        "charset": "UTF-8"
                    },
                    "metricsSpec": [{
                        "type": "count",
                        "name": "count"
                    }, {
                        "type": "sum",
                        "name": "발송량",
                        "fieldName": "발송량",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "발송단가",
                        "fieldName": "발송단가",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "광고비용",
                        "fieldName": "광고비용",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "발송결과실패",
                        "fieldName": "발송결과실패",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "발송결과성공",
                        "fieldName": "발송결과성공",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "수신결과완료",
                        "fieldName": "수신결과완료",
                        "inputType": "double"
                    }],
                    "enforceType": True,
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "MONTH",
                        "queryGranularity": "DAY",
                        "rollup": False,
                        "append": False,
                        "intervals": [intervalValue]
                    }
                },
                "ioConfig": {
                    "type": "index",
                    "firehose": {
                        "type": "local",
                        "baseDir": "/home/druid/app/druid-ingestion/data/download/",
                        "filter": filterFile
                    }
                },
                "tuningConfig": {
                    "type": "index",
                    "targetPartitionSize": 5000000,
                    "indexSpec": {
                        "bitmap": {
                            "type": "roaring"
                        },
                        "dimensionSketches": {
                            "type": "none"
                        },
                        "allowNullForNumbers": False
                    },
                    "buildV9Directly": True,
                    "ignoreInvalidRows": False,
                    "maxRowsInMemory": 75000,
                    "maxOccupationInMemory": -1
                }
            },
            "dataSource": datasourceName,
            "interval": intervalValue
        }
        URL = deleteUrl + '/druid/coordinator/v1/datasources/' + datasourceName
        headers = {'charset' : 'utf-8'}
        response = requests.delete(URL, headers = headers)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('====== DELETE tmsdata2 =====')

        URL = ingestionUrl + 'druid/indexer/v1/task'
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        jsonString = json.dumps(ingestionSpec)
        logger.debug('=== Ingestion Spec ===')
        logger.debug(jsonString)
        logger.debug('=== Ingestion Spec ===')
        response = requests.post(URL, headers=headers, data=jsonString)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('Response Data : ' + str(response.json()))
        logger.info('====== Finish - tmsdata2 =====')
    except:
        logger.exception("Got exception on tmsdata2")
