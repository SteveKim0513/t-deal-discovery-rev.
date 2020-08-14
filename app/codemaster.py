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
    logger.info('======== Start codemaster =========')
    try:
        # DRUID END POINT
        ingestionUrl = TargetConfig.DRUID_INGESTION_URL
        deleteUrl = TargetConfig.DRUID_DELETE_URL
        # aws configure 설정 필수
        s3 = boto3.resource('s3')
        s3.Bucket('glue-jobs-bucket').download_file('incross/codemaster/codemaster.csv', 'data/download/codemaster.csv')
        # currentGMTtime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%Z")
        # "missingValue" : str(currentGMTtime),
        # "invalidValue" : str(currentGMTtime),
        ingestionSpec = {
            "type" : "index",
            "spec" : {
                "dataSchema" : {
                    "dataSource" : "codemaster_v1",
                    "parser" : {
                        "type" : "csv.stream",
                        "timestampSpec" : {
                            "column" : "current_datetime",
                            "missingValue" : "1970-01-01T00:00:00.000Z",
                            "invalidValue" : "1970-01-01T00:00:00.000Z",
                            "replaceWrongColumn" : "true"
                        },
                        "dimensionsSpec" : {
                            "dimensions" : [ "ï»¿unique_num", "campain_code", "customer_code", "customer_name", "brand_code", "brand_name", "pro_code", "pro_name", "encrypt_procode", "decrypt_procode", "url" ],
                            "dimensionExclusions" : [ ],
                            "spatialDimensions" : [ ]
                        },
                        "columns" : [ "ï»¿unique_num", "campain_code", "customer_code", "customer_name", "brand_code", "brand_name", "pro_code", "pro_name", "encrypt_procode", "decrypt_procode", "url" ],
                        "delimiter" : ",",
                        "recordSeparator" : "\n",
                        "skipHeaderRecord" : "true",
                        "charset" : "UTF-8"
                    },
                    "metricsSpec" : [ {
                        "type" : "count",
                        "name" : "count"
                    } ],
                    "enforceType" : "true",
                    "granularitySpec" : {
                        "type" : "uniform",
                        "segmentGranularity" : "DAY",
                        "queryGranularity" : "MINUTE",
                        "rollup" : "false",
                        "append" : "false",
                        "intervals" : [ "1900-01-01T00:00:00.000Z/2100-12-31T00:00:00.000Z" ]
                    }
                },
                "ioConfig" : {
                    "type" : "index",
                    "firehose" : {
                        "type" : "local",
                        "baseDir" : "/home/druid/app/druid-ingestion/data/download/",
                        "filter" : "codemaster.csv"
                    }
                },
                "tuningConfig" : {
                    "type" : "index",
                    "targetPartitionSize" : 5000000,
                    "indexSpec" : {
                        "bitmap" : {
                            "type" : "roaring"
                        },
                        "dimensionSketches" : {
                            "type" : "none"
                        },
                        "allowNullForNumbers" : "false"
                    },
                    "buildV9Directly" : "true",
                    "ignoreInvalidRows" : "false",
                    "maxRowsInMemory" : 75000,
                    "maxOccupationInMemory" : -1
                }
            }
        }

        URL = deleteUrl + '/druid/coordinator/v1/datasources/' + "name of datasource"
        headers = {'charset' : 'utf-8'}
        response = requests.delete(URL, headers = headers)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('====== DELETE codemaster =====')

        URL = ingestionUrl + 'druid/indexer/v1/task'
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        jsonString = json.dumps(ingestionSpec)
        logger.debug('=== Ingestion Spec ===')
        logger.debug(jsonString)
        logger.debug('=== Ingestion Spec ===')
        response = requests.post(URL, headers = headers, data = jsonString)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('Response Data : ' + str(response.json()))
        logger.info('====== Finish - codemaster =====')
    except:
        logger.exception("Got exception on codemaster")
