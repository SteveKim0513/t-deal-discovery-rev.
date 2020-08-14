"""
T-DEAL AIRFLOW 배치
"""
import boto3
import json
import requests
from config.config import TargetConfig


def run(logger):
    logger.info('======== Start genieworks =========')
    global nameOfFile
    try:
        # aws configure 설정 필수
        client = boto3.client('s3')
        resource = boto3.resource('s3')
        bucket = 'tdeal-dashboard-bucket'
        prefix = 'genieworks/'
        downloadFile = 'data/download/genieworks.csv'
        filterFile = 'genieworks.csv'
        datasourceName = 'finalgenieworks'
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
                            "column": "주문일시",
                            "format": "yyyy-MM-dd HH:mm:ss",
                            "replaceWrongColumn": True,
                            "timeZone": "UTC",
                            "locale": "en"
                        },
                        "dimensionsSpec": {
                            "dimensions": ["base_date", "시간", "상품주문번호", "주문상태", "배송상태", "SHOP", "상품번호", "상품명", "옵션",
                                           "주문번호", "주문순번", "배송번호", "수취인", "연락처", "배송지", "우편번호", "택배사", "택배코드", "송장번호",
                                           "발송처리일", "구매자ID", "구매자명", "구매자 연락처", "취소일시", "결제타입", "결제방법", "구매확정일시",
                                           "unique_num", "customer_code", "customer_name", "brand_code", "brand_name",
                                           "decrypt_procode"],
                            "dimensionExclusions": [],
                            "spatialDimensions": []
                        },
                        "columns": ["base_date", "시간", "상품주문번호", "주문상태", "배송상태", "주문일시", "SHOP", "상품번호", "상품명", "옵션",
                                    "수량", "판매가", "할인가", "공급가", "옵션가", "옵션 공급가", "주문금액", "결제금액", "취소금액", "기본배송비",
                                    "지역별배송비", "주문번호", "주문순번", "배송번호", "수취인", "연락처", "배송지", "우편번호", "택배사", "택배코드",
                                    "송장번호", "발송처리일", "구매자ID", "구매자명", "구매자 연락처", "취소일시", "결제타입", "결제방법", "구매확정일시",
                                    "unique_num", "customer_code", "customer_name", "brand_code", "brand_name",
                                    "decrypt_procode"],
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
                        "name": "수량",
                        "fieldName": "수량",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "판매가",
                        "fieldName": "판매가",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "할인가",
                        "fieldName": "할인가",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "공급가",
                        "fieldName": "공급가",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "옵션가",
                        "fieldName": "옵션가",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "옵션 공급가",
                        "fieldName": "옵션 공급가",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "주문금액",
                        "fieldName": "주문금액",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "결제금액",
                        "fieldName": "결제금액",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "취소금액",
                        "fieldName": "취소금액",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "기본배송비",
                        "fieldName": "기본배송비",
                        "inputType": "double"
                    }, {
                        "type": "sum",
                        "name": "지역별배송비",
                        "fieldName": "지역별배송비",
                        "inputType": "double"
                    }],
                    "enforceType": True,
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "MONTH",
                        "queryGranularity": "HOUR",
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
        logger.info('====== DELETE genieworks =====')

        URL = ingestionUrl + 'druid/indexer/v1/task'
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        jsonString = json.dumps(ingestionSpec)
        logger.debug('=== Ingestion Spec ===')
        logger.debug(jsonString)
        logger.debug('=== Ingestion Spec ===')
        response = requests.post(URL, headers = headers, data = jsonString)
        logger.info('Status Code : ' + str(response.status_code))
        logger.info('Response Data : ' + str(response.json()))
        logger.info('====== Finish - genieworks =====')
    except:
        logger.exception("Got exception on genieworks")
