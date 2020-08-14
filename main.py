"""
T-DEAL AIRFLOW 배치
"""
from datetime import datetime
import argparse
from util.util import CreateLogger
from app import codemaster, genieworks, landingtype, campaigndata, tmsdata, tmsdata2, tmsdata3



def main(args, logger):
    logger.info('======== T-DEAL =========')
    for target in args.target:
        logger.info('====== CSV File : ' + target)
        if target == 'CODEMASTER':
            try:
                codemaster.run(logger)
            except:
                logger.exception("Got exception on main - CODEMASTER")
        elif target == 'GENIEWORKS':
            try:
                genieworks.run(logger)
            except:
                logger.exception("Got exception on main - GENIEWORKS")
        elif target == 'LANDINGTYPE':
            try:
                landingtype.run(logger)
            except:
                logger.exception("Got exception on main - LANDINGTYPE")
        elif target == 'CAMPAIGNDATA':
            try:
                campaigndata.run(logger)
            except:
                logger.exception("Got exception on main - CAMPAIGNDATA")
        elif target == 'TMSDATA':
            try:
                tmsdata.run(logger)
            except:
                logger.exception("Got exception on main - TMSDATA")
        elif target == 'TMSDATA2':
            try:
                tmsdata2.run(logger)
            except:
                logger.exception("Got exception on main - TMSDATA2")
        elif target == 'TMSDATA3':
            try:
                tmsdata3.run(logger)
            except:
                logger.exception("Got exception on main - TMSDATA3")



if __name__ == '__main__':
    start_time = datetime.now().strftime('%Y%m%d_%H%M')
    logger = CreateLogger('batch', "logs/{socket.gethostname()}_log.log")
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', nargs='+', help='batch')
    args = parser.parse_args()
    try:
        main(args, logger)
    except:
        logger.exception("Got exception on main")
        raise




