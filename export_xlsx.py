import dq2 as dq
import csv
import sys, traceback
import os
import datetime
import logging
import logging.config
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

paths = sys.argv[1:]

analysis = dq.dq()

for path in paths:
    analysis.loadYmlFile(path)

yml = analysis.yml
prefix = os.path.basename(path).replace('.yml','')

for dkey, dvalue in yml['dataset'].items():

    try:

        logger.info('Extracting dataset: ' + dkey + '...')
        analysis.extractDataset(dkey)
        logger.info('Finshed extracting dataset: ' + dkey)

        analysis.exportRules()
    except:
        traceback.print_exc()
        pass