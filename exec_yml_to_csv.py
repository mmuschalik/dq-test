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


engine = create_engine(yml['datasource']['Result']['url'])
conn = engine.connect()
conn.execute("insert into dbo.run (description) values (null)")
runid = conn.execute("select max(id) as runid from dbo.run").fetchone()['runid']

for dkey, dvalue in yml['dataset'].items():

    try:

        logger.info('Extracting dataset: ' + dkey + '...')
        analysis.extractDataset(dkey)
        logger.info('Finshed extracting dataset: ' + dkey)

        for rkey, rvalue in yml['analysis'].items():
            if(yml['analysis'][rkey]['dataset'] == dkey):

                try:

                    logger.info('Executing Rule: ' + rkey + '...')
                    res = analysis.executeRule(rkey, True, verbose=False)
                    logger.info('Finished executing Rule: ' + rkey)

                    res['execution_id'] = "RUN" + str(runid)
                    res['execution_date'] = datetime.datetime.now().strftime('%Y-%m-%d')

                    logger.info('Saving results for rule: ' + rkey + '...')
                    res.to_csv('data/' + dkey + '_' + rkey + '.csv', sep='\t', index=False, escapechar='\\',  quoting=csv.QUOTE_NONE)
                    logger.info('Results saved for rule: ' + rkey)
    
                except:
                    traceback.print_exc()
                    pass
    except:
        traceback.print_exc()
        pass