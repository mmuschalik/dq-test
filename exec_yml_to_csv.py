import dq2 as dq
import csv
import sys
import os
import datetime
from sqlalchemy import create_engine

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
    print('Extracting dataset: ' + dkey)
    analysis.extractDataset(dkey)
    for rkey, rvalue in yml['analysis'].items():
        if(yml['analysis'][rkey]['dataset'] == dkey):
            res = analysis.executeRule(rkey, True)
            res['execution_id'] = "RUN" + str(runid)
            res['execution_date'] = datetime.datetime.now().strftime('%Y-%m-%d')
            print('Saving results for rule: ' + rkey + '...')
            res.to_csv('data/' + dkey + '_' + rkey + '.csv', sep='\t', index=False,  quoting=csv.QUOTE_NONE)
            print('Results saved for rule: ' + rkey)