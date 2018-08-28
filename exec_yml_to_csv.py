import dq2 as dq
import csv
import sys
import os
import datetime

path = sys.argv[1]

analysis = dq.dq()
yml = analysis.loadYmlFile(path)
prefix = os.path.basename(path).replace('.yml','')

for dkey, dvalue in yml['dataset'].items():
    analysis.extractDataset(dkey)
    for rkey, rvalue in yml['analysis'].items():
        if(yml['analysis'][rkey]['dataset'] == dkey):
            res = analysis.executeRule(rkey, True)
            res['execution_id'] = "RUN004"
            res['execution_date'] = datetime.datetime.now().strftime('%Y-%m-%d')
            res.to_csv('data/' + prefix + '_' + rkey + '.csv', sep='\t', index=False,  quoting=csv.QUOTE_NONE)