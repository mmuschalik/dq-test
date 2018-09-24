import pandas as pd
import dfunc as df
import numpy as np
import yaml
from yaml import load, dump
from sqlalchemy import create_engine, event
import datetime
import csv
import time
from urllib.parse import quote_plus

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class dq:

    yml = None
    dset = None
    dset_name = ''
    data = pd.DataFrame()
    execution_id = "RUN003"
    execution_date = datetime.datetime.now()

    def loadYmlFile(self, path):
        stream = open(path, 'r')
        yml = load(stream)
        self.loadYml(yml)

    def loadYml(self, yml):
        self.yml = yml if self.yml is None else df.merge(yml, self.yml)
    
    def datasource(self, name):
        return self.yml['datasource'][name]

    def dataset(self, name):
        return self.yml['dataset'][name]

    def rule(self, name):
        return self.yml['analysis'][name]

    def extractDataset(self, dataset_name):
        self.dset = self.dataset(dataset_name)
        self.dset_name = dataset_name
        dsource = self.datasource(self.dset['datasource'])

        if dsource['type'] == 'sql':
            engine = create_engine(dsource['url'])
            self.data = pd.read_sql_query(self.dset['query'], con=engine)
        else:
            self.data = pd.read_csv(dsource['url'], index_col=False, parse_dates=[0])
        return self.data

    def showFails(self, rule_name):
        ret = self.executeRule(rule_name)
        agg = pd.concat([ret,self.data], axis=1, sort=False)[ret.columns.values.tolist() + self.data.columns.values.tolist()]
        return agg[~agg.value.isin([1,np.nan])]
    
    def executeRule(self, rule_name, add_keywords=False, verbose=True):
        rule = self.rule(rule_name)

        print(bcolors.BOLD + bcolors.UNDERLINE + bcolors.HEADER + "Executing Rule: " + rule_name + bcolors.ENDC)
        
        if 'filter' in rule:
            input = df.filter(self.data, rule['filter'])
        else:
            input = self.data

        datadict = {}
        for krule, vrule in rule.items():
            for kset in self.yml['dataset'].keys():
                if(vrule == kset and krule != 'dataset'):
                    ndq = dq()
                    ndq.loadYml(self.yml)
                    print('extracting ' + kset)
                    ndq.extractDataset(kset)
                    datadict[krule] = ndq.data

        datadict['data'] = input
        res = df.getRuleWithDataFrames(rule, datadict, self.yml, rule_name)
        
        # add source_id
        update_to_list = lambda a: a if isinstance(a, list) else [a]
        res['source_id'] = eval("+ ', ' +".join(map(lambda c: "input['" + c + "'].map(str)", update_to_list(self.dset['id'])))).replace('\t',' ')

        # add keywords
        if add_keywords:
            keywords = {
                'dataset_name': rule['dataset'],
                'analysis_name': rule_name,
                'analysis_description': rule['description']
            }
            if('keywords' in self.datasource(self.dset['datasource'])):
                keywords.update(self.datasource(self.dset['datasource'])['keywords'])
            if('keywords' in self.dset):
                keywords.update(self.dset['keywords'])
            if('keywords' in rule):
                keywords.update(rule['keywords'])

            for key, value in keywords.items():
                res[key.replace(' ', '_')] = value

        print(bcolors.BOLD + bcolors.HEADER + "Result:")
        print(bcolors.WARNING + res['value'].replace({1:'Pass',0:'Fail',np.nan:'Excluded'}).value_counts(dropna=False).to_string())
        print(bcolors.ENDC)

        if verbose:
            print("Top Failures:")
            print(res[~res.value.isin([1,np.nan])].selector.value_counts(dropna=False).head(20).to_string())

        return res

    def commit(self, rule_name):
        s = time.time()
        result = self.executeRule(rule_name, True)
        print('\nQueried in  ' + str((time.time() - s)/60) + ' minutes')
        result['execution_id'] = self.execution_id
        result['execution_date'] = self.execution_date
        
        engine = create_engine(self.yml['datasource']['Result']['url'])
        
        @event.listens_for(engine, 'before_cursor_execute')
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True
                
        s = time.time()
        #result.to_sql('row_analysis_history', con=engine, if_exists = 'append', chunksize = None, index=False)
        result.to_sql('row_analysis_history', con=engine, if_exists = 'append', chunksize=50, index=False)
        print('Uploaded in ' + str((time.time() - s)/60) + ' minutes')

        
    def executeAll(self, yml):
        for x in yml['analysis']:
            ret = self.executeRule(x)
            print('\n')
            
    def clearResultsForSystem(self, systemname):
        engine = create_engine(self.yml['datasource']['Result']['url'])
        engine.execute("delete from row_analysis_history where system='" + systemname + "'")
            
    def commitAll(self, yml):
        for x in yml['analysis']:
            self.commit(x)
            print('\n')

    def exportRules(self):
        exportData = self.data.copy()
        writer = pd.ExcelWriter(self.dset_name + '_export.xlsx')
        tabs = {}
        summary = pd.DataFrame(columns=['Rule Name', 'Rule Description', 'Failed Records', 'Total Records', 'Percent'])
        row = 0

        for rkey in self.yml['analysis'].keys():
            if(self.yml['analysis'][rkey]['dataset'] == self.dset_name):
                try:
                    res = self.executeRule(rkey, False)
                    exportData[rkey] = res['value']
                    res['pass/fail'] = res['value'].apply(lambda r: 'fail' if r == 0  else 'pass')
                    failed_records = len(res[res['value'] == 0])
                    tota_records = len(res)
                    tab = res[['selector','pass/fail']].groupby(['selector','pass/fail']).size().reset_index(name='counts').sort_values(['pass/fail','counts'],ascending=[True,False]).fillna('')
                    tabs[rkey] = tab
                    summary.loc[row] = [rkey,self.rule(rkey)['description'], failed_records, tota_records, 0 if tota_records == 0 else failed_records / tota_records * 100]
                    row = row + 1
                except:
                    pass

        summary.to_excel(writer, 'Summary', index=False)
        # exportData.to_excel(writer,'All (Detail)', index=False)

        for n,t in tabs.items():
            print(n)
            if(len(t)>0):
                t.to_excel(writer, n[0:30], index=False)

        writer.save()
        return exportData

    def saveAs(self, file_name):
        with open(file_name, 'w') as yaml_file:
            yaml.dump(self.yml, yaml_file, default_flow_style=False)