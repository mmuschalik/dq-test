import pandas as pd
import dfunc as df
import numpy as np
import yaml
from yaml import load, dump
from sqlalchemy import create_engine
import datetime
import csv

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
        res = self.yml['analysis'][name]

        if 'function' in res and isinstance(res['function'],str):
            res['function'] = self.yml['function'][res['function']]
        return res

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

        print(bcolors.BOLD + bcolors.WARNING + "Test Results:")
        print(res['value'].replace({1:'Pass',0:'Fail',np.nan:'Excluded'}).value_counts(dropna=False))

        if verbose:
            print(bcolors.ENDC + "\nTop Failures:")
            print(res[~res.value.isin([1,np.nan])].selector.value_counts(dropna=False).head(20))

        return res

    def commit(self, rule_name):
        result = self.executeRule(rule_name, True, verbose=False)
        result['execution_id'] = self.execution_id
        #result['execution_date'] = self.execution_date
        engine = create_engine(self.yml['datasource']['Result']['url'])
        engine.execute("delete from row_analysis_history where analysis_name='" + rule_name + "' and execution_id='" + self.execution_id + "'")
        #result.to_sql('row_analysis_history', con=engine, if_exists = 'append', chunksize=50, index=False)
        result.to_csv('result.csv', sep='\t', index=False,  quoting=csv.QUOTE_NONE)

    def exportRules(self):
        exportData = self.data.copy()
        writer = pd.ExcelWriter(self.dset_name + '_export.xls')
        tabs = {}
        summary = pd.DataFrame(columns=['Rule Name', 'Rule Description', 'Failed Records'])
        row = 0

        for rkey in self.yml['analysis'].keys():
            if(self.yml['analysis'][rkey]['dataset'] == self.dset_name):
                try:
                    res = self.executeRule(rkey, False)
                    exportData[rkey] = res['value']
                    tab_fail = res[~res.value.isin([1,np.nan])][['selector']]
                    tab = pd.concat([tab_fail, self.data], axis=1, join='inner').fillna('')
                    tabs[rkey] = tab
                    summary.loc[row] = [rkey,self.rule(rkey)['description'],len(tab)]
                    row = row + 1
                except:
                    pass

        summary.to_excel(writer, 'Summary', index=False)
        exportData.to_excel(writer,'All (Detail)', index=False)

        for n,t in tabs.items():
            print(n)
            if(len(t)>0):
                t.to_excel(writer, n[0:30], index=False)

        writer.save()
        return exportData

    def saveAs(self, file_name):
        with open(file_name, 'w') as yaml_file:
            yaml.dump(self.yml, yaml_file, default_flow_style=False)