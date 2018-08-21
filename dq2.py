import pandas as pd
import dfunc as df
import numpy as np
import yaml
from yaml import load, dump
from sqlalchemy import create_engine
import datetime

class dq:

    yml = None
    dset = None
    data = pd.DataFrame()
    execution_id = "RUN003"
    execution_date = datetime.datetime.now()

    def loadYmlFile(self, path):
        stream = open(path, 'r')
        self.yml = load(stream)
        return self.yml

    def loadYml(self, yml):
        self.yml = yml if self.yml is None else df.merge(yml, self.yml)
    
    def datasource(self, name):
        return self.yml['datasource'][name]

    def dataset(self, name):
        return self.yml['dataset'][name]

    def rule(self, name):
        res = self.yml['analysis'][name]

        if 'function' in res:
            res['function'] = self.yml['function'][res['function']]
        return res

    def extractDataset(self, dataset_name):
        self.dset = self.dataset(dataset_name)
        self.data = pd.read_csv(self.datasource(self.dset['datasource'])['url'], index_col=False, parse_dates=[0])
        return self.data

    def showFails(self, rule_name):
        ret = self.executeRule(rule_name)
        return ret[ret.value == 0]
    
    def executeRule(self, rule_name, add_keywords=False):
        rule = self.rule(rule_name)
        r = df.getRuleLambda(rule)
        
        if 'filter' in rule:
            input = df.filter(self.data, rule['filter'])
        else:
            input = self.data

        # add source_id
        res = r(input)
        update_to_list = lambda a: a if isinstance(a, list) else [a]
        res['source_id'] = eval("+ ', ' +".join(map(lambda c: "input['" + c + "'].map(str)", update_to_list(self.dset['id']))))

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
                res[key] = value

        return res

    def commit(self, rule_name):
        result = self.executeRule(rule_name, True)
        result['execution_id'] = self.execution_id
        result['execution_date'] = self.execution_date
        engine = create_engine(self.yml['datasource']['Result']['url'])
        engine.execute("delete from row_analysis_history where analysis_name='" + rule_name + "'")
        result.to_sql('row_analysis_history', con=engine, if_exists = 'append', chunksize=50, index=False)
