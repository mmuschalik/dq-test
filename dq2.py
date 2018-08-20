import pandas as pd
import dfunc as df
import numpy as np
import yaml
from yaml import load, dump


class dq:

    yml = None
    dset = None
    data = pd.DataFrame()

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
    
    def executeRule(self, rule_name):
        rule = self.rule(rule_name)
        r = df.getRuleLambda(rule)
        
        if 'filter' in rule:
            input = df.filter(self.data, rule['filter'])
        else:
            input = self.data

        res = r(input)
        update_to_list = lambda a: a if isinstance(a, list) else [a]
        res['source_id'] = eval("+ ', ' +".join(map(lambda c: "input['" + c + "'].map(str)", update_to_list(self.dset['id']))))
        return res


mydq = dq()
yml = mydq.loadYmlFile('test.yml')
mydq.extractDataset('titanic')
print(mydq.executeRule('a4'))

