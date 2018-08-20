import pandas as pd
import dfunc as df
import numpy as np
import yaml
from yaml import load, dump


class dq:

    yml = None
    data = pd.DataFrame()

    def loadYmlFile(self, path):
        stream = open(path, 'r')
        self.yml = load(stream)
        return self.yml
    
    def datasource(self, name):
        return yml['datasource'][name]

    def dataset(self, name):
        return yml['dataset'][name]

    def rule(self, name):
        return yml['analysis'][name]

    def extractDataset(self, dataset_name):
        self.data = pd.read_csv(self.datasource(self.dataset(dataset_name)['datasource'])['url'], index_col=False, parse_dates=[0])
        return self.data
    
    def executeRule(self, rule_name):
        rule = self.rule(rule_name)
        r = df.getRuleLambda(rule)
        
        if 'filter' in rule:
            input = df.filter(self.data, rule['filter'])
        else:
            input = self.data

        res = r(input)
        return res


mydq = dq()
yml = mydq.loadYmlFile('test.yml')
mydq.extractDataset('titanic')
print(mydq.executeRule('a3'))

