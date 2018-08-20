import pandas as pd
import dfunc as df
import numpy as np
import yaml
from yaml import load, dump

def myfunc(myarg):
    print (myarg)


def getRuleLambda(rule):
    res = []
    funcName = rule['type']
    for key, value in rule.items():
        if key not in ['type','description','dataset', 'points', 'keywords']:
            res.append(key + '=' + ("'" + value + "'" if isinstance(value, str) else str(value)))
    return eval('lambda d: df.' + funcName + '(data=d, ' + ', '.join(res) + ')')

stream = open('test.yml', 'r')
yml = load(stream)
r = getRuleLambda(yml['analysis']['a1'])

data = pd.read_csv("titanic.csv", index_col=False, parse_dates=[0])
#data['duplicate'] = df.isDuplicate(data, fields=['Sex'])
#data['percentile'] = df.occurancePercentile(data, 'Pclass', 0.9)
data = r(data)
print(data)
#eval("myfunc(myarg='test')")


