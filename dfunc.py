import pandas as pd
import rule_functions as rf


def getRuleLambda(rule):
    res = []
    funcName = rule['type'] if 'type' in rule else 'udf'
    for key, value in rule.items():
        if key not in ['type','description','dataset', 'points', 'keywords','filter']:
            res.append(key + '=' + ("'" + value + "'" if isinstance(value, str) else str(value)))
    
    return eval('lambda d: rf.' + funcName + '(data=d, ' + ', '.join(res) + ')')

def apply(data, expr):
    return data.apply(eval("lambda " + expr), axis=1)

def filter(data, expr):
    return data[eval("lambda " + expr.replace('->',': '))(data)]





