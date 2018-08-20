import pandas as pd
from itertools import chain
import numpy as np
import re

tuple_to_str = lambda t: ', '.join(map(str, t)) if isinstance(t, tuple) else str(t)
add_lambda = lambda a: 'r[\'' + str(a) + '\']'



## generic rule functions for dataframes:
## value, total_value, selector
def occurancePercentile(data, field, percentile):
    res = pd.DataFrame()
    
    vc = data[field].value_counts()
    limit = vc.quantile(percentile)

    res['selector'] = data[field]
    res['value'] = data.apply(lambda d: 0 if vc.loc[d[field]] >= limit else 1, axis=1)
    res['total_value'] = 1

    return res

def uniqueness(data, fields):
    res = pd.DataFrame()
    
    dup = data.duplicated(fields, keep='first')
    
    res['selector'] = data.apply(eval('lambda r: tuple_to_str((' + ', '.join(map(add_lambda, fields)) + '))'), axis=1)
    res['value'] =  dup.apply(lambda d: 0 if d == True else 1)
    res['total_value'] = 1

    return res

def udf(data, bind, function):
    res = pd.DataFrame()

    fn = function['expression'].replace('->',': ')
    value = bind.replace('->', ': (lambda ' + fn + ')(') + ')'
    selector = bind.replace('->', ': tuple_to_str((') + '))'

    res['selector'] = data.apply(eval("lambda " + selector), axis=1)
    res['value'] = data.apply(eval("lambda " + value), axis=1)
    res['total_value'] = function['max']

    return res