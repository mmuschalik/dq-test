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

def whiteblacklist(data, list, bind, function):
    res = pd.DataFrame()
    
    nbind = bind + ', ' + str(list)
    fn = function['expression'].replace('->',': ')
    value = nbind.replace('->', ': (lambda ' + fn + ')(') + ')'
    selector = bind.replace('->', ': tuple_to_str((') + '))'

    res['selector'] = data.apply(eval("lambda " + selector), axis=1)
    res['value'] = data.apply(eval("lambda " + value), axis=1)
    res['total_value'] = function['max']

    return res

def regex(data, bind, regex, regexType):
    res = pd.DataFrame()
    
    fn = ("a -> 1 if a is None or pd.isna(a) or (isinstance(a, str) and (a.strip()=='' or re.search(r'" + regex + "', a))) else 0").replace('->',': ')
    value = bind.replace('->', ': (lambda ' + fn + ')(') + ')'
    selector = bind.replace('->', ': tuple_to_str((') + '))'

    res['selector'] = data.apply(eval("lambda " + selector), axis=1)
    res['value'] = data.apply(eval("lambda " + value), axis=1)
    res['total_value'] = 1

    return res


def aboveNormalFrequency(data, field, stdeviations, threshold):
    res = pd.DataFrame()
    
    freq = data[field].value_counts()
    freq = freq[freq > 2]
    
    mean = freq.mean()
    std  = freq.std()
    limit = mean + (std * stdeviations)
    
    print(mean)
    
    exceptions = freq[freq > limit]
    exceptions = limit / exceptions
    exceptions = exceptions[exceptions < threshold]
    
    res['selector'] = data[field]
    res['value'] = [exceptions[x] if x in exceptions else 1 for x in data[field]]
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

def integration(data, lookup, keys, match):
    res = pd.DataFrame()

    lookup = lookup.set_index(list(keys.keys()))

    tup = lambda r: tuple(map( (lambda d: eval('lambda ' + d.replace('->',': ')))(r), keys.values())) if len(keys)>1 else eval('lambda ' + list(keys.values())[0].replace('->',': '))(r)

    res['selector'] = data.apply(lambda r: tuple_to_str(tup(r)), axis=1)
    res['value'] = data.apply(lambda r: 1 if tup(r) in lookup.index else 0, axis=1)

    res['total_value'] = 1

# ('a','3') in lookup.index
# lookup.loc[('a','3')]

    return res