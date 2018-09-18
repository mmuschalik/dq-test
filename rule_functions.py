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

def whiteblacklist(data, list, bind, expression, max):
    res = pd.DataFrame()
    
    nbind = bind + ', ' + str(list)
    fn = expression.replace('->',': ')
    value = nbind.replace('->', ': (lambda ' + fn + ')(') + ')'
    selector = bind.replace('->', ': tuple_to_str((') + '))'

    res['selector'] = data.apply(eval("lambda " + selector), axis=1)
    res['value'] = data.apply(eval("lambda " + value), axis=1)
    res['total_value'] = max

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


def uniqueness(data, fields, excludeanynull=False):
    res = pd.DataFrame()
    
    dup   = data.duplicated(fields, keep=False)
    if excludeanynull:
        nulls = data[fields].applymap(lambda a: 1 if a is None or pd.isna(a) or a == '' else 0).max(axis=1)
    else:
        nulls = data[fields].applymap(lambda a: 1 if a is None or pd.isna(a) or a == '' else 0).min(axis=1)
    
    res['selector'] = data.apply(eval('lambda r: tuple_to_str((' + ', '.join(map(add_lambda, fields)) + '))'), axis=1)
    res['value'] =  dup.apply(lambda d: 0 if d == True else 1)
    
    res.loc[nulls == 1, 'value'] = np.nan
    
    res['total_value'] = 1

    return res

def udf(data, bind, expression, max):
    res = pd.DataFrame()

    fn = expression.replace('->',': ')
    value = bind.replace('->', ': (lambda ' + fn + ')(') + ')'
    selector = bind.replace('->', ': tuple_to_str((') + '))'

    res['selector'] = data.apply(eval("lambda " + selector), axis=1)
    res['value'] = data.apply(eval("lambda " + value), axis=1)
    res['total_value'] = max

    return res

def integration(data, lookup, keys, match = {}):
    res = pd.DataFrame()

    lookup = lookup.set_index(list(keys.keys()))

    tup = lambda r: tuple(map( (lambda d: eval('lambda ' + d.replace('->',': ')))(r), keys.values())) if len(keys)>1 else eval('lambda ' + list(keys.values())[0].replace('->',': '))(r)

    res['selector'] = data.apply(lambda r: tuple_to_str(tup(r)), axis=1)
    res['value'] = data.apply(lambda r: 1 if tup(r) in lookup.index else 0, axis=1)

    res['total_value'] = 1

    return res




# simple functions

def checkABN_ACN(a):
    nums = np.array([int(d) for d in str(re.sub('[^0-9]','', str(a)))])
    if len(nums) == 11:
        nums[0] -= 1
        weighting = np.array([10,1,3,5,7,9,11,13,15,17,19])
        if sum(nums*weighting) % 89 == 0:
            return 1 
        else: 
            return 0
    elif len(nums) == 9:
        weighting = np.array([8,7,6,5,4,3,2,1])
        checkdigit = 10 - sum(nums[:8]*weighting) % 10
        checkdigit = 0 if checkdigit == 10 else checkdigit
        if nums[8] == checkdigit:
            return 1
        else:
            return 0
    else:
        return 0