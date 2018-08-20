import pandas as pd


def occurancePercentile(data, field, percentile):
    vc = data[field].value_counts()
    limit = vc.quantile(percentile)
    return data.apply(lambda d: 0 if vc.loc[d[field]] >= limit else 1, axis=1)

def isDuplicate(data, fields):
    dup = data.duplicated(fields, keep='first')
    return dup.apply(lambda d: 0 if d == True else 1)

def apply(data, expr):
    return data.apply(eval("lambda " + expr), axis=1)

def filter(data, expr):
    return data[eval("lambda " + expr.replace('->',': '))(data)]