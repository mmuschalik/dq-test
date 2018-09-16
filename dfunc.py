import pandas as pd
import rule_functions as rf
import inspect


def getRuleLambda(rule):
    res = []
    funcName = rule['type'] if 'type' in rule else 'udf'
    for key, value in rule.items():
        if key not in ['type', 'description', 'dataset', 'points', 'keywords', 'filter']:
            res.append(key + '=' + ('"' + value +
                                    '"' if isinstance(value, str) else str(value)))

    return eval('lambda d: rf.' + funcName + '(data=d, ' + ', '.join(res) + ')')


def getRuleWithDataFrames(rule, dict, yml, rule_name):
    res = []
    funcName = rule['type'] if 'type' in rule else 'udf'
    for key, value in rule.items():
        if key not in ['type', 'description', 'dataset', 'points', 'keywords', 'filter', 'function'] and key not in dict.keys():
            res.append(key + '=' + ('"' + value +
                                    '"' if isinstance(value, str) else str(value)))

    if('yml' in inspect.signature(eval("rf." + funcName)).parameters.keys()):
        res.append("yml=yml")
    if('rule_name' in inspect.signature(eval("rf." + funcName)).parameters.keys()):
        res.append("rule_name=rule_name")

    if 'function' in rule:
        for key, value in yml['function'][rule['function']].items():
            if key != 'description':
                res.append(key + '=' + ('"' + value +
                                        '"' if isinstance(value, str) else str(value)))

    for key in dict.keys():
        res.append(key + '=' + 'dict[\'' + key + '\']')

    eval_str = 'rf.' + funcName + '(' + ', '.join(res) + ')'

    return eval(eval_str)


def apply(data, expr):
    return data.apply(eval("lambda " + expr), axis=1)


def filter(data, expr):
    return data[eval("lambda " + expr.replace('->', ': '))(data)]


def merge(source, destination):
    for key, value in source.items():
        if isinstance(value, dict):
            node = destination.setdefault(key, {})
            merge(value, node)
        else:
            destination[key] = value
    return destination
