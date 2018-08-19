class Datasource:
    def __init__(self, name, datasource_type, url):
        self.name = name
        self.datasource_type = datasource_type
        self.url = url
    
class Dataset:
    def __init__(self, name, query, datasource, id):
        self.name = name
        self.query = query
        self.datasource = datasource
        self.id = id

class Analysis:
    def __init__(self, name, description, dataset, bind, function, points, filter=""):
        self.name = name
        self.description = description
        self.dataset = dataset
        self.bind = bind
        self.function = function
        self.points = points
        self.filter = filter

class DuplicateAnalysis:
    def __init__(self, name, description, dataset, fields, points):
        self.name = name
        self.description = description
        self.dataset = dataset
        self.fields = fields
        self.points = points

class Function:
    def __init__(self, name, description, expression, max):
        self.name = name
        self.description = description
        self.expression = expression
        self.max = max

from sqlalchemy import create_engine
import pandas as pd
import yaml
from yaml import load, dump
import re

tuple_to_str = lambda t: ', '.join(map(str, t)) if isinstance(t, tuple) else str(t)
add_lambda = lambda a: 'r[\'' + str(a) + '\']'

class dq:

    data_sources = {}
    data_sets = {}
    functions = {}
    analysises = {}
    last_data_source = ""

    def load_yml_file(self, path):
        stream = open(path, 'r')
        yml = load(stream)
        self.load_yml(yml)

    def load_yml(self, yml):

        if 'datasource' in yml:
            for d in yml['datasource']:
                 ds = yml['datasource'][d]
                 self.add_datasource(Datasource(d, ds['type'], ds['url']))

        if 'dataset' in yml:
            for d in yml['dataset']:
                 ds = yml['dataset'][d]
                 self.add_dataset(Dataset(d, ds['query'], ds['datasource'], ds['id']))
        
        if 'function' in yml:
            for f in yml['function']:
                 fs = yml['function'][f]
                 self.add_function(Function(f, fs['description'], fs['expression'], fs['max']))
        
        if 'analysis' in yml:
            for d in yml['analysis']:
                 ds = yml['analysis'][d]
                 if 'type' in ds and ds['type'] == "duplication":
                     self.add_analysis(DuplicateAnalysis(d, ds['description'], ds['dataset'], ds['fields'],ds['points']))
                 else:
                     self.add_analysis(Analysis(d, ds['description'], ds['dataset'], ds['bind'], ds['function'], ds['points']))

    def add_datasource(self, datasource):
        self.data_sources[datasource.name] = datasource

    def add_dataset(self, dataset):
        self.data_sets[dataset.name] = dataset

    def add_function(self, function):
        self.functions[function.name] = function

    def add_analysis(self, analysis):
        self.analysises[analysis.name] = analysis

    def connectByName(self, datasource_name):
        self.connect(self.data_sources[datasource_name])

    def connect(self, datasource):
        if datasource.datasource_type == 'sql':
            engine = create_engine(datasource.url)
            engine.connect()

    def discoverByName(self, datasource_name):
        return self.discover(self.data_sources[datasource_name])

    def discover(self, datasource):
        return self.extract(datasource, Dataset("schema", "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'", datasource.name, ""))

    def extract(self, datasource, dataset):
        data = pd.DataFrame()
        if datasource.datasource_type == 'sql':
            engine = create_engine(datasource.url)
            data = pd.read_sql_query(dataset.query, con=engine)
        else:
            data = pd.read_csv(datasource.url, index_col=[0], parse_dates=[0])
			
        data = data.reset_index()
        return data

    def extractByName(self, dataset_name):
        return self.extract(self.data_sources[self.data_sets[dataset_name].datasource],self.data_sets[dataset_name])

    def sampleByName(self, dataset_name, size):
        return self.extractByName(dataset_name).head(size)

    def describeByName(self, dataset_name):
        data = self.extract(self.data_sources[self.data_sets[dataset_name].datasource],self.data_sets[dataset_name])
        return data.dtypes

    def discoverAllByName(self, datasource_name):
        self.last_data_source = datasource_name
        return self.discoverAll(self.data_sources[datasource_name])

    def describe(self, schema, table):
        datasource = self.data_sources[self.last_data_source]
        self.add_dataset(Dataset(table, "SELECT * FROM " + schema + '.' + table, datasource.name, ""))
        data = self.extract(datasource, self.data_sets[table])
        result = pd.DataFrame()
        result['col'] = list(data)
        result['nulls'] = result.apply(lambda r: len(data[data[r.col].isnull()]), axis=1)
        result['unique'] = result.apply(lambda r: len(data[r.col].unique()), axis=1)
        result['cnt'] = len(data)
        result = result[result.nulls != result.cnt]
        return result
		

    def discoverAll(self, datasource):
        data = self.discover(datasource)
        result = pd.DataFrame()
        for index, row in data.iterrows():
            inter = self.extract(datasource, Dataset("schema", "SELECT count(*) as cnt FROM " + row['TABLE_SCHEMA'] + '.' + row['TABLE_NAME'], datasource.name, ""))
            inter['TABLE_NAME'] = row['TABLE_NAME']
            inter['TABLE_SCHEMA'] = row['TABLE_SCHEMA']
            result = pd.concat([result,inter])
        result = result.sort_values(by=['cnt'], ascending=False)
        result = result[result.cnt > 0]
        return result
		
		
    def analyseShowFail(self, analysis_name):
        data = self.analyse(analysis_name)
        return data[data.value == 0]
		
    def showFails(self, analysis_name):
	    return self.analyseShowFail(analysis_name)

    def analyse(self, analysis_name, show_keywords=False):
        analysis = self.analysises[analysis_name]
        dataset = self.data_sets[analysis.dataset]
        datasource = self.data_sources[dataset.datasource]
        data = self.extractByName(analysis.dataset)
		
        if(analysis.filter != ""):
            data = data[eval("lambda " + analysis.filter.replace('->',': '))(data)]
		
        maxValue = 1
        
        if(isinstance(analysis, Analysis)):
            function = self.functions[analysis.function]
            maxValue = function.max
            fn = function.expression.replace('->',': ')
            value = analysis.bind.replace('->', ': (lambda ' + fn + ')(') + ')'
            selector = analysis.bind.replace('->', ': tuple_to_str((') + '))'
            data['value'] = data.apply(eval("lambda " + value), axis=1)
        else:
            data['duplicate'] = data.duplicated(analysis.fields, keep='first')
            selector = 'r: tuple_to_str((' + ', '.join(map(add_lambda, analysis.fields)) + '))'
            data['value'] = data.apply(lambda r: 0 if r.duplicate == True else 1, axis=1)
            
        pkey = dataset.id.replace('->', ': tuple_to_str((') + '))'
        
        data['source_id'] = data.apply(eval("lambda " + pkey), axis=1)
        data['selector'] = data.apply(eval("lambda " + selector), axis=1)
        data = data[['source_id','value','selector']]

        keywords = {
            'total_value': maxValue, 
            'points': analysis.points,
            'dataset_name': dataset.name,
            'analysis_name': analysis.name,
            'analysis_description': analysis.description
            }
        
        #keywords.update(datasource.keywords)
        #keywords.update(dataset.keywords)
        #keywords.update(analysis.keywords)

        if show_keywords:
            for key, value in keywords.items():
                data[key] = value

        return data
