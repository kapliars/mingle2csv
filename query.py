#!/usr/bin/env python

#curl -XGET -d mql="SELECT number where type=story and sprint=number 768" https://user:password@minglehosting.thoughtworks.com/account/api/v2/projects/project_id/cards/execute_mql.json

import httplib2
from urllib import urlencode
import xml.etree.ElementTree as ET
import logging
_log = logging.getLogger(__name__)
import copy

def __config_logging(verbosity):
    level = {0: 'WARN', 1: 'INFO', 2:'DEBUG', None: None}[verbosity] or 'WARN'    

    import logging.config
    logging.config.dictConfig({
        'version': 1,              
        'disable_existing_loggers': False,
        'formatters': {
            'console': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            },
        },
        'handlers': {
            'default': {
                'level':level,    
                'class':'logging.StreamHandler',
            },  
        },
        'loggers': {
            '': {                  
                'handlers': ['default'],        
                'level': level,  
                'propagate': True  
                }
        }
    }) 

class MingleApi(object):
    def __init__(self, url, is_secure, username, password, project):
        self.url = url
        self.username, self.password = username, password
        self.project = project
        self.carrier = httplib2.Http() 
        protocol = "https" if is_secure else "http"
        self.carrier.add_credentials(username, password)
        self.base_url = "%s://%s/api/v2/projects/%s" % (protocol, url, project)
        self.query_url = "%s/cards/execute_mql.xml" % self.base_url

    def send_query(self, mql):
        _log.debug("Send query >>%s<<", mql)
        response, content = self.carrier.request(self.query_url, "POST", body=urlencode({'mql': mql}))
        if response.status != 200:
            _log.debug("Response %s:\n--------\n%s\n----", response, content)
        return QueryResult(content)

    def historical_queries(self, queries, dates):
        results = QueryResult()
        for date in dates:
            result = QueryResult()
            for query in queries:
                result.merge(self.send_query(query['query'] % (" AS OF '" + date + "'")), query['prefix'])
            results.union(result, rownum=date)
        return results


class QueryResult(object):
    def __init__(self, xml=None):
        if not xml:
            self.rows = []
            return
        root = ET.fromstring(xml)
        rows = []
        for item in root:
            # convert items in dict
            row = {}
            for field in item:
                row[field.tag] = field.text 
            rows.append(row)
        self.rows = rows

    def merge(self, other, prefix=""):
    	""" Merge this query result with other, providing results of double width """
        if len(self.rows) > 1 or len(other.rows) > 1:
            raise Exception("Only one line results may be merged")
        t = dict([(prefix + k, v) for (k,v) in other.rows[0].items()])
 
        if len(self.rows) == 0:
            if len(other.rows) == 0:
                return
            self.rows.append(t)
        else:
           self.rows[0].update(t)

    def union(self, other, rownum=None):
        """ Add rows of other to self """
        r = copy.deepcopy(other.rows)
        if rownum:
            for a in r:
                _log.debug("R is %s", r)
                a['rownum'] = rownum
        self.rows.extend(r)
            

    def to_dict(self):
        result = [self.rows[0].keys()]
        result.extend([[row[k] for k in result[0]] for row in self.rows])
        return result 
             
    def to_csv(self):
        return "\n".join( [",".join([str(x) for x in y]) for y in self.to_dict()])


def history_query(url, is_secure, username, password, project, queries, dates):
    """ aply same query to a set of historical dates, render results in table """ 
    _log.debug("queries %s, dates %s", queries, dates)
    mingle = MingleApi(url, is_secure, username, password, project)
    return mingle.historical_queries(queries, dates)

def execute_mql(url, is_secure, username, password, project, query):
    mingle = MingleApi(url, is_secure, username, password, project)
    return mingle.send_query(query)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', dest='username', help='username')
    parser.add_argument('-P', dest='password', help='password')
    parser.add_argument('-l', dest='url', help='url')
    parser.add_argument('-s', dest='is_secure', action='store_true')
    parser.add_argument('-p', dest='project', help='project name')
    parser.add_argument('-o', dest='output', help='output file')
    parser.add_argument('-v', dest="verbosity", action='count')
    parser.add_argument('-d', dest="dates", action='append')
    parser.add_argument('-y', dest='query', default=None)
    parser.add_argument('--qf', dest='query_file', default=None)

    
    options = parser.parse_args()
    
    __config_logging(options.verbosity)
    _log.debug(options)

    #TODO: separate lib and two tools
    csv = None
    if options.dates or options.query_file:
        queries = []
        dates = options.dates and [date for date in options.dates] or []

        if options.query_file:
            import yaml
            with open(options.query_file) as f:
                config = yaml.load(f)

            queries.extend(config['queries'])
            dates.extend(config['dates'])

        _log.debug("Queries: %s, Dates %s", queries, options.dates)
        csv = history_query(options.url, 
                            options.is_secure, 
                            options.username, 
                            options.password, 
                            options.project, 
                            queries, 
                            dates).to_csv()
    else:
        csv = execute_mql(options.url, options.is_secure, options.username, 
                          options.password, options.project, options.query).to_csv()

    if options.output:
        _log.debug("CSV is %s, type %s", csv, type(csv))
        with open(options.output, 'w') as out:
            out.write(csv)
    else:
        print csv
