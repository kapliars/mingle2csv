#!/usr/bin/env python

#curl -XGET -d mql="SELECT number where type=story and sprint=number 768" https://user:password@minglehosting.thoughtworks.com/account/api/v2/projects/project_id/cards/execute_mql.json

import httplib2
from urllib import urlencode
import xml.etree.ElementTree as ET
import logging
_log = logging.getLogger(__name__)


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
        response, content = self.carrier.request(self.query_url, "POST", body=urlencode({'mql': mql}))
        if response.status != 200:
            _log.debug("Response %s:\n--------\n%s\n----", response, content)
        return QueryResult(content)

class QueryResult(object):
    def __init__(self, xml):
        root = ET.fromstring(xml)
        rows = []
        for item in root:
            # convert items in dict
            row = {}
            for field in item:
                row[field.tag] = field.text 
            rows.append(row)
        self.rows = rows

    def _to_csv(self):
        result = [row.values() for row in self.rows]
        result.insert(0, self.rows[0].keys())
        return result 
             
    def to_csv(self):
        return "\n".join( [",".join([str(x) for x in y]) for y in self._to_csv()])

def execute_mql(url, is_secure, username, password, project, query):
    mingle = MingleApi(url, is_secure, username, password, project)
    return mingle.send_query(query)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', dest='username', help='username')
    parser.add_argument('-P', dest='password', help='password')
    parser.add_argument('-l', dest='url', help='url')
    parser.add_argument('-s', dest='is_secure', action='store_const', const=True, default=False)
    parser.add_argument('-p', dest='project', help='project name')
    parser.add_argument('-o', dest='output', help='output file')
    parser.add_argument('-v', dest="verbosity", action='count')
    parser.add_argument('query')
    
    options = parser.parse_args()
    
    __config_logging(options.verbosity)

    csv = execute_mql(options.url, options.is_secure, options.username, options.password, options.project, options.query).to_csv()
    if options.output:
        _log.debug("CSV is %s, type %s", csv, type(csv))
        with open(options.output, 'w') as out:
            out.write(csv)
    else:
        print csv
