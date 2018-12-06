#!/usr/bin/env python
#
# This needs python 3.6 or better
#

import json
import oauthlib
from sys import argv, exit
from os.path import exists
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session

def kaput(error_string):
    print(f"### ERROR:  {error_string}")
    exit(13)
    
def read_config(config_file):
    if exists(config_file):
        config_fh = open(config_file)
        config = json.load(config_fh)
        return(config)
    else:
        kaput("Config file not found.")

def get_oauth_token(config):
    client_id = config['client_id'] 
    client_secret = config['client_secret']
    simplelists = OAuth2Session(client=LegacyApplicationClient(client_id=client_id))
    token = simplelists.fetch_token(token_url=config['token_url'],
        username=config['username'], password=config['password'], 
        client_id=client_id, client_secret = client_secret)
    return(simplelists)

def query_api(api,config):
    returned_addr = []
    query_json = {'entity':'contact',
            'action': 'get',
            'params': {
                'match':'all',
                'conditions':[
                    {
                        'field':'surname',
                        'value':config['list_name'],
                        'op':'like'
                        }
                    ]
                }}
    json_obj = json.dumps(query_json)
    result = api.post(config['api_url'],data=json_obj)
    if result.status_code == 200:
        result_dict = json.loads(result.text)
        for id_num in result_dict['return']:
            for e_mail_obj in range(0,len(result_dict['return'][id_num]['emails'])):
                returned_addr.append(result_dict['return'][id_num]['emails'][e_mail_obj]['email'])
        return(returned_addr)

        
    else:
        kaput("API call did not return 200.  It returned {}".format(result))
    
def main():
    config = read_config(argv[1])
    api = get_oauth_token(config)
    current_subscribers = query_api(api,config)


if __name__ == '__main__':
    main()
