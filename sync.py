#!/usr/bin/env python
#
# This needs python 3.6 or better
#

import json
import oauthlib
import mysql.connector
from sys import argv, exit
from os.path import exists
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session
from mysql.connector import errorcode

def kaput(error_string):
    print(f"### ERROR:  {error_string}")
    exit(13)
    
def read_config(config_file):
    if exists(config_file):
        config_fh = open(config_file)
        config = json.load(config_fh)
        config_fh.close
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
                raw_returned_addr = str(result_dict['return'][id_num]['emails'][e_mail_obj]['email'])
                returned_addr.append(raw_returned_addr.lower())
        return(returned_addr)

        
    else:
        kaput("API call did not return 200.  It returned {}".format(result))

def get_club_members(config):
    club_members = []
    try:
        cnx = mysql.connector.connect(
              user=config['db_username'],
              password=config['db_password'],
              host = config['db_host'],
              database=config['db_name'])
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          kaput("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          kaput("Database does not exist")
        else:
          kaput(err)
    else:
        cursor = cnx.cursor()
        cursor.execute(config['db_query_string'])
        for (ignored1, email, ignored2, ignored3, ignored4) in cursor:
            raw_email = str(email)
            club_members.append(raw_email.lower())
        cursor.close()
        cnx.close()
        return(club_members)

def compare_lists(club_members,current_subscribers):
    members_to_add = []

    for club_member in club_members:
        if club_member in current_subscribers:
            current_subscribers.remove(club_member)
        else:
            members_to_add.append(club_member)
    return(current_subscribers,members_to_add)
    
def main():
    config = read_config(argv[1])
    api = get_oauth_token(config)
    current_subscribers = query_api(api,config)
    club_members = get_club_members(config)
    members_to_remove, members_to_add = compare_lists(club_members,current_subscribers)



if __name__ == '__main__':
    main()
