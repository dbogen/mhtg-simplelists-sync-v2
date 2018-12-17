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
        config_fh = open(config_file) # create filehandle
        config = json.load(config_fh) # read file json into dict
        config_fh.close
        return(config)
    else:
        kaput("Config file not found.")

def get_oauth_token(config):
    #
    # Fetch an OAUTH token from simplelists and stick it into a Requests
    # object that we can use to query the Simplelists API.
    #
    simplelists = OAuth2Session(client=LegacyApplicationClient(client_id=config['client_id']))
    token = simplelists.fetch_token(token_url=config['token_url'],
        username=config['username'], password=config['password'], 
        client_id=config['client_id'], client_secret=config['client_secret'])
    return(simplelists)

def query_api(api,config):
    current_subscribers = {}
    #
    # Create a dict that we can turn into a json object to query the
    # simplelists API.  This is their specified format.
    #
    query_json = {'entity':'contact',
            'action': 'get',
            'params': {
                'match':'all',
                'conditions':[
                    {
                        'field':'email',
                        'value':config['search_text'],
                        'op':'like'
                        }
                    ]
                }}
    json_obj = json.dumps(query_json)
    #
    # Query the API using the Results object, the OAUTH token, and the
    # JSON object we created above.
    #
    result = api.post(config['api_url'],data=json_obj)
    if result.status_code == 200: # keep going only if we had success
        #
        # The API supplies results in JSON.
        # Load the results into a dict so that we can iterate over them.
        #
        # Uncomment this to see the JSON returned by your search.
        #print(result.text)
        result_dict = json.loads(result.text)
        #
        # there's probably a better way to do this, but this is what I
        # came up with right now.
        #
        for id_num in result_dict['return']:
            for e_mail_obj in range(0,len(result_dict['return'][id_num]['emails'])):
                raw_returned_addr = str(result_dict['return'][id_num]['emails'][e_mail_obj]['email'])
                #
                # Store the e-mail as the dict key; store the Simplelists
                # ID numbers as the value.  We need both for later
                # operations.
                #
                current_subscribers[raw_returned_addr.lower()] = id_num
        #
        # Simple circuit breaker here.  If we get fewer than 350 entries
        # in the dict, abort.  Something is probably wrong.
        #
        if len(current_subscribers) < 350:
            kaput("We didn't get as many subscribers as we should have.  Exiting to prevent disaster.")
        else:
            pass
        return(current_subscribers)

        
    else:
        kaput("API call did not return 200.  It returned {}".format(result))

def get_club_members(config):
    club_members = []
    #
    # This try/except block is pretty much right out of the
    # mysql.connector docs.  Connect to the database and create a 
    # connection object.
    #
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
        #
        # We're in.  create a cursor to conduct queries for us.
        # Pack in the canned query in the config.json
        #
        cursor = cnx.cursor()
        cursor.execute(config['db_query_string'])
        #
        # We get back a bunch of fields we don't need; we only need
        # email so throw away everything else.
        #
        for (ignored1, email, ignored2, ignored3, ignored4) in cursor:
            raw_email = str(email)
            #
            # Turn e-mail to lowercase (like we did above for what we got
            # out of the API) so that comparisons between the two are 
            # easier.
            #
            club_members.append(raw_email.lower())
        cursor.close()
    cnx.close()
    #
    # Again, another simple circuit breaker.  If we got fewer than 350
    # entries from the database, abort.
    #
    if len(club_members) < 350:
        kaput("We got fewer than 350 club members from the database. Exiting to prevent disaster.")
    else:
        pass
    return(club_members)

def compare_lists(club_members,current_subscribers):
    members_to_add = []

    #
    # The goal here is to return two things:
    # 1) A list of e-mail address in the club db that need to be added to
    #    the simplelists address book and the members list.
    # 2) A list of the e-mail addresses in the simplelists contact list
    #    that needed to be removed because they are no longer club members.
    #
    # Do that by removing all e-mail addresses between the two lists that
    # are the same.  That leaves people who need to come off the list
    # on the Simpleilsts roster. Create a list of people who need to be
    # added to Simpleilsts.
    #
    for club_member in club_members:
        if club_member in current_subscribers:
            del current_subscribers[club_member]
        else:
            members_to_add.append(club_member)
    return(current_subscribers,members_to_add)

def add_members(api,config,members_to_add):
    if len(members_to_add) == 0:
        print("No new members to add.")
        return(0)

    new_members = []
    #
    # This dict is dictated by the Simplelists API.
    #
    post_json = {
            "entity": "contact",
            "action": "create",
            "options": {
                "append_lists":"true"
            }
        }
 
    #
    # Construct the dict format needed by the Simplelists API to get
    # people added and on he right list.
    #
    for this_member in members_to_add:
        temp_dict = {
                    
                        "emails" : [
                        {
                            "email":this_member
                        }
                        ],
                        "lists": [
                        {
                            "name":config['list_name'],
                            "id":config['list_id_num']
                        }
                        ] 
                    }
        new_members.append(temp_dict)
        temp_dict.clear()

    #
    # Add the array of dicts to the post_json dict and then convert the
    # whole mess to JSON.
    #
    post_json['params'] = new_members
    json_obj = json.dumps(post_json)
    #
    # Uncomment this to see the JSON you're going to submit to the API.
    #print(json_obj)
    #
    # Query the API using the Results object, the OAUTH token, and the
    # JSON object we created above.
    #
    result = api.post(config['api_url'],data=json_obj)
    #
    # Uncomment this to see raw json returned by the API.
    #
    #print(result.text)
    result_dict = json.loads(result.text)
    if result_dict['is_error'] == 1:
        kaput "The API returned an error."

def remove_expired_members(api,config,members_to_remove):
    pass
    
def main():
    config = read_config(argv[1])
    api = get_oauth_token(config)
    current_subscribers = query_api(api,config)
    club_members = get_club_members(config)
    members_to_remove, members_to_add = compare_lists(club_members,current_subscribers)
    add_members(api,config,members_to_add)
    remove_expired_members(api,config,members_to_remove)



if __name__ == '__main__':
    main()
