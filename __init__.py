import requests
import json
import pandas as pd
#import pyodbc
import datetime
import logging
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="httpAzureGraph")
def httpAzureGraph(req: func.HttpRequest) -> func.HttpResponse:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()


    app_id = 'xxxxxxxxxxxxxxxx' #Application Id - on the azure app overview page
    client_secret = 'xxxxxxxxxxxxxxxx' #SecretValue Id
    tenantId = 'xxxxxxxxxxxxxxxx' #Tenant Id of Azure Suscription
    #Use the redirect URL to create a token url
    token_url = 'https://login.microsoftonline.com/'+tenantId+'/oauth2/token'

    token_data = {
        'grant_type': 'client_credentials',
        'client_id': app_id,
        'client_secret': client_secret,
        'resource': 'https://graph.microsoft.com',
        'scope':'https://graph.microsoft.com'
    }

    token_r = requests.post(token_url, data=token_data)
    token = token_r.json().get('access_token')

    token_r.content

    '''
    code snippet insert into database

    server = 'SERVERNAME\INSTANCEID,64346'
    database = 'GRAPHAPI' 
    username = 'uname' 
    password = 'Happy2020'


    def insert_user_db(users_df,server,database,username,password):
    #Create a connection string
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    for index in range(users_df.shape[0]): 
        #insert into table 
        try:
            insert_query = “INSERT INTO GRAPHAPI.dbo.[DIM_User] ([userId],[displayName],[mailAddress]) VALUES (?,?,?)”
            cursor.execute(insert_query,users_df['userId'][index],users_df['displayName'][index],users_df['mailAddress'][index])
        except:
            cnxn.rollback()
        finally:
            cnxn.commit()
            cnxn.close()
            #Call the function
            insert_user_db(users_df,server,database,username,password)
    '''


    # Use the token using microsoft graph endpoints
    users_url = 'https://graph.microsoft.com/v1.0/users?$top=500'

    headers = {
        'Authorization': 'Bearer {}'.format(token)
    }

    user_response_data = json.loads(requests.get(users_url, headers=headers).text)

    # user_response_data['@odata.nextLink']
    #initial user data
    #get all users

    userId =[]
    displayName =[]
    mailAddress =[]
    

    print(user_response_data)


    for user in user_response_data['value']:
     userId.append(user['id'])
     displayName.append(user['displayName'])
     mailAddress.append(user['userPrincipalName'])
    
    users_dict = {'userId':userId,'displayName':displayName,'mailAddress':mailAddress}
    users_df = pd.DataFrame(data=users_dict)
    print(users_df)

    '''
    #additional user query for paging
    while '@odata.nextLink' in user_response_data:
     user_response_data = json.loads(requests.get(users_url, headers=headers).text)
     if '@odata.nextLink' in user_response_data: 
     users_url = user_response_data['@odata.nextLink']
    
     for user in user_response_data['value']:
     userId.append(user['id'])
     displayName.append(user['displayName'])
     mailAddress.append(user['userPrincipalName'])
     
     users_dict = {'userId':userId,'displayName':displayName,'mailAddress':mailAddress}
     users_df = pd.DataFrame(data=users_dict)
     users_df.head()
    '''
    
    

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
