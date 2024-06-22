"""
This code demonstrate how to train a XGBoost Logistic Regression model for credit card fraud detection
The code use datasets from 3 parties
- 2 banks providing the labels (class) for each transactions being fraudulent or not
- A financial data intermediary or payment processor providing the transactions data on which Dimensionality Reduction Techniques for Data Privacy has been applied.

"""


import logging
import time
import requests
import os
import json
from datetime import datetime
import base64
import certifi

#cryptodome
from Crypto.Cipher import PKCS1_OAEP
from Crypto.PublicKey import RSA

from dv_utils import default_settings, Client 
import duckdb
import pandas as pd



logger = logging.getLogger(__name__)

input_dir = "/resources/data"
#input_dir = "config"
keys_input_dir = "/resources/data"
#keys_input_dir = "demo-keys"

output_dir = "/resources/outputs"

# let the log go to stdout, as it will be captured by the cage operator
logging.basicConfig(
    level=default_settings.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# define an event processing function
def event_processor(evt: dict):
    """
    Process an incoming event
    Exception raised by this function are handled by the default event listener and reported in the logs.
    """
    
    logger.info(f"Processing event {evt}")

    # dispatch events according to their type
    evt_type =evt.get("type", "")
    if(evt_type == "SCORE"):
        process_score_event(evt)
    else:
        generic_event_processor(evt)


def generic_event_processor(evt: dict):
    # push an audit log to reccord for an event that is not understood
    logger.info(f"Received an unhandled event {evt}")

def process_score_event(evt: dict):
    """
    Score mule accounts 
     """

    logger.info(f"---------------------------------------------------------")
    logger.info(f"|                     START SCORING                     |")
    logger.info(f"|                                                       |")
    start_time = time.time()
    logger.info(f"|    Start time:  {start_time} secs               |")
    logger.info(f"|                                                       |")
    
    # load the training data from data providers
    # duckDB is used to load the data and aggregated them in one single datasets

    # DATA_PROVIDER_1_URL=os.environ.get("DATA_PROVIDER_1_URL", "")
    # DATA_PROVIDER_2_URL=os.environ.get("DATA_PROVIDER_2_URL", "")
    # DATA_PROVIDER_3_URL=os.environ.get("DATA_PROVIDER_3_URL", "")

    # DATA_PROVIDER_1_ENCRYPTION_KEY = os.environ.get("DATA_PROVIDER_1_ENCRYPTION_KEY", "")
    # DATA_PROVIDER_2_ENCRYPTION_KEY = os.environ.get("DATA_PROVIDER_2_ENCRYPTION_KEY", "")
    # DATA_PROVIDER_3_ENCRYPTION_KEY = os.environ.get("DATA_PROVIDER_3_ENCRYPTION_KEY", "")
    
    # DATA_PROVIDER_1_KEY = os.environ.get("DATA_PROVIDER_1_KEY", "")
    # DATA_PROVIDER_1_SECRET = os.environ.get("DATA_PROVIDER_1_SECRET", "")

    # DATA_PROVIDER_2_CONNECTION_KEY = os.environ.get("DATA_PROVIDER_2_CONNECTION_KEY", "")
    
    # DATA_PROVIDER_3_KEY = os.environ.get("DATA_PROVIDER_3_KEY", "")
    # DATA_PROVIDER_3_SECRET = os.environ.get("DATA_PROVIDER_3_SECRET", "")
    # DATA_PROVIDER_3_REGION = os.environ.get("DATA_PROVIDER_3_REGION", "")

    #load parameters
    accountsList= evt.get("accounts", "")

    #load data access configs
    with open(input_dir+'/data-provider-1.json', 'r', newline='') as file:
        jsonFile = json.load(file)
        DATA_PROVIDER_1_URL=jsonFile["DATA_PROVIDER_URL"]
        DATA_PROVIDER_1_ENCRYPTION_KEY=jsonFile["DATA_PROVIDER_ENCRYPTION_KEY"]
        DATA_PROVIDER_1_KEY=jsonFile["DATA_PROVIDER_KEY"]
        DATA_PROVIDER_1_SECRET=jsonFile["DATA_PROVIDER_SECRET"]
    
    
    with open(input_dir+'/data-provider-2.json', 'r', newline='') as file:
        jsonFile = json.load(file)
        DATA_PROVIDER_2_URL=jsonFile["DATA_PROVIDER_URL"]
        DATA_PROVIDER_2_ENCRYPTION_KEY=jsonFile["DATA_PROVIDER_ENCRYPTION_KEY"]
        DATA_PROVIDER_2_CONNECTION_KEY=jsonFile["DATA_PROVIDER_CONNECTION_KEY"]
        DATA_PROVIDER_2_SHARE_ACCESS_TOKEN=jsonFile["DATA_PROVIDER_SHARE_ACCESS_TOKEN"]


    with open(input_dir+'/data-provider-3.json', 'r', newline='') as file:
        jsonFile = json.load(file)
        DATA_PROVIDER_3_URL=jsonFile["DATA_PROVIDER_URL"]
        DATA_PROVIDER_3_ENCRYPTION_KEY=jsonFile["DATA_PROVIDER_ENCRYPTION_KEY"]
        DATA_PROVIDER_3_KEY=jsonFile["DATA_PROVIDER_KEY"]
        DATA_PROVIDER_3_SECRET=jsonFile["DATA_PROVIDER_SECRET"]
        DATA_PROVIDER_3_REGION=jsonFile["DATA_PROVIDER_REGION"]

    #load private keys
    with open (keys_input_dir+"/key.pem", "rb") as prv_file:
        privateKey=prv_file.read()
    cipher = PKCS1_OAEP.new(RSA.importKey(privateKey))

    logger.info(f"| 1. Connect data from data providers                   |")
    logger.info(f"|    {DATA_PROVIDER_1_URL} |")
    logger.info(f"|    {DATA_PROVIDER_2_URL}  |")
    logger.info(f"|    {DATA_PROVIDER_3_URL} |")

    
    #check if data exists in memory
    res=duckdb.sql("SHOW ALL TABLES; ")
    if len(res)==0:
        
        #dataset1
        res=duckdb.sql(f"PRAGMA add_parquet_key('DATA_PROVIDER_1_ENCRYPTION_KEY', '{DATA_PROVIDER_1_ENCRYPTION_KEY}')")
        res=duckdb.sql(f"CREATE SECRET (TYPE GCS,KEY_ID '{DATA_PROVIDER_1_KEY}',SECRET '{DATA_PROVIDER_1_SECRET}');")

        #dataset2
        res=duckdb.sql(f"PRAGMA add_parquet_key('DATA_PROVIDER_2_ENCRYPTION_KEY', '{DATA_PROVIDER_2_ENCRYPTION_KEY}')")
        res=duckdb.sql(f"CREATE SECRET (TYPE AZURE,CONNECTION_STRING '{DATA_PROVIDER_2_CONNECTION_KEY}');")
        res=duckdb.sql("SET azure_transport_option_type = 'curl';")
    
        #dataset3
        res=duckdb.sql(f"PRAGMA add_parquet_key('DATA_PROVIDER_3_ENCRYPTION_KEY', '{DATA_PROVIDER_3_ENCRYPTION_KEY}')")
        res=duckdb.sql(f"CREATE SECRET (TYPE S3,KEY_ID '{DATA_PROVIDER_3_KEY}',SECRET '{DATA_PROVIDER_3_SECRET}',REGION '{DATA_PROVIDER_3_REGION}');")
    
        parquet1="'"+DATA_PROVIDER_1_URL+"', encryption_config = {footer_key: 'DATA_PROVIDER_1_ENCRYPTION_KEY'}"
        #parquet2="'"+DATA_PROVIDER_2_SHARE_ACCESS_TOKEN+"', encryption_config = {footer_key: 'DATA_PROVIDER_2_ENCRYPTION_KEY'}"
        parquet2="'"+DATA_PROVIDER_2_URL+"', encryption_config = {footer_key: 'DATA_PROVIDER_2_ENCRYPTION_KEY'}"
        parquet3="'"+DATA_PROVIDER_3_URL+"', encryption_config = {footer_key: 'DATA_PROVIDER_3_ENCRYPTION_KEY'}"
        #query=f"SELECT * FROM read_parquet({parquet1}) UNION ALL SELECT * FROM read_parquet({parquet2}) UNION ALL SELECT * FROM read_parquet({parquet3})"
        query=f"SELECT * FROM read_parquet({parquet2})"
        
        try:
            os.environ['CURL_CA_PATH']="/usr/local/lib/python3.10/site-packages/certifi"
            os.environ['CURL_CA_INFO']="/usr/local/lib/python3.10/site-packages/certifi/cacert.pem"
            logger.info(certifi.where())
            res=duckdb.sql("CREATE TABLE localdb AS "+query) 
        except Exception as err:
            logger.info(f"Unexpected {err=}, {type(err)=}")
            
    output="" 
    logger.info(f"|                                                       |")
    logger.info(f"| 2. Calculate scoring                                  |")
    for x in range(len(accountsList)):
        accountId=base64.b64decode(accountsList[x])
        accountId = cipher.decrypt(accountId).decode("utf-8")
        #accountId=accountsList[x]
        muleOutput=find_mule_account(f"SELECT * from localdb WHERE account_id='{accountId}'")
        if muleOutput!="" :
            output=output+muleOutput+","
    if output!="" :
        output=output[:-1]
    collaborationOutput='''{
    "report_date": "'''+datetime.today().strftime('%Y-%m-%d %H:%M:%S')+'''",
    "accounts": [ '''+output+'''
    ]
    }
    '''

    logger.info(f"|                                                       |")
    logger.info(f"| 3. Send scoring report                                |")
    with open('/resources/outputs/scoring-report.json', 'w', newline='') as file:
        file.write(collaborationOutput)
    logger.info(f"|                                                       |")
    execution_time=(time.time() - start_time)
    logger.info(f"|    Execution time:  {execution_time} secs           |")
    logger.info(f"|                                                       |")
    logger.info(f"--------------------------------------------------------")

   
def find_mule_account(query):
    df = duckdb.sql(query).df()
    numberOfScoringParties=0
    combinedRiskScore=0
    redFlags=""
    accountHolderName=""
    account_id=""
    output=""
    for index, row in df.iterrows():
        account_id=row["account_id"]
        accountHolderName=accountHolderName+'"'+row["account_holder_name"]+'",'
        numberOfScoringParties=numberOfScoringParties+1
        redFlagsArray=json.loads(row["red_flag"])["red_flags"]
        for x in range(len(redFlagsArray)):
            redFlags=redFlags+str(redFlagsArray[x])+","
        combinedRiskScore=combinedRiskScore+row["risk_score"]
    combinedRiskScore=combinedRiskScore/3
    if account_id!= "":
        output='''
        {
                "account_id": "'''+account_id+'''",
                "account_holder_names": [
                    '''+accountHolderName[:-1]+'''
                ],
                "combined_risk_score": "'''+str(int(combinedRiskScore))+'''",
                "number_of_scoring_parties":"'''+str(numberOfScoringParties)+'''",
                "red_flags": [
                    '''+redFlags.replace("'","\"")[:-1]+'''
                ]
            }
        '''
    return output
    



if __name__ == "__main__":
    test_event = {
        "type": "SCORE",
        "accounts": [
            "GB82EVJA51473322705367"
        ]
    }

    test_event = {
        "type": "SCORE",
        "accounts": [
            "GB82EVJA51473322705367",
            "RO79OSQB3432547082039702",
            "DK311448088022695900"
        ]
    }

    test_event = {
        "type": "SCORE",
        "accounts": [
            "cM8WNktMbOgrDiAjma9JIyRYgple1inKhvEjv8EgsUwTXVYLWSaPmr1xMy8RH5cj80T8anOlNjXZmYC2uSGTtTUYGh+Kq0ZuJPsqniiUW6N+KOZ7/h1D2KDeSAxXw3VFQmjQCD+qq82vGYXphMPmrTqVky/P+uB/WGy0/RHhKCPBzyDnaIgSQojwxfUryUXQjrkiQYES7/2GZhvvwKLulSjJJnlQYt/0XWMb2Tr712HcpcPuJs15ZjkxJLMlrNSoYud3eSneQOQEitcXanwY/gyaOHJpArj3kSfQKB+TIGI6rYunnfn9Whz58wgFCwaMdTKba0gzBWpNbrYZawzi8A==",
            "xSEEz7qrxfKen9xUYfeLWErIbTkSW1fcYXflZDVsMEDYmCdVAa1fpBO9f6hfpTxE11IsIlTSxo1CJ/wLiu83HIM7/Be+lY45vkgMKp0hUpEo1BaXssPDiTPq6taQGKE1rmOLe19S+Iq5rGW+gPA7LFl9tL/7IqgdmFW2yZu9IpBW0Cq/5D4/GGzeFXlw7wqKJojDiaHbB8hy3mIE5Vay4QxlJpJZoyACDhDHnXBGihtRxFfv/3kgUY2K/CCpSEDBt81JEZQjSURVLUDA0wA/csIhndPZG1vqGZs7QtYhorhmea8HTD6cn6FtRrNi1ud2huUjnp3WHkh9krfBU8YGrQ==",
            "VlacnpQ/ifP1DfPQqIAk39VWcHydG9FRCtANscLCt9Z6UJ9XhUZOXrDse/fnVZpqd4SIJGXVaAIU5kVCVP0bdOdCpJ+wZEn97K3VoO9UKQqdSEYOHFw5jQlcTDcokfoODt8xdOb/cj4vJw9FfmuPInCvdj3g/iydgOpNkUEPegDTcnE5FdwKqsErNBwH/TUY1Z5Apw8ykqkbKMRdjgDwOU1DYy86jn/so5aHXZ5v7rMLnp6PpfUVaTvt4GTtaya83iRJRoIO0diBxbwKWx+/JTMbqmbMmvCHDyvewYq7kt+a4XUc0847vJHHdKQ2G7Ydc3oaeZ1EHwj4alg8ck5x3Q=="
   ]
    }


    process_score_event(test_event)