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

#cryptodome
#from Crypto.PublicKey import RSA
#from Crypto.Cipher import PKCS1_OAEP

from dv_utils import default_settings, Client 
import duckdb
import pandas as pd



logger = logging.getLogger(__name__)

input_dir = "/resources/data"
#input_dir = "config"
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


    with open(input_dir+'/data-provider-3.json', 'r', newline='') as file:
        jsonFile = json.load(file)
        DATA_PROVIDER_3_URL=jsonFile["DATA_PROVIDER_URL"]
        DATA_PROVIDER_3_ENCRYPTION_KEY=jsonFile["DATA_PROVIDER_ENCRYPTION_KEY"]
        DATA_PROVIDER_3_KEY=jsonFile["DATA_PROVIDER_KEY"]
        DATA_PROVIDER_3_SECRET=jsonFile["DATA_PROVIDER_SECRET"]
        DATA_PROVIDER_3_REGION=jsonFile["DATA_PROVIDER_REGION"]

    #load private keys
    #with open (input_dir+"/key.pem", "r") as prv_file:
    #    privateKey=prv_file.read()
    #cipher = PKCS1_OAEP.new(privateKey)

    logger.info(f"| 1. Match data with data providers                     |")
    logger.info(f"|    {DATA_PROVIDER_1_URL} |")
    logger.info(f"|    {DATA_PROVIDER_2_URL}  |")
    logger.info(f"|    {DATA_PROVIDER_3_URL} |")

    logger.info("TEST 0")
    #dataset1
    res=duckdb.sql(f"PRAGMA add_parquet_key('DATA_PROVIDER_1_ENCRYPTION_KEY', '{DATA_PROVIDER_1_ENCRYPTION_KEY}')")
    res=duckdb.sql(f"CREATE SECRET (TYPE GCS,KEY_ID '{DATA_PROVIDER_1_KEY}',SECRET '{DATA_PROVIDER_1_SECRET}');")
    logger.info("TEST 1")

    #dataset2
    res=duckdb.sql(f"PRAGMA add_parquet_key('DATA_PROVIDER_2_ENCRYPTION_KEY', '{DATA_PROVIDER_2_ENCRYPTION_KEY}')")
    res=duckdb.sql(f"SET azure_storage_connection_string = '{DATA_PROVIDER_2_CONNECTION_KEY}'")
    #df = duckdb.sql("SELECT * FROM read_parquet('"+DATA_PROVIDER_2_URL+"', encryption_config = {footer_key: 'DATA_PROVIDER_2_ENCRYPTION_KEY'})").df()
    logger.info("TEST 2")
    #dataset3
    res=duckdb.sql(f"PRAGMA add_parquet_key('DATA_PROVIDER_3_ENCRYPTION_KEY', '{DATA_PROVIDER_3_ENCRYPTION_KEY}')")
    res=duckdb.sql(f"CREATE SECRET (TYPE S3,KEY_ID '{DATA_PROVIDER_3_KEY}',SECRET '{DATA_PROVIDER_3_SECRET}',REGION '{DATA_PROVIDER_3_REGION}');")
    #df = duckdb.sql("SELECT * FROM read_parquet('"+DATA_PROVIDER_3_URL+"', encryption_config = {footer_key: 'DATA_PROVIDER_3_ENCRYPTION_KEY'})").df()

    logger.info("TEST 3")
    accountsList= evt.get("accounts", "")
    logger.info("TEST 4")
    parquet1="'"+DATA_PROVIDER_1_URL+"', encryption_config = {footer_key: 'DATA_PROVIDER_1_ENCRYPTION_KEY'}"
    parquet2="'"+DATA_PROVIDER_2_URL+"', encryption_config = {footer_key: 'DATA_PROVIDER_2_ENCRYPTION_KEY'}"
    parquet3="'"+DATA_PROVIDER_3_URL+"', encryption_config = {footer_key: 'DATA_PROVIDER_3_ENCRYPTION_KEY'}"
    query=f"SELECT * FROM read_parquet({parquet1}) UNION ALL SELECT * FROM read_parquet({parquet2}) UNION ALL SELECT * FROM read_parquet({parquet3})"
    logger.info("TEST 5")
    res=duckdb.sql("CREATE TABLE localdb AS "+query) 
    logger.info("TEST 6")
    output="" 
    logger.info(f"|                                                       |")
    logger.info(f"| 2. Calculate scoring                                  |")
    for x in range(len(accountsList)):
        #accountId=base64.b64decode(accountsList[x])
        #accountId = cipher.decrypt(accountId).decode("utf-8")
        accountId=accountsList[x]
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
    print(collaborationOutput)
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
            "QNNDw0tOznVkK7s4uBL8e0D+UKL8QvzsplSj1GrbKwLoe78pStcczgkrEhhfn7gft8QTc2vexv3AamlVVXSfjw2zddyXQWWJARMiXWICAJ9BP82Ph4Hf+to+zcdLuu9CQqcfylNIgADTqnNj8rzuOuS3fnaTuLXrbcmpLUypVmLPNTef9s/tMQgKY68/ksYfOcOx3rT300xGpMihIGfkOTI6qfFHZecexWesFzqS3G7QZ2Sh4OfKH5kJQDvBPB1t78zHUHTLSnv54ZinJyd7EJoe/ylXphCqAqLeSLnWDIMBZ1/Su40Xko66e+sCdvAO6ocJ9RQgspFTqrlFh9DauA==",
            "aWlRsY35ks028GkbBWuCxtgR4VGc3N/YHwLbSg1wYDM6Q2qxfrUH/AoVIpvzHmOu7pxAxqXG1u6lwXxTrDB8hwwnkHqIApp9V+5p4XcFAgkX/QfDeIcmjQwRJImC0ZG5OfFdqkdyX9eKQtuDaSdVhp/wntLO4pDu2SWUtaJQrnqIU+/bHJHANyIvXnmAeBF6JGxjSFE4yN9Tce53PRDH3KqTLlgBgDcz+RZAoY5YgU64Hds6iRO/dcN54MSlwuRMwXZNaNBTfLFS8kNiD2APRvvyvBPZO41z89t8SlqlEkmK22Lp3c5up0HGBYmmXUSyOK6LJG4CsvgRdWNBfhqdjA==",
            "GS7IShwjX6+A3gIrN6vXdfed71gCVVRE0QHDhdyU3PK/FcxWv8qb0Y3y8+y4mPgl+VArIuM7/Q6er2iW+oGyZwhzlXuwrVDBRoAb/IHaGVv/eAAVTJdfVLG60H/bLO4gkXxIsQyoAbBiWfdo7M/6y5ClCsyJM5jsA0Li2DdtaCK1/Nsn9Rm7xMcDNlLE6yuNE9WCUembO0mdp+dwJ4TstVRbwbj0aQP48bSN/+yxIZ+4IOLDhH4D8TK3kzzwKSrtiP0kUSBBjyRBktpALMgP+7qruFNN4L1P598HM6UBFjuj6e5aQdN5T7CgW3F3ut5cPt3vx9Lo7GVCZbDJEp88Ag=="
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


    process_score_event(test_event)