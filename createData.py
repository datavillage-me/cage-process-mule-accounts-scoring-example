import duckdb
import random
import os
from datetime import datetime

from duckdb.typing import *
from faker import Faker

# {
#     "accounts": [
#         {
#             "account_id": "string",
#             "account_holder_name": "string",
#             "risk_score": integer,
#             "risk_category": "string",
#             "red_flags": [
#                  "flagged_date": "date",
#                   "reason": "string"
#              ]
#         }
#     ]
# }



# Locales for Europe, the UK, and North America

locales = [
    # Europe
    'bg_BG',  # Bulgarian (Bulgaria)
    'cs_CZ',  # Czech (Czech Republic)
    'da_DK',  # Danish (Denmark)
    'de_AT',  # German (Austria)
    'de_CH',  # German (Switzerland)
    'de_DE',  # German (Germany)
    'el_GR',  # Greek (Greece)
    'en_IE',  # English (Ireland)
    'en_GB',  # English (United Kingdom)
    'es_ES',  # Spanish (Spain)
    'et_EE',  # Estonian (Estonia)
    'fi_FI',  # Finnish (Finland)
    'fr_BE',  # French (Belgium)
    'fr_FR',  # French (France)
    'fr_CH',  # French (Switzerland)
    'hr_HR',  # Croatian (Croatia)
    'hu_HU',  # Hungarian (Hungary)
    'it_IT',  # Italian (Italy)
    'lt_LT',  # Lithuanian (Lithuania)
    'lv_LV',  # Latvian (Latvia)
    'nl_BE',  # Dutch (Belgium)
    'nl_NL',  # Dutch (Netherlands)
    'no_NO',  # Norwegian (Norway)
    'pl_PL',  # Polish (Poland)
    'pt_PT',  # Portuguese (Portugal)
    'ro_RO',  # Romanian (Romania)
    'ru_RU',  # Russian (Russia)
    'sk_SK',  # Slovak (Slovakia)
    'sl_SI',  # Slovenian (Slovenia)
    'sv_SE',  # Swedish (Sweden)
    'uk_UA',  # Ukrainian (Ukraine)

    # UK
    'en_GB',  # English (United Kingdom)

    # North America
    'en_CA',  # English (Canada)
    'en_US',  # English (United States)
    'es_MX',  # Spanish (Mexico)
    'fr_CA',  # French (Canada)
]

currentLocale="fr_CA"

def random_iban(n):
    i=random.randrange(0, 36)
    currentLocale=locales[i]
    fake = Faker(currentLocale)
    fake.seed_instance(int(n*10))
    return fake.iban()

def random_name(n):
    i=random.randrange(0, 36)
    currentLocale=locales[i]
    fake = Faker(currentLocale)
    fake.seed_instance(int(n*10))
    return fake.name()

def random_risk_score(n):
    return random.randint(40,100) 

def random_red_flag(n):
    fake = Faker(int(n*10))
    redflag='{"red_flags": ['
    
    data = [
    "High volume of transactions in a short period",
    "Frequent international transfers, especially to high-risk countries",
    "Multiple accounts linked to the same holder",
    "Inconsistent transaction amounts",
    "Transactions to or from known suspicious entities",
    "Significant discrepancies in customer information",
    "Incomplete KYC (Know Your Customer) details",
    "Frequent changes in account holder details",
    "Transactions involving high-risk regions",
    "Unusual use of accounts, such as frequent deposits and withdrawals",
    "Multiple flagged accounts across institutions",
    "Transactions just below reporting thresholds",
    "Use of third-party accounts for transactions",
    "Dormant accounts suddenly becoming active",
    "Unusual patterns of transaction timing",
    "Round-number transactions",
    "Unexplained source of funds",
    "Frequent cash transactions",
    "Accounts used primarily for large wire transfers",
    "Transactions that don't match the account holder's profile",
    "Suspicious activities identified by machine learning models",
    "High-risk IP addresses or geolocations",
    "Linked to known criminal activities or entities",
    "Unusually high-risk scores from internal risk models"
    ]

    j=random.randrange(1, 4)
    for x in range(j):
        fakeDate=fake.date_between(start_date=datetime(2020,1,1))
        i=random.randrange(0, 24)
        redflag=redflag+'{"flagged_date": "'+str(fakeDate)+'", "reason": "'+data[i]+'"}'
        if x!=j-1:
            redflag=redflag+","
    redflag=redflag+"]}"

    return redflag

duckdb.create_function("iban", random_iban, [DOUBLE], VARCHAR)
duckdb.create_function("name", random_name, [DOUBLE], VARCHAR)
duckdb.create_function("riskscore", random_risk_score, [DOUBLE], INTEGER)
duckdb.create_function("redflag", random_red_flag, [DOUBLE], VARCHAR)

numberOfRecords=10000
numberOfDatasets=3
keys = ["GZs0DsMHdXr39mzkFwHwTHvCuUlID3HB","8SX9rT9VSHohHgEz2qRer5oCoid2RUAS","DrRLoOybRrUUANB9fkhHU9AZ7g4NKkMs"]
for x in range(numberOfDatasets):
    #res = duckdb.sql("COPY (SELECT iban(i) as account_id, name(i) as account_holder_name, riskscore(i) as risk_score,redflag(i) as red_flag FROM generate_series(1, "+str(numberOfRecords)+") s(i)) TO 'data/mule-accounts"+str(x)+".parquet'  (FORMAT 'parquet')")
    key = keys[x]
    keyName="dataset"+str(x)
    res=duckdb.sql("PRAGMA add_parquet_key('"+keyName+"','"+key+"')")
    #res=duckdb.sql("COPY (SELECT * FROM './data/mule-accounts"+str(x)+".parquet') TO './data/mule-accounts"+str(x)+"-encrypted.parquet' (ENCRYPTION_CONFIG {footer_key: '"+keyName+"'})")
    #res = duckdb.sql("SELECT * FROM read_parquet('data/mule-accounts"+str(x)+"-encrypted.parquet', encryption_config = {footer_key: '"+keyName+"'})")
    
df = duckdb.sql("SELECT * FROM read_parquet('data/mule-accounts0-encrypted.parquet', encryption_config = {footer_key: 'dataset0'}) AS dataset0 JOIN (SELECT * FROM read_parquet('data/mule-accounts1-encrypted.parquet', encryption_config = {footer_key: 'dataset1'})) AS dataset1 ON (dataset0.account_id=dataset1.account_id) JOIN (SELECT * FROM read_parquet('data/mule-accounts2-encrypted.parquet', encryption_config = {footer_key: 'dataset2'})) AS dataset2 ON (dataset1.account_id=dataset2.account_id)").df()
print (df)