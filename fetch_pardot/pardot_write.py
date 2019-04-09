from pypardot.client import PardotAPI
from pyspark import SparkContext, HiveContext
from pyspark.sql import HiveContext
import json
import logging
from pprint import pprint

def upsert_pardot(jsoninput):
    "Connect to Pardot and push json data"
    p = PardotAPI(email='',password='',user_key='')
    p.authenticate()
    p.prospects.batchUpsert(prospects=jsoninput)
    #read_pardot(p,'avvo_edw_test1@test_email.com')


def read_pardot(client,email):
    "Temporary function for testing"
    pprint(client.prospects.read_by_email(email=email))


def create_chunks(data):
    """ Chunks data into batches based on chunk size """
    chunk = []
    chunk_size = 50
    for idx, row in enumerate(data):
        if idx % chunk_size == 0 and idx > 0:
            yield chunk
            chunk = []
        chunk.append(row)
    yield chunk


def push_to_pardot(data):
    chunks = create_chunks(data)
    for chunk in chunks:
        inputdata = {"prospects": chunk}
        jsondata = json.dumps(inputdata)
        print(jsondata)
        upsert_pardot(jsondata)	


def readData():
    "Get data from Hive table and convert to json format"
    contactsDF = sqlContext.sql("""
        select query
	""")
    data = []
    rows = contactsDF.rdd.collect()
    for row in rows:
        data.append({row.email:{
            'Professional_ID':row.Professional_ID,
            'Claimed_Date':row.Claimed_Date,
            'Claimed_Profile_':row.Claimed_Profile_
        }})
    print(set(data))
    #print(json.dumps(data))
    #push_to_pardot(data)


def main():
	 # setup
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    readData()


if __name__ == '__main__':
    sc = SparkContext()
    sqlContext = HiveContext(sc)
    main()

