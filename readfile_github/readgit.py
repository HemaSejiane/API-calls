#!/usr/bin/env python3.5

import logging
import sys
import json
import requests
from data_vault import get_client,read_key
from hdfs.ext.kerberos import KerberosClient
from pyspark import SparkContext
from pyspark.sql import HiveContext


class SchemaValidationException(Exception):
    pass


def get_auth_from_vault(key, attribute, vault_key_jobrunner):
    logging.info('Get git credentials from vault')
    vault_client = get_client(vault_appid=vault_key_jobrunner)
    secret = json.loads(read_key(vault_client, key))
    auth = secret[attribute]
    return auth


def load_hdfs(destination, content_out, hdfs_url):
    logging.info('Writes file to hdfs')
    client = KerberosClient(hdfs_url)
    client.write(destination, content_out, overwrite=True)


def get_hive_schema(hive_client, HIVE_TABLE):
    logging.info('Fetch Hive table and return list of columns')
    hive_table = hive_client.table(HIVE_TABLE)
    schema = hive_table.schema
    schema_out=[i.name for i in schema.fields]
    return schema_out


def get_hdfs_data(spark_client, HDFS_FILE, header_flag):
    logging.info('Extracts csv data from github and hdfs')
    csv_file = spark_client.textFile(HDFS_FILE)
    if header_flag == 'Y':
        data_out=csv_file.first()
        data_out_header=data_out.encode('utf-8').decode().split(',')
        data_out_content=csv_file.filter(lambda line: line != data_out).collect()
        return data_out_header,data_out_content
    else:
        return csv_file.collect()


def schema_validator(hive_client, HIVE_TABLE, header):
    logging.info('Compares the column header from csv file and table')
    hive_columns = get_hive_schema(hive_client, HIVE_TABLE)
    csv_columns = header
    if hive_columns != csv_columns:
        raise SchemaValidationException('Hive columns DO NOT match CSV columns!')
    else:
        return True


def file_comparison(file1_data, file2_data):
    logging.info('Compares the file content from csv file and table')
    comparison_dataset= set(file1_data).difference(set(file2_data))
    if len(comparison_dataset) > 0:
        logging.info('Difference in file content')
        print('Difference in file content')
        return True
    else:
        logging.info('File matches')
        print('File matches')
        return False


def get_file_content_github():
    logging.info('Main function to invoke the file pull')
    vault_key_jobrunner= sys.argv[1]
    hdfs_url= sys.argv[2]
    git_url= sys.argv[3]
    HDFS_FILE = sys.argv[4]
    hdfs_temp_file= sys.argv[5]
    HIVE_TABLE = sys.argv[6]
    spark_client = SparkContext()
    hive_client = HiveContext(spark_client)
    
    token=get_auth_from_vault('gitpullToken','token',vault_key_jobrunner)
    head = {'Authorization': 'token {}'.format(token)}
    response = requests.get(git_url, headers=head)
    content_out=response.content
    load_hdfs(hdfs_temp_file,content_out,hdfs_url)
    git_header, git_data=get_hdfs_data(spark_client,hdfs_temp_file,'Y')
    hdfs_data=get_hdfs_data(spark_client,HDFS_FILE,'N')
    validation_check= schema_validator(hive_client,HIVE_TABLE,git_header)
    if validation_check:
        file_comparison_check= file_comparison(git_data,hdfs_data)
        if file_comparison_check:
            load_hdfs(HDFS_FILE,content_out,hdfs_url)


if __name__ == '__main__':
    get_file_content_github()
