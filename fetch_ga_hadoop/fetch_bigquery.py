import csv
import os
import sys
from datetime import datetime, timedelta
from google.cloud import bigquery
from data_vault import get_client, read_key


def query_phone_contacts(start_date, end_date, sql_input_file):
    """ Generate phone contacts query using BigQuery Standard SQL """
    with open(sql_input_file,'r') as rd:
        query = rd.read()
        return query.format(start_date=start_date, end_date=end_date)


def data_vault_read(bq_credential_filename):
    vault_client = get_client(vault_appid=os.environ.get('VAULT_KEY_JOBRUNNER'))
    read_token = read_key(vault_client, 'bigquery')
    with open(bq_credential_filename, 'w') as fh:
        fh.write(read_token)


def get_phone_contacts(bq_client, query):
    """ Fetch phone contacts from GA sessions in BigQuery """
    query_job = bq_client.query(query, location='US')
    return query_job.result()


def write_phone_contacts(data, filename):
    """ Write BigQuery results to CSV file """
    with open(filename, 'w') as fh:
        csv_writer = csv.writer(fh)
        for row in data:
            csv_writer.writerow(row.values())
    print('Total rows written to {}: {}'.format(filename, data.total_rows))


def main(): 
    # set variables
    FILE_DIRECTORY = sys.argv[1]
    bq_credential_filename = FILE_DIRECTORY+'/'+sys.argv[2]
    sql_input_file = FILE_DIRECTORY+'/'+sys.argv[3]
    filename = FILE_DIRECTORY+'/'+sys.argv[4] 
    data_vault_read(bq_credential_filename)
    bq_client = bigquery.Client.from_service_account_json(bq_credential_filename)
    yesterday = datetime.today() - timedelta(days=1)
    start_date = yesterday.strftime('%Y%m%d')
    # start_date = '20190315'
    # end_date = '20190315'
    end_date = yesterday.strftime('%Y%m%d')

    # function calls
    phone_contacts_query = query_phone_contacts(start_date, end_date, sql_input_file)
    phone_contacts_data = get_phone_contacts(bq_client, phone_contacts_query)
    write_phone_contacts(phone_contacts_data, filename)


if __name__ == '__main__':
    main()

