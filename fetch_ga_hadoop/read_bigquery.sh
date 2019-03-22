#!/bin/bash

set -e
echo "$PWD"
echo "running python"

exec_filepath="$1"
database="$2"
tablename="$3"
bq_credential_filename="$4"
sql_input_file="$5"
out_filename="$6"
filepath2="$2/$3"
exec_filename="fetch_bigquery.py"


#hadoop fs -mkdir "$filepath2"
hadoop fs -get "$exec_filepath/$exec_filename" "$PWD"
hadoop fs -get "$exec_filepath/$sql_input_file" "$PWD"

bash vPython.sh -run $exec_filepath $exec_filename "$PWD" "$bq_credential_filename" "$sql_input_file" "$out_filename"
echo "python ran"

hadoop fs -put -f "$PWD/$out_filename" "$filepath2"

