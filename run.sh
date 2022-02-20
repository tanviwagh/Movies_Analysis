#!/bin/bash
echo -e "###RUN SCRIPT###"

file_name = $1

echo "File name is: $file_name"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"

`spark-submit --name $file_name `

spark_cmd_status = $?

if [ $(spark_cmd_status) -eq 0]
then
 echo "Job is success"
else
  echo "Job is failed"
fi