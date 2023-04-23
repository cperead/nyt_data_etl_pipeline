#!/bin/bash

# Debug: set -x
# set +x disable mode of the shell where all executed commands are printed to the terminal.
set +x

# This script retrieves data from the NYT Archive API for every month 
# and saves the result of each request to a separate JSON file.


# Gets variables from configuration file
# INPUT_DATA_DIR, NYT_API_KEY, START_YEAR, END_YEAR, START_MONTH, END_MONTH
source /etl/config_vars.py


# Get the current date
current_year=$(date +'%Y')
current_month=$(date +'%-m')


# Check year
if [ "$START_YEAR" -gt "$END_YEAR" ]; then
    echo ">>> Not allowed: Start Year ${START_YEAR} is greater than End Year ${END_YEAR}"
    exit 1 
fi

# Check month
if [ "$START_MONTH" -gt "$END_MONTH" ]; then
    echo ">>> Not allowed: Start Month ${START_YEAR} is greater than End Month ${END_YEAR}"
    exit 1 
fi

# Retrieve the .json files, one per month
for (( year=$START_YEAR; year<=$END_YEAR; year++ ))
do
  for (( month=$START_MONTH; month<=$END_MONTH; month++))
  do
    # Run until one month before current date if needed
    if [[ $year == $current_year && $month == $current_month ]]
    then
      break
    fi

    # Request API
    data=$(curl -X GET https://api.nytimes.com/svc/archive/v1/$year/$month.json?api-key=$NYT_API_KEY -s)

    # Save into a new .json file
    echo $data > "../$INPUT_DATA_DIR/NYT_$((year))_$((month)).json"

    # Wait for 12 seconds
    # There are two rate limits per API. 
    # 500 requests per day and 5 requests per minute.
    sleep 12

  done
done
