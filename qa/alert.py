#!/usr/bin/env python3
"""
QA Alert Script for Snowflake Data Validation

This script connects to Snowflake, retrieves the latest QA daily summary data,
and performs validation checks on the data. It's designed to be run in Jenkins
and integrated with a Teams connector for alerting.

Validation checks:
1. Verify the date is yesterday
2. Check if all expected combinations are present (16 columns)
3. Ensure row/event ratio is approximately 1
"""

import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
import os
import sys
from dotenv import load_dotenv
from typing import Optional, Tuple, Dict, Any, List

# Load environment variables
load_dotenv()

# SQL query
QUERY_SNOWFLAKE = """
    SELECT * FROM MYDATASPACE.A208946_REUTERS_ANALYTICS.QA_DAILY_SUMMARY_NONPROD
    WHERE date = (SELECT MAX(date) FROM MYDATASPACE.A208946_REUTERS_ANALYTICS.QA_DAILY_SUMMARY_NONPROD);"""

def get_snowflake_data():
    """
    Get data from Snowflake database
    
    Returns:
        DataFrame with query results
    
    Raises:
        Exception: If there's an error connecting to Snowflake or executing the query
    """
    try:
        # Connect to Snowflake
        con = snowflake.connector.connect(
            user="joonkyung.kim@thomsonreuters.com",
            account="THOMSONREUTERS-A206448_PROD",
            authenticator="externalbrowser",
        )
        
        # Set up cursor and execute query
        cur = con.cursor()
        cur.execute("""USE ROLE A208946_REUTERS_ANALYTICS_MDS_READ_WRITE""")
        cur.execute("""USE WAREHOUSE A208946_REUTERS_ANALYTICS_MDS_WH""")
        cur.execute(QUERY_SNOWFLAKE)
        
        # Fetch results
        df = cur.fetch_pandas_all()
        
        # Close cursor and connection
        cur.close()
        con.close()
        
        return df
    
    except Exception as e:
        print(f"Error connecting to Snowflake or executing query: {e}")
        sys.exit(1)

def df_check(df):
    """
    Perform validation checks on the retrieved data
    
    Args:
        df: DataFrame with query results
    
    Returns:
        bool: True if any check fails, False if all checks pass
    """
    flag = False
    issues = []

    # 1. Check if the max date is not yesterday
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    unique_values = df['DATE'].unique()[0].strftime('%Y-%m-%d')
    
    if unique_values != yesterday_str:
        flag = True
        issues.append(f"Date check failed: Expected {yesterday_str}, got {unique_values}")
    
    # 2. Check if the expected number of columns is present
    if df.shape[1] != 16:
        flag = True
        issues.append(f"Column count check failed: Expected 16, got {df.shape[1]}")
    
    # 3. Check if the row/event ratio is approximately 1
    max_value = df['ROW_EVENT_RATIO'].max()
    min_value = df['ROW_EVENT_RATIO'].min()
    
    if max_value > 1.00001 or min_value < 0.99999:
        flag = True
        issues.append(f"Row/Event ratio check failed: Values outside acceptable range (min={min_value}, max={max_value})")
    
    # Print issues for logging/alerting
    if issues:
        print("Validation checks failed:")
        for issue in issues:
            print(f"- {issue}")
    else:
        print("All validation checks passed")
    
    return flag

def main():
    """
    Main function to execute the script
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    try:
        # Get data from Snowflake
        print("Connecting to Snowflake and retrieving data...")
        df = get_snowflake_data()
        
        if df.empty:
            print("No data retrieved from Snowflake")
            return 1
        
        # Perform validation checks
        print("Performing validation checks...")
        flag = df_check(df)
        
        if flag:
            print("Alert: One or more validation checks failed")
            return 1
        else:
            print("Success: All validation checks passed")
            return 0
    
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
