# Databricks notebook source
# When Merge is not Adequate, capturing all history

"""
In some scenarios, using the merge operation is not sufficient to capture all history. The merge operation
is suitable for simple updates and inserts, but it doesn't handle scenarios where we need to track
all changes made to a record.

To capture all history, we can implement slowly changing dimensions (SCD) techniques:
1. Type 1 SCD: Overwrite existing values with new values, losing historical information.
2. Type 2 SCD: Create a new record with updated values, preserving historical information.
3. Type 3 SCD: Add columns to track historical changes, storing limited history.

These SCD techniques can be implemented in various ways using SQL and Python code depending on the use case
and requirements. By applying these techniques, we can effectively capture all history and provide traceable
data changes.
"""

# COMMAND ----------

import pandas as pd
import datetime

CALENDAR = ""
class Capture_Data_History:
    def __init__(self, new_data: pd.DataFrame, old_data_date: str, data_loc: str):
        self.new_data = new_data
        self.old_data_date = old_data_date
        self.data_loc = data_loc
    
    def _insert_date(self,):
        """ Inserts a new calendar entry based on the current time in EST
        >>> _insert_date()
        ......................
        """
        # Format new date to insert in the calendar
        new_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # get max day_id from the current calendar based on our current date
        dayid_q = (f"select max(day_id) as day_id from {CALENDAR} where date<='{new_dt}'")

        dayid_df = pd.DataFrame()

        try:
            dayid_df = spark.sql(dayid_q).toPandas()
        except:
            pass
        self.day_id = 0

        if len(dayid_df.index) > 0:
            self.day_id = dayid_df.day_id.max()

        # Set new dayid as old dayid + 1
        new_dayid = 1

        try:
            new_dayid = int(self.day_id) + 1
        except:
            pass

        if self.day_id == 0:
            # If table does not exist create it
            create_cal_q = f"create table {CALENDAR}(date date, day_id, int)"
            spark.sql(create_cal_q)

            # Insert first calendar entry into the new table
            ins_cal_q = f"INSERT INTO {CALENDAR} VALUES ('{new_dt}', {new_dayid});"

            spark.sql(ins_cal_q)
