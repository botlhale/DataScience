# DataExplore
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
