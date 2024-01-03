# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.
# CHANGE SETTINGS FOR ANACONDA IN SETTINGS
# CHANGE THE SESSION INFORMATION IN THE TOP OF THE WORKSHEET WITH THE SCROLL OPTIONS

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import pandas as pd
#import numpy as np 

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    #database_name = 'FROSTBYTE_TASTY_BYTES'
    table_name = 'COUNTRY'

    dataFrame = session.table(table_name).filter(col("COUNTRY") == "Germany")

    # Convert the Snowflake DataFrame to a Pandas DataFrame
    pandas_df = dataFrame.toPandas()

    # Retrieve the first 10 rows
    first_10_rows = pandas_df.head(10)

    # Convert the Pandas DataFrame back to a Snowpark DataFrame
    snowpark_df = session.createDataFrame(first_10_rows)

    # Return the Snowpark DataFrame
    return snowpark_df
