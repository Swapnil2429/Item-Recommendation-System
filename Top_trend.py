# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/itemsales__1_.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
sparkdf = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

df = sparkdf.toPandas()

# COMMAND ----------



# Print column names to check if they match expectations
print(df.columns)

# Rename columns if necessary (replace with your actual column names)
df = df.rename(columns={'Item': 'ITEM CODE',
'Qty. Sold': 'Quantity Sold',
'Total Revenue': 'Amount'})
print(df.columns)

# COMMAND ----------

df.dtypes

# COMMAND ----------

df['ITEM CODE'] = df['ITEM CODE'].astype(str)  # Convert ITEM CODE to string
df['Quantity Sold'] = pd.to_numeric(df['Quantity Sold'], errors='coerce')  # Convert Quantity Sold to numeric
df['Amount'] = df['Amount'].replace({',': ''}, regex=True).astype(float)


# COMMAND ----------

df.dtypes

# COMMAND ----------



# Calculate Total Revenue per product
df['Total Revenue'] = df['Quantity Sold'] * df['Amount']

# Group by ITEM CODE and sum Total Revenue, then sort by Total Revenue
top_10_revenue = df.groupby('ITEM CODE').agg({'Total Revenue': 'sum'}).reset_index()

# Sort the values to get the top 10 products by Total Revenue
top_10_revenue = top_10_revenue.sort_values(by='Total Revenue', ascending=False).head(10)

# Print the top 10 products by revenue
print(top_10_revenue)

# Plot the data
plt.figure(figsize=(10, 6))
sns.barplot(data=top_10_revenue, x='ITEM CODE', y='Total Revenue', palette='viridis')

# Add titles and labels
plt.title('Top 10 Products by Revenue')
plt.xlabel('ITEM CODE')
plt.ylabel('Total Revenue')

# Rotate x-axis labels for better visibility
plt.xticks(rotation=45)

# Adjust layout
plt.tight_layout()

# Show the plot
plt.show()

# COMMAND ----------


top_10_quantity = df.groupby('ITEM CODE').agg({'Quantity Sold': 'sum'}).reset_index()

# Sort the products by Quantity Sold in descending order and get the top 10
top_10_quantity = top_10_quantity.sort_values(by='Quantity Sold', ascending=False).head(10)

# Print the top 10 products by quantity sold (for verification)
print(top_10_quantity)

# Plot the Top 10 products by quantity sold
plt.figure(figsize=(10, 6))
sns.barplot(data=top_10_quantity, x='ITEM CODE', y='Quantity Sold', palette='viridis')

# Add titles and labels
plt.title('Top 10 Products by Quantity Sold')
plt.xlabel('ITEM CODE')
plt.ylabel('Quantity Sold')

# Rotate x-axis labels for better readability
plt.xticks(rotation=45)

# Adjust layout for better visibility
plt.tight_layout()

# Show the plot
plt.show()
