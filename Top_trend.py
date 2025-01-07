
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# File location and type
file_location = "/FileStore/tables/itemsales__1_.csv"
file_type = "csv"


infer_schema = "false"
first_row_is_header = "True"
delimiter = ","


  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

#converting spark dataframe in pandas dataframe

df = sparkdf.toPandas()



# Print column names for verification 
print(df.columns)

#rename column to achive consitency 
df = df.rename(columns={'Item': 'ITEM CODE',
'Qty. Sold': 'Quantity Sold',
'Total Revenue': 'Amount'})
print(df.columns)

# check data types 
df.dtypes
# converting in to proper data types 
df['ITEM CODE'] = df['ITEM CODE'].astype(str)  # Convert ITEM CODE to string
df['Quantity Sold'] = pd.to_numeric(df['Quantity Sold'], errors='coerce')  # Convert Quantity Sold to numeric
df['Amount'] = df['Amount'].replace({',': ''}, regex=True).astype(float)

#verifying data types after conversion 
df.dtypes

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

#adding labels to plot 

plt.title('Top 10 Products by Revenue')
plt.xlabel('ITEM CODE')
plt.ylabel('Total Revenue')

# Rotate x-axis labels for better visibility
plt.xticks(rotation=45)

# Adjust layout
plt.tight_layout()

# Show the plot
plt.show()

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
