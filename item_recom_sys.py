import pandas as pd

# Replace 'your_sales_data.csv' with the actual file path
df = pd.read_csv('C:/Users/dell/Documents/Python/itemsales.csv')

# Print column names to check if they match expectations
print(df.columns)

# Rename columns if necessary (replace with your actual column names)
df = df.rename(columns={'Item': 'ITEM CODE',
'Qty. Sold': 'Quantity Sold',
'Total Revenue': 'Amount'})
print(df.columns)

# Convert data types (replace 'str' and 'int' with appropriate types for your data)
df['ITEM CODE'] = df['ITEM CODE'].astype(str)
df['Quantity Sold'] = df['Quantity Sold'].astype(str)
# df['Amount'] = df['Amount'].astype(int)  # Assuming quantity
df['Amount'] = df['Amount'].str.replace(",", "", regex=True)

# Now convert to integer
df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')

# Check for missing values
print(df.isnull().sum())  # This will display a Series showing missing values in each column

# Handle missing values (replace with your preferred method)
# Example: Remove rows with missing values
df.dropna(inplace=True)

# Print the DataFrame to see the cleaned data
print(df.head())  # This shows the first few rows

# Proceed with building your recommender system using the prepared DataFrame 'df'

# Group by item ID and sum the quantity
quantity_by_item = df.groupby('ITEM CODE')['Quantity Sold'].sum()

# Sort by quantity in descending order and get the top 10
top_10_quantity = quantity_by_item.sort_values(ascending=False).head(10)

# Print or use these results for recommendations
print("Top 10 Products by Quantity:")
print(top_10_quantity)


# Group by item ID and sum the total amount
amount_by_item = df.groupby('ITEM CODE')['Amount'].sum()

# Sort by amount in descending order and get the top 10
top_10_amount = amount_by_item.sort_values(ascending=False).head(10)

# Print or use these results for recommendations
print("\nTop 10 Products by Amount:")
print(top_10_amount)
