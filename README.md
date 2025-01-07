***Project Overview***

This project is centered on reviewing sales data and producing a summary of information such as a list of products that has had the highest quantity of sales as well as their total sales values. The additions in the script are data cleaning, transformation, and preparation to create a preliminary recommendation based on the sales data.

***Data Input***

The script operates with a CSV file with sales data. 

Csv contain below columns 

Item (or equivalent): Identifies the product.

Qty. Sold (or equivalent): Quantity of the product sold.

Total Revenue (or equivalent): Revenue generated by the product.

***Script Workflow***

***Column Renaming***:
Adapts the column names to obtain expected names when alignment is necessary.

***Data Type Conversion***:
Coverts ITEM CODE and Quantity Sold to string.



***Top 10 Products by Quantity***: The collected data is grouped by ITEM CODE, the Quantity Sold is summed up and the first ten products in the list are chosen.

***Top 10 Products by Revenue***: we group the data by ITEM CODE, calculate the sum of the amount, and select the top ten products.

