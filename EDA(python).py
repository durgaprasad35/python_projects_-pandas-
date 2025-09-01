import pandas as pd   
import numpy as np 
import matplotlib.pyplot as plt 
import seaborn as sns
#Importing the data sets into the python envinorment
df = pd.read_csv(r"C:\Users\HP\Downloads\gold.dim_customers.csv") 
df2 = pd.read_csv(r"C:\Users\HP\Downloads\gold.dim_products.csv") 
df3 = pd.read_csv(r"C:\Users\HP\Downloads\gold.fact_sales.csv") 
print(df) 
print(df2) 
print(df3) 
#Creating the start schema for the  above three tables
# Join sales with customers
sales_customers = pd.merge(df3, df, on="customer_key", how="left") 
# Join the result with products
final_df = pd.merge(sales_customers, df2, on="product_key", how="left") 


#Total sales by the category 
total_sales_by_category=final_df.groupby('category')['sales_amount'].sum()
print(total_sales_by_category) 
#Total sales by the product or finding the best product 
total_sales_product = final_df.groupby('product_name')['sales_amount'].sum().reset_index() .sort_values(by='sales_amount',ascending=False)
print(total_sales_product) 
print(final_df.head()) 


#total_sales per year 
#Extracting the year from the order date 
final_df['order_date'] = pd.to_datetime(final_df['order_date'])
final_df['year'] = final_df['order_date'].dt.year
total_sales_per_year = final_df.groupby('year')['sales_amount'].sum().reset_index(name='Total_sales_per_year')
print(total_sales_per_year) 


#Total sales per month 
final_df['month'] = final_df['order_date'].dt.month 
total_sales_by_month=final_df.groupby('month')['sales_amount'].sum().reset_index(name='Total_sales_per_month') 
print(total_sales_by_month) 

#counting the customers per year 
print(final_df.groupby('year')['customer_key'].count().reset_index(name='Total_customers'))

#country wise sales 
print(final_df.groupby('country')['sales_amount'].sum().reset_index(name='Country_wise_sales6'))  
 
#Calculating the age from birthdate 
final_df['birthdate'] = pd.to_datetime(final_df['birthdate']) 
from datetime import date
today = date.today()
final_df['age'] = final_df['birthdate'].apply(
    lambda x: today.year - x.year - ((today.month, today.day) < (x.month, x.day))
)
print(final_df[['customer_key','first_name','last_name','age']]) 

#Total_products_per_year 
print(final_df.groupby('year')['product_key'].count().reset_index(name='Total_products_per_year')) 

#Total_products_month 
print(final_df.groupby('month')['product_key'].count().reset_index(name='Total_products_per_month')) 

print(final_df.columns.tolist()) 


#Creating the pivo table 

pivot_table=pd.pivot_table(final_df,values='sales_amount',index='category',columns='subcategory',aggfunc='sum') 

print(pivot_table)
