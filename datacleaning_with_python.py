import numpy as np 
import pandas as pd  
import matplotlib.pyplot as plt 
import seaborn as sns 
#Importing the datasets 
df1 = pd.read_csv(r"C:\Users\HP\Downloads\bronze.crm_cust_info.csv") 
print(df1) 
#An quick overview of the data 
print(df1.info()) 
print('+===================')  
#To get the statistical overview of the data
print(df1.describe()) 
print('=====================') 
#To get the shape of the data set 
print(df1.shape) 
print('======================')
# To get the column names in the form of list 
print(df1.columns.to_list()) 
print('==================') 
#To get the datatypes of the data set 
print(df1.dtypes) 
print('=====================') 
#Exploring the nulls in each column 
print(df1.isna().sum()) 
print('============================') 
print('HANDLING THE NULL VALUES IN EACH COLUMN') 
#Handling nulls in the cst_id 
df1['cst_id'] = df1['cst_id'].fillna(df1['cst_key'])  
#Handling the nulls in the gender column 
df1['cst_gndr'] = df1['cst_gndr'].fillna('Unknown') 
#Handling the nulls in the cst_marital_status 
df1['cst_marital_status'] = df1['cst_marital_status'].fillna('Not supported') 
# Creating the new_column using the firstname ans lastname columns 
df1["full_name"] = df1["cst_firstname"].fillna("") + " " + df1["cst_lastname"].fillna("")
df1["full_name"] = df1["full_name"].str.strip()
# Dropping the cst_firstname and cst_lastname 
df1=df1.drop(['cst_firstname','cst_lastname'],axis=1) 
#Handling the nulls in the cst_create_date 
df1['cst_create_date']=pd.to_datetime(df1['cst_create_date'])  
df1['cst_create_date'] = df1['cst_create_date'].fillna(method='ffill') 
#Now checking the count of the nulls in each colums 
print(df1.isna().sum()) 
print('=================================================')  
print('CHECKING FOR THE DUPLICATES IN THE DATASET') 
print(df1.duplicated().sum()) 
#The above result tells us there are no duplicates in the dataset 
print('=================================================') 
print('NOW CHECKING THE QUALITY OF THE DATA') 
k=df1['cst_gndr']==df1['cst_gndr'].str.strip() 
print(k.sum()==len(df1)) 
f = df1['cst_marital_status'] == df1['cst_marital_status'] 
print(f.sum()==len(df1)) 
l = df1['full_name']==df1['full_name'].str.strip() 
print(l.sum()==len(df1)) 
m = df1['cst_key']==df1['cst_key'].str.strip() 
print(m.sum()==len(df1))  
print(df1['cst_marital_status'])
print('============================================') 
df1['cst_gndr'] = df1['cst_gndr'].replace({'M':'Male','F':'Female'}) 
df1['cst_marital_status'] = df1['cst_marital_status'].replace({'M':'Married','S':'Single'}) 
print(df1['cst_marital_status'])  
print(df1.columns.to_list()) 
df1.rename(columns={'cst_id':'customer_id','cst_key': 'customer_key','cst_marital_status':'marital_status','cst_gndr':'gender','cst_create_date':'Create_Date'},inplace=True)
print(df1)
print('=============================================================')
#Load another table as df2 for cleaning 
df2 = pd.read_csv(r"C:\Users\HP\Downloads\bronze.crm_prd_info.csv") 
# For Quick overview of the data 
print(df2.head(10)) 
print(df2.info()) 
print(df2.describe()) 
print(df2.shape) 
print(df2.columns.to_list()) 
print(df2.dtypes) 
print('================================')  
#To check the nulls in the each column
print(df2.isna().sum()) 
# handling the nulls in the cost column 
df2['prd_cost'] = df2['prd_cost'].fillna(0) 
# Handling the nulls in the prd_line column
df2['prd_line'] = df2['prd_line'].str.strip()  # remove spaces
df2['prd_line'] = df2['prd_line'].replace({
    'R': 'Road',
    'r': 'Road',
    'S': 'other_sales',
    'm': 'Mountain',
    'M': 'Mountain',
    't': 'Touring',
    'T': 'Touring'
})
print(df2.duplicated().sum())
df2.drop('prd_end_dt',axis=1,inplace=True)  
#Renaming the columns 
df2.rename(columns={'prd_id':'product_id','prd_key':'product_key','prd_cost':'cost','prd_nm':'product_name','prd_line':'product_line','prd_start_date': 'start_date'},inplace=True) 
df2['category_id'] = df2['product_key'].str[:5] 
df2['product_number'] = df2['product_key'].str[5:] 
df2['product_number'] = df2['product_number'].str.lstrip('-')
df2.drop('product_key',axis=1,inplace=True) 
print(df2) 
df2['product_key'] = range(1,len(df2)+1) 
print(df2) 
clos = ['product_key','product_id','product_number','product_name','category_id','product_line','start_date']
# keep only the columns that actually exist in df2
clos_existing = [col for col in clos if col in df2.columns]
# rearrange
df2 = df2[clos_existing]
print(df2)
print('============================================================') 
#LOADING THE NEW DATA SEt 
df = pd.read_csv(r"C:\Users\HP\Downloads\bronze.crm_sales_details.csv") 
#Quick overview of the data 
# to see the first 5 rows
print(df.head()) 
#it will give a clear picture of the data
print(df.info()) 
#it will give the statistical overview of the data
print(df.describe()) 
#it will give the shape of the data (rows,columns)
print(df.shape) 
# it will give the datatypes of each column
print(df.dtypes)
#it will create a list of columns names
print(df.columns.tolist()) 
#to know the count of the null values in data set 
print(df.isna().sum()) 
# --- Convert date fields ---
def clean_date(col):
    # Convert to string, check length == 8, and exclude zeros
    return pd.to_datetime(
        df[col].astype(str).where((df[col].astype(str).str.len() == 8) & (df[col] != 0)),
        errors="coerce"  # Invalid values → NaT (NULL in SQL)
    )

df["sls_order_dt"] = clean_date("sls_order_dt")
df["sls_ship_dt"]  = clean_date("sls_ship_dt")
df["sls_due_dt"]   = clean_date("sls_due_dt")

# --- Recalculate sls_sales if missing/invalid ---
df["sls_sales"] = np.where(
    (df["sls_sales"].isna()) | 
    (df["sls_sales"] <= 0) | 
    (df["sls_sales"] != df["sls_quantity"] * df["sls_price"].abs()),
    df["sls_quantity"] * df["sls_price"].abs(),
    df["sls_sales"]
)
# --- Derive sls_price if invalid ---
df["sls_price"] = np.where(
    (df["sls_price"].isna()) | (df["sls_price"] <= 0),
    df["sls_sales"] / df["sls_quantity"].replace(0, np.nan),
    df["sls_price"]
)
# --- Final cleaned DataFrame ---
df.rename(columns={'sls_ord_num':'order_number','sls_prd_key':'product_key','sls_cust_id':'customer_id','sls_order_dt':'order_date','sls_ship_dt':'ship_date','sls_due_dt':'due_date','sls_sales':'sales','sls_quantity':'quantity','sls_price':'price'},inplace=True) 
print(df.isna().sum())
print(df.head()) 
df1.to_csv("cleaned_customer_data.csv", index=False)
df2.to_csv("cleaned_product_data.csv", index=False)
df.to_csv("cleaned_sales_data.csv", index=False) 



