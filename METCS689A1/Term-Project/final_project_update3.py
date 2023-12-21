import pandas as pd

############################################################# Part 7 ##############################################################

# load Customer.csv 
print("======================= Load Customer.csv =======================")
Cust_file = 'I:\WindowsG备份\BU_STUDY\METCS689A1\FP\Customer.csv'
df_cust = pd.read_csv(Cust_file)
print(df_cust.head())
print()

# # load Location.csv
# print("======================= Load Location.csv =======================")
# Loc_file = 'G:\BU_STUDY\METCS689A1\FP\Location.csv'
# df_Loc = pd.read_csv(Loc_file, keep_default_na=False)
# print(df_Loc.head())
# print()

# # load OrderDim.csv
# print("======================= Load OrderDim.csv =======================")
# od_file = 'G:\BU_STUDY\METCS689A1\FP\OrderDim.csv'
# df_od = pd.read_csv(od_file)
# print(df_od.head())
# print()

# # load product.csv
# print("======================= Load product.csv =======================")
# product_file = 'G:\BU_STUDY\METCS689A1\FP\product.csv'
# df_product = pd.read_csv(product_file)
# print(df_product.head())
# print()

# # load time_monthly.csv
# print("======================= Load time_monthly.csv =======================")
# tm_file = r'G:\BU_STUDY\METCS689A1\FP\time_monthly.csv'
# df_tm = pd.read_csv(tm_file)
# print(df_tm.head())
# print()

# # load time_daily.csv
# print("======================= Load time_daily.csv =======================")
# td_file = r'G:\BU_STUDY\METCS689A1\FP\time_daily.csv'
# df_td = pd.read_csv(td_file)
# print(df_td.head())
# print()

# # load SalesFact.csv
# print("======================= Load SalesFact.csv =======================")
# sf_file = 'G:\BU_STUDY\METCS689A1\FP\SalesFact.csv'
# df_sf = pd.read_csv(sf_file)
# print(df_sf.head())
# print()

# # load ProductPerformanceFact.csv
# print("======================= Load ProductPerformanceFact.csv =======================")
# ppf_file = 'G:\BU_STUDY\METCS689A1\FP\ProductPerformanceFact.csv'
# df_ppf = pd.read_csv(ppf_file)
# print(df_ppf.head())
# print()

# # load CustomerInteractionFact.csv
# print("======================= Load CustomerInteractionFact.csv =======================")
# CustomerInteractionFact_file = 'G:\BU_STUDY\METCS689A1\FP\CustomerInteractionFact.csv'
# df_CustomerInteractionFact = pd.read_csv(CustomerInteractionFact_file)
# print(df_CustomerInteractionFact.head())
# print()

# # load CustomerPurchaseBehaviorFact.csv
# print("======================= Load CustomerPurchaseBehaviorFact.csv =======================")
# cpbf_file = 'G:\BU_STUDY\METCS689A1\FP\CustomerPurchaseBehaviorFact.csv'
# df_cpbf = pd.read_csv(cpbf_file, keep_default_na=False)
# print(df_cpbf.head())
# print()


############################################################# Part 8 ##############################################################

# Transformation for Customer.csv
print("--------------------------------------------- Transformation for Customer.csv --------------------------------------------")
age_aggregates = df_cust['age'].describe()
print(age_aggregates)
df_cust=df_cust.drop_duplicates()
print(df_cust.head())

# # Transformation for Location.csv
# print("--------------------------------------------- Transformation for Location.csv --------------------------------------------")
# df_Location=df_Loc.drop_duplicates()
# print(df_Location.head())

# # Transformation for time_monthly.csv
# print("--------------------------------------------- Transformation for time_monthly.csv --------------------------------------------")
# df_tm=df_tm.drop_duplicates()
# print(df_tm.head())

# # Transformation for time_daily.csv
# print("--------------------------------------------- Transformation for time_daily.csv --------------------------------------------")
# df_td=df_td.drop_duplicates()
# print(df_td.head())

# # Transformation for ProductPerformanceFact.csv
# print("--------------------------------------------- Transformation for SalesFact.csv --------------------------------------------")
# sf_aggregates_qtrorder = df_sf['qty_ordered'].describe()
# print(sf_aggregates_qtrorder)
# print()
# sf_aggregates_p = df_sf['price'].describe()
# print(sf_aggregates_p)
# print()
# sf_aggregates_value = df_sf['value'].describe()
# print(sf_aggregates_value)
# print()
# sf_aggregates_dm = df_sf['discount_amount'].describe()
# print(sf_aggregates_dm)
# print()
# sf_aggregates_total = df_sf['total'].describe()
# print(sf_aggregates_total)

# # Transformation for ProductPerformanceFact.csv
# print("--------------------------------------------- Transformation for ProductPerformanceFact.csv --------------------------------------------")
# ppf_aggregates_qtrorder = df_ppf['qty_ordered'].describe()
# print(ppf_aggregates_qtrorder)
# print()
# ppf_aggregates_totalRev = df_ppf['Revenue_Total'].describe()
# print(ppf_aggregates_totalRev)

# # Transformation for CustomerInteractionFact.csv
# print("--------------------------------------------- Transformation for CustomerInteractionFact.csv --------------------------------------------")
# CustomerInteractionFact_aggregates_ts = df_CustomerInteractionFact['Time_Spent'].describe()
# print(CustomerInteractionFact_aggregates_ts)
# print()
# CustomerInteractionFact_aggregates_NumPurchase = df_CustomerInteractionFact['N_Purchases'].describe()
# print(CustomerInteractionFact_aggregates_NumPurchase)

############################################################## Part 9 ##############################################################
import pyodbc as db

conn = db.connect('Driver={SQL Server};'
                'Server=XUYUHAN;'
               'Server=SQLServer-PC;'
               'Database=689fp;'
                'Trusted_Connection=yes;'
                'Connection Timeout=3000')

cursor = conn.cursor()


print("...................................... Load data into Table Customer ......................................")
insert_query = """
INSERT INTO Customer (Cust_id,Name_Prefix,First_Name,Middle_Initial,Last_Name,
                        Gender,age,full_name,E_mail,Customer_Since,
                        SSN,Phone_No,PlaceName,County,City,
                        State,Zip,Region,User_Name)
VALUES (?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?)
"""
for index, row in df_cust.iterrows():
    Key = index + 1
    cursor.execute(insert_query, row['cust_id'],row['Name_Prefix'],row['First_Name'],row['Middle_Initial'],row['Last_Name'],
                   row['Gender'],row['age'],row['full_name'],row['E_mail'],row['Customer_Since'],
                   row['SSN'],row['Phone_No'],row['PlaceName'],row['County'],row['City'],
                   row['State'],row['Zip'],row['Region'],row['User_Name'])
conn.commit()


# print("...................................... Load data into Table Time_Monthly ......................................")
# insert_query1 = """
# INSERT INTO  Time_Monthly (MonthKey,year,month)
# VALUES (?,?,?)
# """
# for index, row in df_tm.iterrows():
#     Key = index + 1
#     cursor.execute(insert_query1, row['MonthKey'],row['year'],row['month'])
# conn.commit()


# print("...................................... Load data into Table Time_daily ......................................")
# insert_query2 = """
# INSERT INTO  Time_daily (DateKey,Year,Quarter,Month,Day)
# VALUES (?,?,?,?,?)
# """
# for index, row in df_td.iterrows():
#     Key = index + 1
#     cursor.execute(insert_query2, row['DateKey'],row['Year'],row['Quarter'],row['Month'],row['Day'])
# conn.commit()


# print("...................................... Load data into Table Product ......................................")
# insert_query3 = """
# INSERT INTO  Product (Product_id,SKU,Description,Category)
# VALUES (?,?,?,?)
# """
# for index, row in df_product.iterrows():
#     Key = index + 1
#     cursor.execute(insert_query3, row['Product_id'],row['sku'],row['Description'],row['category'])
# conn.commit()


# print("...................................... Load data into Table OrderDim ......................................")
# insert_query4 = """
# INSERT INTO  OrderDim (Oid,OrderID,InvoiceNo,Status,LastUpdated)
# VALUES (?,?,?,?,?)
# """
# for index, row in df_od.iterrows():
#     Key = index + 1
#     cursor.execute(insert_query4, row['Oid'], row['OrderID'],row['InvoiceNo'],row['status'],row['LastUpdated'])
# conn.commit()


# print("...................................... Load data into Table Location ......................................")
# insert_query5 = """
# INSERT INTO Location (LocationID,Place_Name,County,City,State,
#                         PreviousCity,PreviousState,Zip,Region,Country)
# VALUES (?,?,?,?,?,
#         ?,?,?,?,?)
# """
# for index, row in df_Location.iterrows():
#     Key = index + 1
#     cursor.execute(insert_query5, row['LocationID'],row['Place Name'],row['County'],row['City'],row['State'],
#                    row['PreviousCity'],row['PreviousState'],row['Zip'],row['Region'],row['Country'])
# conn.commit()


# print("...................................... Load data into Table ProductPerformanceFact ......................................")
# insert_query6 = """
# INSERT INTO  ProductPerformanceFact (PerformanceID,OrderID,Product_id,MonthKey,qty_ordered,Revenue_Total)
# VALUES (?,?,?,?,?,?)
# """
# for index, row in df_ppf.iterrows():
#     Key = index + 1
#     cursor.execute(insert_query6, row['PerformanceID'],row['OrderID'],row['Product_id'],row['MonthKey'],row['qty_ordered'],row['Revenue_Total'])
# conn.commit()


# print("...................................... Load data into Table SalesFact ......................................")
# insert_query7 = """
# INSERT INTO SalesFact (SalesID,OrderID,cust_id,Product_id,LocationID,
#                         DateKey,qty_ordered,price,value,discount_amount,total)
# VALUES (?,?,?,?,?,
#         ?,?,?,?,?,?)
# """
# for index, row in df_sf.iterrows():
#     Key = index + 1
#     cursor.execute(insert_query7, row['SalesID'],row['OrderID'],row['cust_id'],row['Product_id'],row['LocationID'],
#                    row['DateKey'],row['qty_ordered'],row['price'],row['value'],row['discount_amount'],
#                    row['total'])
# conn.commit()


# print("...................................... Load data into Table CustomerInteractionFact ......................................")
# insert_query8 = """
# INSERT INTO CustomerInteractionFact (InteractionID,OrderID,cust_id,MonthKey,TimeSpent,N_Purchases)
# VALUES (?,?,?,?,?,?)
# """
# for index, row in df_CustomerInteractionFact.iterrows():
#     Key = index + 1
#     cursor.execute(insert_query8, int(row['InteractionID']),int(row['OrderID']),int(row['cust_id']),int(row['MonthKey']),int(row['Time_Spent']),int(row['N_Purchases']))
# conn.commit()


# print("...................................... Load data into Table CustomerPurchaseBehaviorFact ......................................")
# insert_query9 = """
# INSERT INTO CustomerPurchaseBehaviorFact (BehaviorFactID,cust_id,Product_id,DateKey,Revenue_Total,N_Purchases,Purchase_VALUE,Pay_Method)
# VALUES (?,?,?,?,
#         ?,?,?,?)
# """
# for index, row in df_cpbf.iterrows():
#     Key = index + 1
#     cursor.execute(insert_query9, row['BehaviorFactID'],row['cust_id'],row['Product_id'],row['DateKey'],row['Revenue_Total'],row['N_Purchases'],
#                    row['Purchase_VALUE'],row['Pay_Method'])
# conn.commit()

cursor.close()
conn.close()