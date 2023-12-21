import pandas as pd
import pyodbc as db

conn = db.connect('Driver={SQL Server};'
                'Server=XUYUHAN;'
               'Server=SQLServer-PC;'
               'Database=689fp;'
                'Trusted_Connection=yes;'
                'Connection Timeout=3000')

cursor = conn.cursor()


print("======================= Load CustomerPurchaseBehaviorFact.csv =======================")
cpbf_file = 'G:\BU_STUDY\METCS689A1\FP\CustomerPurchaseBehaviorFact.csv'
df_cpbf = pd.read_csv(cpbf_file, keep_default_na=False)
print(df_cpbf.head())
print()


print("...................................... Load data into Table CustomerPurchaseBehaviorFact ......................................")
insert_query = """
INSERT INTO CustomerPurchaseBehaviorFact (BehaviorFactID,cust_id,Product_id,DateKey,Revenue_Total,N_Purchases,Purchase_VALUE,Pay_Method)
VALUES (?,?,?,?,
        ?,?,?,?)
"""
for index, row in df_cpbf.iterrows():
    Key = index + 1
    cursor.execute(insert_query, row['BehaviorFactID'],row['cust_id'],row['Product_id'],row['DateKey'],row['Revenue_Total'],row['N_Purchases'],
                   row['Purchase_VALUE'],row['Pay_Method'])
conn.commit()

cursor.close()
conn.close()