import pandas as pd
from IPython.display import display


######################################################## t1 ####################################
# Load the CSV files into dataframes
Ship_df = pd.read_csv('G:\BU_STUDY\METCS689A1\HW3A\Ships.csv', keep_default_na=False)
Trip_df = pd.read_csv('G:\BU_STUDY\METCS689A1\HW3A\CLIWOC15.csv', keep_default_na=False)

# Show the first few rows of the dataframes to review the data
print("Ship_df:\n",Ship_df)

print()

print("Trip_df:\n",Trip_df)

######################################################## t2 ####################################

print()
# Group by 'ShipName', 'ShipType', and 'Nationality', then count the size of each group
duplicate_counts = Ship_df.groupby(['ShipName', 'ShipType', 'Nationality']).size()

# Filter the groups where the count is more than 1, indicating duplicates
duplicates = duplicate_counts[duplicate_counts > 1]

# Show the duplicates
print("Duplicate Combinations:")
print(duplicates)
print()

################################################### t3 #########################################################

# Remove duplicate rows based on 'ShipName', 'ShipType', and 'Nationality'
ShipDistinct_df = Ship_df.drop_duplicates(subset=['ShipName', 'ShipType', 'Nationality'])

# Show the first few rows of the new dataframe to review the data
print("ShipDistinct_df:")
print(ShipDistinct_df.head())
print()

################################################### t4 #########################################################

# Show the number of rows in ShipDistinct_df
num_rows = ShipDistinct_df.shape[0]
print("Number of rows in ShipDistinct_df:", num_rows)

# Check for duplicates based on 'ShipName', 'ShipType', and 'Nationality'
duplicates_check = ShipDistinct_df.duplicated(subset=['ShipName', 'ShipType', 'Nationality'])
any_duplicates = duplicates_check.any()

# Show if there are any duplicates
print("Are there any duplicates in ShipDistinct_df based on 'ShipName', 'ShipType', and 'Nationality'? :", any_duplicates)
print()

################################################### t5 #########################################################
# Set pandas option to display all columns
pd.set_option('display.max_columns', None)

# Get and display all column names from Trip_df
column_names = Trip_df.columns.tolist()
print("Column names in Trip_df:")
print(column_names)
print()

################################################### t6 #########################################################
print("=============================================================== t6 ==================================================")
# Perform a left join on (ShipName, ShipType, Nationality)
ShipTrips_df = pd.merge(Trip_df, ShipDistinct_df, on=['ShipName', 'ShipType', 'Nationality'], how='left', indicator=True)

# Show the first few rows of the resulting dataframe to verify the join
print("ShipTrips_df:")
print(ShipTrips_df.head())
print()

################################################### t7 #########################################################
print("=============================================================== t7 ==================================================")
# Show the first few rows of ShipTrips_df with all columns, focusing on the _merge column
print("ShipTrips_df:")
print(ShipTrips_df.head())

# Determine the unique combinations in the _merge column
merge_counts = ShipTrips_df['_merge'].value_counts()
print("\nCounts of unique combinations in the _merge column:")
print(merge_counts)
print()

################################################### t8 #########################################################
print("=============================================================== t8 ==================================================")

new_records_df = ShipTrips_df.query('_merge == "left_only"')[['ShipName', 'ShipType', 'Nationality', '_merge']]

# Show the filtered DataFrame
print("New records to bring into Ships:")
print(new_records_df.head())
print()

################################################### t9 #########################################################
print("=============================================================== t9 ==================================================")

num_new_records = len(new_records_df)
print("Number of new records found:", num_new_records)
print()

################################################### t10 #########################################################
print("=============================================================== t10 ==================================================")

# Group by 'ShipName', 'ShipType', and 'Nationality', then count the size of each group
ships_trips_duplicate_counts = new_records_df.groupby(['ShipName', 'ShipType', 'Nationality']).size()

# Filter the groups where the count is more than 1, indicating duplicates
ships_trips_duplicates = ships_trips_duplicate_counts[ships_trips_duplicate_counts > 1]

# Show the duplicates
print("Duplicate Combinations in ShipsTrips:")
print(ships_trips_duplicates)
print()

# 10.2 Remove Duplicate Records
# Remove duplicate rows based on 'ShipName', 'ShipType', and 'Nationality'
ShipsTrips_Distinct_df = new_records_df.drop_duplicates(subset=['ShipName', 'ShipType', 'Nationality'])

# Show the first few rows of the new dataframe to review the data
print("ShipsTrips_Distinct_df:")
print(ShipsTrips_Distinct_df.head())
print()

# 10.3 Verify Removal of Duplicates
# Show the number of rows in ShipsTrips_Distinct_df
num_rows_distinct = ShipsTrips_Distinct_df.shape[0]
print("Number of rows in ShipsTrips_Distinct_df:", num_rows_distinct)

# Check for duplicates based on 'ShipName', 'ShipType', and 'Nationality'
duplicates_check_distinct = ShipsTrips_Distinct_df.duplicated(subset=['ShipName', 'ShipType', 'Nationality'])
any_duplicates_distinct = duplicates_check_distinct.any()

# Show if there are any duplicates
print("Are there any duplicates in ShipsTrips_Distinct_df based on 'ShipName', 'ShipType', and 'Nationality'? :", any_duplicates_distinct)
print()

################################################### t11 #########################################################
print("=============================================================== t11 ==================================================")
# To show the entire DataFrame
print(ShipsTrips_Distinct_df)

# To get the number of rows (records)
num_records = ShipsTrips_Distinct_df.shape[0]
print("Number of new records in ShipsTrips_Distinct_df:", num_records)
print()

################################################### t12 #########################################################
print("=============================================================== t12 ==================================================")
# Combining the data frames into a single DimShip data frame
DimShip = pd.concat([ShipDistinct_df, ShipsTrips_Distinct_df], ignore_index=True)

# Showing the first few rows of the DimShip data frame
print(DimShip.head())
print()
num_records = len(DimShip)
print("Number of records in DimShip:", num_records)
print()

################################################### t13 #########################################################
print("=============================================================== t13 ==================================================")
# Adding a new column 'Id' that serves as a surrogate primary key, starting the index at 1
DimShip = DimShip.reset_index(drop=True)
DimShip['Id'] = DimShip.index + 1

# Displaying the DimShip data frame to show the new 'Id' column
print(DimShip)
print()

################################################### t15 #########################################################
print("=============================================================== t15 ==================================================")

# Selecting necessary attributes from Trip_df
selected_columns = Trip_df[['ShipName', 'ShipType', 'Nationality', 'RecID', 'Year', 'Month', 'Day', 'ShipSpeed', 'WindForce', 'BaroReading']]

# Merging with DimShip to get DimShipId
FactTrip = pd.merge(selected_columns, DimShip[['ShipName', 'ShipType', 'Nationality', 'Id']], 
                    on=['ShipName', 'ShipType', 'Nationality'], how='left')

# Renaming the Id column to DimShipId
FactTrip = FactTrip.rename(columns={'Id': 'DimShipId'})

# Displaying sample data from FactTrip
print("FactTrip DataFrame:")
print(FactTrip.head())

# Verify counts between the original Trip_df and FactTrip
print("\nCount of rows in Trip_df:", len(Trip_df))
print("Count of rows in FactTrip:", len(FactTrip))
print()

################################################### t16 & 17 #########################################################
print("=============================================================== t16 & 17 ==================================================")

# Resetting the index of the FactTrip DataFrame
FactTrip = FactTrip.reset_index(drop=True)

# Adding a new column 'Id' as the surrogate key
FactTrip['Id'] = FactTrip.index + 1

# Displaying the FactTrip DataFrame to show the new 'Id' column
display(FactTrip.head())

################################################### t18 #########################################################
print("=============================================================== t18 ==================================================")

FactTrip['DateString'] = pd.to_datetime(FactTrip[['Year', 'Month', 'Day']].astype(str).agg('-'.join, axis=1), errors='coerce', format='%Y-%m-%d')
FactTrip['Date'] = pd.to_datetime(FactTrip['DateString'], errors='coerce')

print(FactTrip)
print()

################################################### t19 #########################################################
print("=============================================================== t19 ==================================================")
print(FactTrip.head(20).to_string())
print()

print("======================================================================================================================")

################################################### t20 #########################################################
print("=============================================================== t20 ==================================================")
import pyodbc as db  # SQL Server
conn = db.connect('Driver={SQL Server};'
                'Server=XUYUHAN;'
               'Server=SQLServer-PC;'
               'Database=hw3;'
                'Trusted_Connection=yes;')


cursor = conn.cursor()

stateQuery = "IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Dim_Ship') DROP TABLE Dim_Ship"

cursor.execute(stateQuery)
conn.commit()

create_table_query = """
CREATE TABLE Dim_Ship (
    ShipKey INT PRIMARY KEY,
    ShipName VARCHAR(255),
    ShipType VARCHAR(255),
    Nationality VARCHAR(255),
);"""

cursor.execute(create_table_query)
conn.commit()
print("Dim_Ship table created successfully!")

cursor.close()
conn.close()
print()

################################################### t21 #########################################################
print("=============================================================== t21 ==================================================")

DimShip1 = DimShip.drop(['_merge'], axis = 1)


print(DimShip1)
import pyodbc as db  # SQL Server
conn1 = db.connect('Driver={SQL Server};'
                'Server=XUYUHAN;'
               'Server=SQLServer-PC;'
               'Database=hw3;'
                'Trusted_Connection=yes;')


cursor1 = conn1.cursor()

insert_query = """
INSERT INTO Dim_Ship (ShipKey ,ShipName, ShipType, Nationality)
VALUES (?, ?, ?, ?)
"""

# Iterate over the rows of the DataFrame and insert each row into the database
for index, row in DimShip1.iterrows():
    Ship_Key = index + 1
    
    cursor1.execute(insert_query, row['Id'], row['ShipName'], row['ShipType'], row['Nationality'])

# Commit the transaction
conn1.commit()

# Close the cursor and connection
cursor1.close()
conn1.close()

print("Data inserted into Dim_Ship table successfully!")

################################################### t23 #########################################################
print("=============================================================== t23 ==================================================")

print(FactTrip)

FactTrip1 = FactTrip.drop(['ShipSpeed'], axis = 1)

FactTrip1 = FactTrip1.drop(['BaroReading'], axis = 1)

conn2 = db.connect('Driver={SQL Server};'
                'Server=XUYUHAN;'
               'Server=SQLServer-PC;'
               'Database=hw3;'
                'Trusted_Connection=yes;')


cursor2 = conn2.cursor()

stateQuery = "IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Fact_Trip') DROP TABLE Fact_Trip"

cursor2.execute(stateQuery)
conn2.commit()

create_table_query = """
CREATE TABLE Fact_Trip (
    ShipKey INT PRIMARY KEY,
    ShipName VARCHAR(255),
    ShipType VARCHAR(255),
    Nationality VARCHAR(255),
    RecID INT,
    Year INT,
    Month INT,
    Day INT,
    WindForce VARCHAR(255),
    DimShipId INT,
);"""

cursor2.execute(create_table_query)
conn2.commit()
print("Fact_Trip table created successfully!")

cursor2.close()
conn2.close()
print()

################################################### t24 #########################################################
print("=============================================================== t24 ==================================================")

conn3 = db.connect('Driver={SQL Server};'
                'Server=XUYUHAN;'
               'Server=SQLServer-PC;'
               'Database=hw3;'
                'Trusted_Connection=yes;')


cursor3 = conn3.cursor()

insert_query = """
INSERT INTO Fact_Trip (ShipKey, ShipName, ShipType, Nationality, RecID, Year, Month, Day, WindForce, DimShipId)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


# Iterate over the rows of the DataFrame and insert each row into the database
for index, row in FactTrip1.iterrows():
    cursor3.execute(insert_query, row['Id'], row['ShipName'], row['ShipType'], row['Nationality'], row['RecID'], row['Year'], row['Month'], row['Day'], row['WindForce'], row['DimShipId'])

# Commit the transaction
conn3.commit()

# Close the cursor and connection
cursor3.close()
conn3.close()

print("Data inserted into Fact_Trip table successfully!")

################################################### Extra Credit #########################################################
print("=============================================================== Extra Credit ==================================================")

conn4 = db.connect('Driver={SQL Server};'
                'Server=XUYUHAN;'
               'Server=SQLServer-PC;'
               'Database=hw3;'
                'Trusted_Connection=yes;')


cursor4 = conn4.cursor()


# 1. Check the first few rows in Dim_Ship
print("Dim_Ship Table:")
cursor4.execute("SELECT * FROM Dim_Ship;")
for row in cursor4.fetchmany(5):
    print(row)

print("\nFact_Trip Table:")
# 2. Check the first few rows in Fact_Trip
cursor4.execute("SELECT * FROM Fact_Trip;")
for row in cursor4.fetchmany(5):
    print(row)

# 3. Find the top 5 most common ship types
print("\nTop 5 Most Common Ship Types:")
query = """
SELECT ShipType, COUNT(*) as ShipCount
FROM Dim_Ship
GROUP BY ShipType
ORDER BY ShipCount DESC
"""
cursor4.execute(query)
for row in cursor4.fetchmany(5):
    print(row)

# 4. Find the average number of trips per ship
print("\nAverage Number of Trips per Ship:")
query = """
SELECT AVG(TripCount) as AvgTripsPerShip
FROM (SELECT ShipKey, COUNT(*) as TripCount
      FROM Fact_Trip
      GROUP BY ShipKey) as SubQuery
"""
cursor4.execute(query)
for row in cursor4.fetchall():
    print(row)

cursor4.close()
conn4.close()