CREATE TABLE Product (
    Product_id NVARCHAR(4000) PRIMARY KEY,
    SKU NVARCHAR(4000),
    Description NVARCHAR(4000),
    Category NVARCHAR(4000)
);

CREATE TABLE Customer (
    Cust_id NVARCHAR(4000) PRIMARY KEY,
    Name_Prefix NVARCHAR(4000),
    First_Name NVARCHAR(4000),
    Middle_Initial NVARCHAR(4000),
    Last_Name NVARCHAR(4000),
    Gender NVARCHAR(4000),
    Age INT,
	full_name NVARCHAR(4000),
    E_mail NVARCHAR(4000),
    Customer_Since NVARCHAR(4000),
    SSN NVARCHAR(4000),
    Phone_No NVARCHAR(4000),
    PlaceName NVARCHAR(4000),
    County NVARCHAR(4000),
    City NVARCHAR(4000),
    State NVARCHAR(4000),
    Zip NVARCHAR(4000),
    Region NVARCHAR(4000),
    User_Name NVARCHAR(4000)
);

CREATE TABLE Location (
	Lid INT PRIMARY KEY IDENTITY(1,1),
    LocationID NVARCHAR(4000),
    Place_Name NVARCHAR(4000),
    County NVARCHAR(4000),
    City NVARCHAR(4000),
    State NVARCHAR(4000),
    PreviousCity NVARCHAR(4000),
    PreviousState NVARCHAR(4000),
    Zip VARCHAR(4000),
    Region NVARCHAR(4000),
    Country NVARCHAR(4000)
);

CREATE TABLE OrderDim (
    Oid INT PRIMARY KEY,
    OrderID NVARCHAR(4000),
    InvoiceNo NVARCHAR(4000),
    Status NVARCHAR(4000),
    LastUpdated NVARCHAR(4000)
);

CREATE TABLE Time_Monthly (
    MonthKey NVARCHAR(4000) PRIMARY KEY,
    year NVARCHAR(4000),
    month NVARCHAR(4000)
);

CREATE TABLE Time_daily (
    DateKey NVARCHAR(4000) PRIMARY KEY,
    Year NVARCHAR(4000),
	Quarter NVARCHAR(4000),
    Month NVARCHAR(4000),
	Day NVARCHAR(4000),
);

CREATE TABLE SalesFact (
    SalesID INT PRIMARY KEY,
    OrderID NVARCHAR(4000),
	Oid INT,
    Cust_id NVARCHAR(4000),
    Product_id NVARCHAR(4000),
    LocationID NVARCHAR(4000),
	Lid INT,
    DateKey NVARCHAR(4000),
    qty_ordered INT,
    Price DECIMAL(18,2),
    Value DECIMAL(18,2),
    Discount_Amount DECIMAL(18,2),
    Total DECIMAL(18,2),
    FOREIGN KEY (Oid) REFERENCES OrderDim(Oid),
    FOREIGN KEY (Cust_id) REFERENCES Customer(Cust_id),
    FOREIGN KEY (Product_id) REFERENCES Product(Product_id),
    FOREIGN KEY (Lid) REFERENCES Location(Lid),
    FOREIGN KEY (DateKey) REFERENCES Time_daily(DateKey)
);

CREATE TABLE ProductPerformanceFact (
    PerformanceID INT PRIMARY KEY,
    OrderID NVARCHAR(4000),
	Oid INT,
    Product_id NVARCHAR(4000),
    MonthKey NVARCHAR(4000),
    qty_ordered INT,
    Revenue_Total DECIMAL(18,2),
    FOREIGN KEY (Oid) REFERENCES OrderDim(Oid),
    FOREIGN KEY (Product_id) REFERENCES Product(Product_id),
    FOREIGN KEY (MonthKey) REFERENCES Time_Monthly(MonthKey)
);

CREATE TABLE CustomerInteractionFact (
    InteractionID INT PRIMARY KEY,
    OrderID NVARCHAR(4000),
	Oid INT,
    Cust_id NVARCHAR(4000),
    MonthKey NVARCHAR(4000),
    TimeSpent INT,
    N_Purchases INT,
    FOREIGN KEY (Oid) REFERENCES OrderDim(Oid),
    FOREIGN KEY (Cust_id) REFERENCES Customer(Cust_id),
    FOREIGN KEY (MonthKey) REFERENCES Time_Monthly(MonthKey) 
);

CREATE TABLE CustomerPurchaseBehaviorFact (
    BehaviorFactID INT PRIMARY KEY,
    Cust_id NVARCHAR(4000),
    Product_id NVARCHAR(4000) ,
    DateKey NVARCHAR(4000),
    Revenue_Total DECIMAL(19,4),
    N_Purchases INT,
    Purchase_VALUE DECIMAL(19,4),
    Pay_Method INT,
	FOREIGN KEY (Cust_id) REFERENCES Customer(Cust_id),
	FOREIGN KEY (Product_id) REFERENCES Product(Product_id),
	FOREIGN KEY (DateKey) REFERENCES Time_daily(DateKey)
);
