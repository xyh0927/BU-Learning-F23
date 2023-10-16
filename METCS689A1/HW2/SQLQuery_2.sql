-- Creating the DateDimension table (Role-playing dimension)
CREATE TABLE DateDimension (DateKey INT PRIMARY KEY,Day INT,Month INT,Quarter INT,Year INT);

-- Creating the SalesHistory table (Bitemporal table)
CREATE TABLE SalesHistory (HistoryID INT PRIMARY KEY,OrderID INT,CustomerID INT,ProductID INT,OrderDateKey INT,DeliveryDateKey INT,Quantity INT,TotalAmount DECIMAL(10,2),ValidFrom DATE,ValidTo DATE,SystemTime TIMESTAMP
);