-- Creating the Customer table (Type 2 SCD)
CREATE TABLE Customer (CustomerID INT PRIMARY KEY,Name VARCHAR(255),Address VARCHAR(255),Phone VARCHAR(15),Email VARCHAR(255),EffectiveDate DATE,EndDate DATE,CurrentFlag BIT);

-- Creating the Product table (Type 3 SCD)
CREATE TABLE Product (ProductID INT PRIMARY KEY,Name VARCHAR(255),Category VARCHAR(255),Price DECIMAL(10,2),Old_Price DECIMAL(10,2));