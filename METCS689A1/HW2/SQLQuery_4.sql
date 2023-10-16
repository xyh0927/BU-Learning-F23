-- Inserting data into the Customer table
INSERT INTO Customer (CustomerID, Name, Address, Phone, Email, EffectiveDate, EndDate, CurrentFlag)
VALUES 
(1, 'John Doe', '123 Main St', '555-555-5555', 'johndoe@example.com', '2023-01-01', NULL, 1),
(2, 'Jane Smith', '456 Oak St', '555-555-5556', 'janesmith@example.com', '2023-01-02', NULL, 1),
(3, 'Bob Johnson', '789 Pine St', '555-555-5557', 'bobjohnson@example.com', '2023-01-03', NULL, 1),
(4, 'Alice Williams', '321 Elm St', '555-555-5558', 'alicewilliams@example.com', '2023-01-04', NULL, 1),
(5, 'Charlie Brown', '654 Maple St', '555-555-5559', 'charliebrown@example.com', '2023-01-05', NULL, 1);

-- Inserting data into the Product table
INSERT INTO Product (ProductID, Name, Category, Price, Old_Price)
VALUES 
(1, 'City Tour A', 'Tour A', 100.00, NULL),
(2, 'City Tour B', 'Tour B', 200.00, NULL),
(3, 'City Tour C', 'Tour C', 300.00, NULL),
(4, 'City Tour D', 'Tour D', 400.00, NULL),
(5, 'City Tour E', 'Tour E', 500.00, NULL);

-- Inserting data into the DateDimension table
INSERT INTO DateDimension (DateKey, Day, Month, Quarter, Year)
VALUES 
(20230101, 1, 1, 1, 2023),
(20230102, 2, 1, 1, 2023),
(20230103, 3, 1, 1, 2023),
(20230104, 4, 1, 1, 2023),
(20230105, 5, 1, 1, 2023);

-- Inserting data into the SalesHistory table
INSERT INTO SalesHistory (HistoryID,OrderID,CustomerID,ProductID,OrderDateKey,Quantity,TotalAmount,ValidFrom,ValidTo)
VALUES 
(1 ,1 ,1 ,1 ,20230101 ,2 ,200.00 ,'2023-01-01' ,NULL ),
(2 ,2 ,2 ,2 ,20230102 ,3 ,600.00 ,'2023-01-02' ,NULL),
(3 ,3 ,3 ,3 ,20230103 ,4 ,1200.00 ,'2023-01-03' ,NULL),
(4 ,4 ,4 ,4 ,20230104 ,5 ,2000.00 ,'2023-01-04' ,NULL),
(5 ,5 ,5 ,5 ,20230105 ,6 ,3000.00 ,'2023-01-05' ,NULL);

-- Inserting data into the Sales table
INSERT INTO Sales (OrderID,CustomerID,ProductID,OrderDateKey,Quantity,TotalAmount)
VALUES 
(1 ,1 ,1 ,20230101 ,2 ,200.00),
(2 ,2 ,2 ,20230102 ,3 ,600.00),
(3 ,3 ,3 ,20230103 ,4 ,1200.00),
(4 ,4 ,4 ,20230104 ,5 ,2000.00),
(5 ,5 ,5 ,20230105 ,6 ,3000.00);

-- Inserting data into the Inventory table
INSERT INTO Inventory (ProductID,DateKey ,OpeningStock ,StockIn ,StockOut )
VALUES 
(1 ,20230101 ,100 ,50 ,2 ),
(2 ,20230102 ,200 ,100 ,6 ),
(3 ,20230103 ,300 ,150 ,12 ),
(4 ,20230104 ,400 ,200 ,20 ),
(5 ,20230105 ,500 ,250 ,30 );