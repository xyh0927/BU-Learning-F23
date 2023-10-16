-- Creating the Sales table (Snapshot Fact)
CREATE TABLE Sales (OrderID INT PRIMARY KEY,CustomerID INT,ProductID INT,OrderDateKey INT,Quantity INT,TotalAmount DECIMAL(10,2));

-- Creating the Inventory table (Cumulative Fact)
CREATE TABLE Inventory (ProductID INT PRIMARY KEY,DateKey INT,OpeningStock INT,StockIn INT,StockOut INT,ClosingStock AS (OpeningStock + StockIn - StockOut));