-- Calculate Revenue for Sales
SELECT SUM(TotalAmount) as Rev FROM Sales;

-- Calculate SoldQuantity for Sales
SELECT SUM(Quantity) as SoldQuantity FROM Sales;

-- Calculate StockChange for Inventory
SELECT SUM(StockIn - StockOut) as StockChange FROM Inventory;
