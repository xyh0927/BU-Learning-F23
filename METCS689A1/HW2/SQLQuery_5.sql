SELECT ProductID, COUNT(OrderID) as NumsBookingsFROM SalesGROUP BY ProductIDORDER BY NumberOfBookings DESC;

SELECT CustomerID, COUNT(OrderID) as NumsBookings FROM Sales GROUP BY CustomerID;

SELECT ProductID, SUM(TotalAmount) as Rev FROM Sales GROUP BY ProductID;

SELECT OrderDateKey, COUNT(OrderID) as NumsBookings FROM Sales GROUP BY OrderDateKey;
