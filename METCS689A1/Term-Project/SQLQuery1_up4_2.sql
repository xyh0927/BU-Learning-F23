CREATE PROCEDURE UpdateLocationInfo
    @Cust_id VARCHAR(50), 
    @NewCity VARCHAR(50), 
    @NewState VARCHAR(50)
AS
BEGIN
    -- Start the transaction
    BEGIN TRANSACTION;

    -- Declare a table variable
    DECLARE @ToUpdate TABLE (LocationID INT);

    -- Populate the table variable with the IDs to update
    INSERT INTO @ToUpdate (LocationID)
    SELECT loc.LocationID
    FROM Location loc
    INNER JOIN SalesFact sf ON loc.LocationID = sf.LocationID
    WHERE sf.Cust_id = @Cust_id;

    -- Perform the update using the table variable
    UPDATE loc
    SET
        loc.PreviousCity = loc.City,
        loc.PreviousState = loc.State,
        loc.City = @NewCity,
        loc.State = @NewState
    FROM Location loc
    INNER JOIN @ToUpdate ut ON loc.LocationID = ut.LocationID;

    -- Commit the transaction
    COMMIT;
END;



EXEC UpdateLocationInfo '42485', 'Boston', 'MA';



CREATE PROCEDURE UpdateAndInsertCustomer
    @Cust_id INT, 
    @NewCity VARCHAR(50), 
    @NewState VARCHAR(50),
    @NewZip VARCHAR(10)
AS
BEGIN
    -- Start the transaction
    BEGIN TRANSACTION;

    -- Update the existing customer record
    UPDATE Customer
    SET LastUpdated = GETDATE(), 
        isCurrent = 0 
    WHERE Cust_id = @Cust_id
    AND isCurrent = 1;

    -- Insert a new record for the customer with the updated information
    INSERT INTO Customer (
        Cust_id, Name_Prefix, First_Name, Middle_Initial, Last_Name,
        Gender, age, full_name, E_mail, Customer_Since,
        SSN, Phone_No, PlaceName, County, City,
        State, Zip, Region, User_Name, LastUpdated, isCurrent
    )
    SELECT 
        Cust_id, Name_Prefix, First_Name, Middle_Initial, Last_Name,
        Gender, age, full_name, E_mail, Customer_Since,
        SSN, Phone_No, PlaceName, County, @NewCity,
        @NewState, @NewZip, Region, User_Name, GETDATE(), 1
    FROM Customer 
    WHERE Cust_id = @Cust_id
    AND isCurrent = 0;

    -- Commit the transaction
    COMMIT;
END;

EXEC UpdateAndInsertCustomer @Cust_id ='42485', @NewCity = 'Boston', @NewState = 'MA', @NewZip = '02215';






SELECT 
  Customer.Age, 
  Customer.Gender, 
  AVG(CustomerPurchaseBehaviorFact.Purchase_VALUE) AS AvgPurchaseValue
FROM 
  Customer
JOIN CustomerPurchaseBehaviorFact ON Customer.Cust_id = CustomerPurchaseBehaviorFact.Cust_id
GROUP BY 
  Customer.Age, Customer.Gender;



SELECT 
  S1.Product_id AS Product1, 
  S2.Product_id AS Product2, 
  COUNT(*) AS TimesPurchasedTogether
FROM 
  SalesFact S1
JOIN SalesFact S2 ON S1.OrderID = S2.OrderID AND S1.Product_id < S2.Product_id
GROUP BY 
  S1.Product_id, S2.Product_id;


SELECT 
  C.Gender, 
  C.Age, 
  AVG(SF.Total) AS AverageOrderValue, 
  AVG(SF.Discount_Amount) AS AverageDiscount
FROM 
  SalesFact SF
JOIN Customer C ON SF.Cust_id = C.Cust_id
GROUP BY 
  C.Gender, 
  C.Age;