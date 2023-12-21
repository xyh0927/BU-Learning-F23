CREATE TABLE CustomerHistory (
	Chid int PRIMARY KEY IDENTITY(1,1),
	Cust_id NVARCHAR(4000),
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
    User_Name NVARCHAR(4000),
	newE_mail NVARCHAR(4000),
    newCustomer_Since NVARCHAR(4000),
    newSSN NVARCHAR(4000),
    newPhone_No NVARCHAR(4000),
    newPlaceName NVARCHAR(4000),
    newCounty NVARCHAR(4000),
    newCity NVARCHAR(4000),
    newState NVARCHAR(4000),
    newZip NVARCHAR(4000),
    newRegion NVARCHAR(4000),
    newUser_Name NVARCHAR(4000),
	UpdateTime DATETIME
);

DROP TABLE CustomerHistory;

DROP PROCEDURE UpdateAndInsertCustomerAddress;
CREATE PROCEDURE UpdateAndInsertCustomerAddress
    @Cust_id INT, 
    @NewCity VARCHAR(50), 
    @NewState VARCHAR(50),
    @NewZip VARCHAR(10)
AS BEGIN
    -- Start the transaction
    BEGIN TRANSACTION;
		IF EXISTS (SELECT Cust_id FROM Customer WHERE Cust_id = @Cust_id AND isCurrent = 1) BEGIN
			IF (SELECT City FROM Customer WHERE Cust_id = @Cust_id AND isCurrent = 1) != @NewCity BEGIN
				IF(SELECT City FROM Customer WHERE Cust_id = @Cust_id AND isCurrent = 1) != @NewState BEGIN
					INSERT INTO CustomerHistory (
						Cust_id, City, State, Zip,
						newCity, newState, newZip, UpdateTime
					)
					VALUES(
						(SELECT Cust_id FROM Customer WHERE Cust_id = @Cust_id AND isCurrent = 1),
						(SELECT City FROM Customer WHERE Cust_id = @Cust_id AND isCurrent = 1),
						(SELECT State FROM Customer WHERE Cust_id = @Cust_id AND isCurrent = 1),
						(SELECT Zip FROM Customer WHERE Cust_id = @Cust_id AND isCurrent = 1),
						@NewCity, @NewState,@NewZip, GETDATE()
					)

					-- Update the existing customer record
					UPDATE Customer SET LastUpdated = GETDATE(), isCurrent = 2	WHERE Cust_id = @Cust_id AND isCurrent = 1;
	
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
					FROM Customer WHERE Cust_id = @Cust_id AND isCurrent = 2;

					Update Customer SET isCurrent = 0 WHERE Cust_id = @Cust_id AND isCurrent = 2;
				END;
				ELSE BEGIN
					INSERT INTO CustomerHistory (
						Cust_id, City, Zip,
						newCity, newZip, UpdateTime
					)
					VALUES(
						(SELECT Cust_id FROM Customer WHERE Cust_id = @Cust_id AND isCurrent = 1),
						(SELECT City FROM Customer WHERE Cust_id = @Cust_id AND isCurrent = 1),
						(SELECT Zip FROM Customer WHERE Cust_id = @Cust_id AND isCurrent = 1),
						@NewCity,@NewZip, GETDATE()
					)

					-- Update the existing customer record
					UPDATE Customer SET LastUpdated = GETDATE(), isCurrent = 2	WHERE Cust_id = @Cust_id AND isCurrent = 1;
	
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
						State, @NewZip, Region, User_Name, GETDATE(), 1
					FROM Customer WHERE Cust_id = @Cust_id AND isCurrent = 2;

					Update Customer SET isCurrent = 0 WHERE Cust_id = @Cust_id AND isCurrent = 2;
				END;
			END;

			ELSE BEGIN
				PRINT 'Your input Address is same as the Address in the Database.'
			END;

		END;

		ELSE BEGIN 
			PRINT '@Customer ID Input DOES NOT EXIST IN DATABASE, Check again' 
		END;
    -- Commit the transaction
    COMMIT;
END;


EXEC UpdateAndInsertCustomerAddress @Cust_id ='42485', @NewCity = 'Ames', @NewState = 'IA', @NewZip = '50011';


SELECT * FROM Customer WHERE Cust_id = '42485'
SELECT * FROM CustomerHistory WHERE Cust_id = '42485'

TRUNCATE TABLE Customer;
TRUNCATE TABLE CustomerHistory;