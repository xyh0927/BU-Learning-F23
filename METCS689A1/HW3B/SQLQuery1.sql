CREATE TABLE Ships_Staging (
    ShipName NVARCHAR(255),
    ShipType NVARCHAR(255),
    Nationality NVARCHAR(255)
);



BULK INSERT Ships_Staging
FROM 'G:\BU_STUDY\METCS689A1\HW3B\Ships.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2
);

SELECT * FROM Ships_Staging;


SELECT * FROM Trips_Staging;

SELECT DISTINCT ShipName, ShipType, Nationality
FROM Ships_staging;


SELECT DISTINCT t.ShipName, t.ShipType, t.Nationality
FROM Trips_Staging t
LEFT JOIN Ships_Staging s
ON t.ShipName = s.ShipName AND t.ShipType = s.ShipType AND t.Nationality = s.Nationality
WHERE s.ShipName IS NULL;



SELECT DISTINCT ShipName, ShipType, Nationality
FROM Ships_staging
UNION
SELECT DISTINCT t.ShipName, t.ShipType, t.Nationality
FROM Trips_Staging t
WHERE NOT EXISTS (
    SELECT 1
    FROM Ships_Staging s
    WHERE t.ShipName = s.ShipName AND t.ShipType = s.ShipType AND t.Nationality = s.Nationality
);

CREATE TABLE DimShipSQL (
    DimShipID INT PRIMARY KEY IDENTITY(1,1),
    ShipName VARCHAR(255),
    ShipType VARCHAR(255),
    Nationality VARCHAR(255)
);

INSERT INTO DimShipSQL (ShipName, ShipType, Nationality)
SELECT ShipName, ShipType, Nationality
FROM (
    -- Selecting unique ships from Ships_Staging
    SELECT DISTINCT ShipName, ShipType, Nationality
    FROM Ships_staging
    UNION
    SELECT DISTINCT ShipName, ShipType, Nationality
    FROM Trips_Staging t
    WHERE NOT EXISTS (
        SELECT 1
        FROM Ships_Staging s
        WHERE t.ShipName = s.ShipName AND t.ShipType = s.ShipType AND t.Nationality = s.Nationality
    )
) AS CombinedShips;



SELECT * FROM DimShipSQL;
SELECT COUNT(*) FROM DimShipSQL;


CREATE TABLE FactTripSQL (
    TripFactID INT PRIMARY KEY IDENTITY(1,1),  -- a. TripFactSQL PK using identity for key management
    DimShipID INT NOT NULL,                    -- b. DimShipID FK which should not be NULL
    TripRecID INT,                             -- c. TripRecID (source column is RecID)
    TripDate DATE,                             -- d. TripDate (date column)
    Distance FLOAT,                            -- e. Measure 1: Distance
    ShipSpeed FLOAT,                           -- e. Measure 2: ShipSpeed
    WindForce FLOAT,                           -- e. Measure 3: WindForce
    FOREIGN KEY (DimShipID) REFERENCES DimShipSQL(DimShipID)   -- Setting up the foreign key relationship
);


SELECT 
    ts.RecID as TripRecID,
    ts.RecID,
    ts.Year,
    ts.Month,
    ts.Day,
    ts.InstName,  
    ts.InstPlace,  
    ts.InstLand,  
    ds.DimShipID
FROM 
    Trips_Staging ts
JOIN 
    DimShipSQL ds ON ts.ShipName = ds.ShipName AND ts.ShipType = ds.ShipType AND ts.Nationality = ds.Nationality;

SELECT COUNT(*) 
FROM Trips_Staging ts
JOIN DimShipSQL ds ON ts.ShipName = ds.ShipName AND ts.ShipType = ds.ShipType AND ts.Nationality = ds.Nationality;


SELECT TOP 20
    RecID,
    Year,
    Month,
    Day,
    CASE 
        WHEN Year IS NOT NULL AND Month IS NOT NULL AND Day IS NOT NULL
        THEN TRY_CONVERT(DATE, CAST(Year AS VARCHAR(4)) + '-' + RIGHT('0' + CAST(Month AS VARCHAR(2)), 2) + '-' + RIGHT('0' + CAST(Day AS VARCHAR(2)), 2), 23)
    END AS TripDate
FROM 
    Trips_Staging



INSERT INTO FactTripSQL (DimShipID, TripRecID, TripDate, Distance, ShipSpeed, WindForce)
SELECT 
    ds.DimShipID,
    ts.RecID AS TripRecID,
    TRY_CAST(ts.Year + '-' + RIGHT('0' + ts.Month, 2) + '-' + RIGHT('0' + ts.Day, 2) AS DATE) AS TripDate,
    TRY_CAST(ts.Distance AS FLOAT),  -- Replace FLOAT with the actual data type
    TRY_CAST(ts.ShipSpeed AS FLOAT),  -- Replace FLOAT with the actual data type
    TRY_CAST(ts.WindForce AS FLOAT)   -- Replace FLOAT with the actual data type
FROM 
    Trips_Staging ts
JOIN 
    DimShipSQL ds ON ts.ShipName = ds.ShipName AND ts.ShipType = ds.ShipType AND ts.Nationality = ds.Nationality;



SELECT * FROM FactTripSQL;
SELECT COUNT(*) FROM FactTripSQL;



ALTER TABLE DimShipSQL ADD ShipTypeCategory NVARCHAR(50);
UPDATE DimShipSQL
SET ShipTypeCategory = 
    CASE 
        WHEN ShipType = 'Frigate' THEN 'Warship'
        WHEN ShipType = 'Brig' OR ShipType = 'Schooner' THEN 'Merchant Ship'
        ELSE 'Other'
    END;

ALTER TABLE FactTripSQL ADD ShipTypeCategory NVARCHAR(50);

UPDATE FactTripSQL
SET ShipTypeCategory = ds.ShipTypeCategory
FROM FactTripSQL ft
JOIN DimShipSQL ds ON ft.DimShipID = ds.DimShipID;


SELECT * FROM DimShipSQL;
SELECT * FROM FactTripSQL;
