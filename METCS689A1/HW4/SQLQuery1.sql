SELECT 
    COUNT(*) AS RecordCount,
    GETDATE() AS TodaysDate,
    'Yuhan Xu' AS Name
FROM 
    MANUFACTURE_FACT;







-- 1
WITH FactoryProduction AS (
    SELECT 
        CAST(SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 4) AS INT) AS Year,
        fd.FACTORY_LABEL,
        SUM(mf.QTY_PASSED) AS TotalUnitsProduced,
        SUM(mf.QTY_FAILED) AS TotalUnitsFailed,
        ROW_NUMBER() OVER (PARTITION BY CAST(SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 4) AS INT) ORDER BY SUM(mf.QTY_PASSED) DESC) AS FactoryRank
    FROM 
        MANUFACTURE_FACT mf
        INNER JOIN FACTORY_DIM fd ON mf.FACTORY_KEY = fd.FACTORY_KEY
    GROUP BY 
        CAST(SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 4) AS INT),
        fd.FACTORY_LABEL
)
SELECT 
    Year,
    FACTORY_LABEL,
    TotalUnitsProduced,
    TotalUnitsFailed,
    FactoryRank
FROM 
    FactoryProduction
WHERE 
    FactoryRank <= 3
ORDER BY 
    Year DESC;



--2a
SELECT 
    fd.FACTORY_LABEL,
    CASE 
        WHEN CHARINDEX('-', mf.MANUFACTURE_CAL_KEY) > 0 THEN 
            SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 2) + '-' +
            DATENAME(MONTH, CONVERT(DATE, SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 10), 120))
    END as Month,
    SUM(mf.QTY_PASSED) AS TotalUnitsProduced,
    SUM(mf.QTY_FAILED) AS TotalUnitsFailed
FROM 
    MANUFACTURE_FACT mf
    INNER JOIN FACTORY_DIM fd ON mf.FACTORY_KEY = fd.FACTORY_KEY
WHERE 
    CAST(SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 4) AS INT) = 2022
GROUP BY 
    fd.FACTORY_LABEL,
    CASE 
        WHEN CHARINDEX('-', mf.MANUFACTURE_CAL_KEY) > 0 THEN 
            SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 2) + '-' +
            DATENAME(MONTH, CONVERT(DATE, SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 10), 120))
    END
WITH ROLLUP;


--2c
WITH MonthlyProduction AS (
    SELECT 
        fd.FACTORY_LABEL,
        CASE 
            WHEN CHARINDEX('-', mf.MANUFACTURE_CAL_KEY) > 0 THEN 
                SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 2) + '-' +
                DATENAME(MONTH, CONVERT(DATE, SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 10), 120))
        END as Month,
        pd.PRODUCT_DESCRIPTION AS ProductDescription,
        mf.QTY_PASSED AS TotalUnitsProduced,
        mf.QTY_FAILED AS TotalUnitsFailed,
        ROW_NUMBER() OVER (
            PARTITION BY fd.FACTORY_LABEL, 
                         CASE 
                            WHEN CHARINDEX('-', mf.MANUFACTURE_CAL_KEY) > 0 THEN 
                                SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 2) + '-' +
                                DATENAME(MONTH, CONVERT(DATE, SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 10), 120))
                         END
            ORDER BY mf.QTY_PASSED DESC
        ) AS Rank
    FROM 
        MANUFACTURE_FACT mf
        INNER JOIN FACTORY_DIM fd ON mf.FACTORY_KEY = fd.FACTORY_KEY
        INNER JOIN PRODUCT_DIM pd ON mf.PRODUCT_KEY = pd.PRODUCT_KEY
    WHERE 
        CAST(SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 4) AS INT) = 2022
)
SELECT 
    FACTORY_LABEL,
    Month,
    ProductDescription,
    TotalUnitsProduced,
    TotalUnitsFailed
FROM 
    MonthlyProduction
WHERE 
    Rank <= 3
ORDER BY 
    FACTORY_LABEL, 
    Month, 
    Rank;


-- 3
SELECT 
    fd.FACTORY_LABEL AS FactoryName,
    CASE 
        WHEN CHARINDEX('-', mf.MANUFACTURE_CAL_KEY) > 0 THEN 
            SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 2) + '-' +
            DATENAME(MONTH, CONVERT(DATE, SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 10), 120))
        END as Month,
    pd.BRAND_LABEL AS Brand,
    SUM(mf.QTY_PASSED) AS TotalUnitsProduced,
    SUM(mf.QTY_FAILED) AS TotalUnitsFailed
FROM 
    MANUFACTURE_FACT mf
    INNER JOIN FACTORY_DIM fd ON mf.FACTORY_KEY = fd.FACTORY_KEY
    INNER JOIN PRODUCT_DIM pd ON mf.PRODUCT_KEY = pd.PRODUCT_KEY
GROUP BY 
    ROLLUP(fd.FACTORY_LABEL, 
            CASE 
                WHEN CHARINDEX('-', mf.MANUFACTURE_CAL_KEY) > 0 THEN 
                    SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 2) + '-' +
                    DATENAME(MONTH, CONVERT(DATE, SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 10), 120))
            END, 
            pd.BRAND_LABEL);


-- 4
SELECT 
    fd.FACTORY_LABEL AS FactoryName,
    CASE 
        WHEN CHARINDEX('-', mf.MANUFACTURE_CAL_KEY) > 0 THEN 
            SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 2) + '-' +
            DATENAME(MONTH, CONVERT(DATE, SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 10), 120))
        END as Month,
    pd.BRAND_LABEL AS Brand,
    SUM(mf.QTY_PASSED) AS TotalUnitsProduced,
    SUM(mf.QTY_FAILED) AS TotalUnitsFailed
FROM 
    MANUFACTURE_FACT mf
    INNER JOIN FACTORY_DIM fd ON mf.FACTORY_KEY = fd.FACTORY_KEY
    INNER JOIN PRODUCT_DIM pd ON mf.PRODUCT_KEY = pd.PRODUCT_KEY
GROUP BY 
    CUBE(fd.FACTORY_LABEL, 
            CASE 
                WHEN CHARINDEX('-', mf.MANUFACTURE_CAL_KEY) > 0 THEN 
                    SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 2) + '-' +
                    DATENAME(MONTH, CONVERT(DATE, SUBSTRING(mf.MANUFACTURE_CAL_KEY, 6, 10), 120))
            END, 
            pd.BRAND_LABEL);


-- 6
SELECT 
    Year,
    FactoryName,
    QuantityPassed
FROM (
    SELECT 
        CAST(SUBSTRING(mf.MANUFACTURE_CAL_KEY, PATINDEX('%[0-9][0-9][0-9][0-9]%', mf.MANUFACTURE_CAL_KEY), 4) AS INT) AS Year,
        fd.FACTORY_LABEL AS FactoryName,
        SUM(mf.QTY_PASSED) AS QuantityPassed,
        CAST(SUBSTRING(mf.MANUFACTURE_CAL_KEY, CHARINDEX('-', mf.MANUFACTURE_CAL_KEY) + 1, 2) AS INT) AS Month
    FROM 
        MANUFACTURE_FACT mf
    INNER JOIN FACTORY_DIM fd 
        ON mf.FACTORY_KEY = fd.FACTORY_KEY
    GROUP BY 
        fd.FACTORY_LABEL,
        CAST(SUBSTRING(mf.MANUFACTURE_CAL_KEY, PATINDEX('%[0-9][0-9][0-9][0-9]%', mf.MANUFACTURE_CAL_KEY), 4) AS INT),
        CAST(SUBSTRING(mf.MANUFACTURE_CAL_KEY, CHARINDEX('-', mf.MANUFACTURE_CAL_KEY) + 1, 2) AS INT)
) AS MonthlyData
WHERE 
    Month = 2 AND 
    Year IN (
        SELECT DISTINCT TOP 5 Year 
        FROM (
            SELECT 
                CAST(SUBSTRING(mf.MANUFACTURE_CAL_KEY, PATINDEX('%[0-9][0-9][0-9][0-9]%', mf.MANUFACTURE_CAL_KEY), 4) AS INT) AS Year
            FROM MANUFACTURE_FACT mf
        ) AS YearsData
        ORDER BY Year DESC
    )
ORDER BY 
    Year DESC, 
    FactoryName;



-- 7
WITH FebruaryData AS (
    SELECT 
        CAST(SUBSTRING(mf.MANUFACTURE_CAL_KEY, PATINDEX('%[0-9][0-9][0-9][0-9]%', mf.MANUFACTURE_CAL_KEY), 4) AS INT) AS Year,
        fd.FACTORY_LABEL AS FactoryName,
        SUM(mf.QTY_PASSED) AS QuantityPassed
    FROM 
        MANUFACTURE_FACT mf
    INNER JOIN FACTORY_DIM fd 
        ON mf.FACTORY_KEY = fd.FACTORY_KEY
    WHERE 
        CAST(SUBSTRING(mf.MANUFACTURE_CAL_KEY, CHARINDEX('-', mf.MANUFACTURE_CAL_KEY) + 1, 2) AS INT) = 2 
    GROUP BY 
        fd.FACTORY_LABEL,
        CAST(SUBSTRING(mf.MANUFACTURE_CAL_KEY, PATINDEX('%[0-9][0-9][0-9][0-9]%', mf.MANUFACTURE_CAL_KEY), 4) AS INT)
)

SELECT 
    FactoryName,
    [2022], [2021], [2020], [2019], [2018]
FROM 
    FebruaryData
PIVOT (
    SUM(QuantityPassed) FOR Year IN ([2022], [2021], [2020], [2019], [2018])
) AS PivotTable
ORDER BY FactoryName;

