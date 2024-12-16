--DWH HOMEWORK 2

--1.Integration.InsertDimensionAccount_Incremental

CREATE OR ALTER PROCEDURE integration.InsertDimensionAccount_Incremental
AS
BEGIN
    SELECT
        a.ID AS AccountID,
        a.AccountNumber,
        a.AllowedOverdraft
    INTO #Data
    FROM BrainsterDB.dbo.Account AS a;

    INSERT INTO dimension.Account (AccountID, AccountNumber, AllowedOverdraft)
    SELECT
        s.AccountID,
        s.AccountNumber,
        s.AllowedOverdraft
    FROM #Data AS s
    LEFT JOIN dimension.Account AS d
        ON s.AccountID = d.AccountID
    WHERE d.AccountID IS NULL;

    UPDATE d
    SET d.AccountNumber = s.AccountNumber
    FROM dimension.Account AS d
    INNER JOIN #Data AS s
        ON d.AccountID = s.AccountID
    WHERE d.AccountNumber <> s.AccountNumber;

    INSERT INTO dimension.Account (AccountID, AccountNumber, AllowedOverdraft)
    SELECT
        s.AccountID,
        s.AccountNumber,
        s.AllowedOverdraft
    FROM #Data AS s
    INNER JOIN dimension.Account AS d
        ON s.AccountID = d.AccountID
    WHERE s.AllowedOverdraft <> d.AllowedOverdraft;

    DROP TABLE #Data;
END;
GO


EXEC Integration.InsertDimensionAccount_Incremental                                 --Execute
GO
 
--INSERT INTO BrainsterDB.dbo.Account (AccountNumber, AllowedOverdraft,CustomerId,CurrencyId, EmployeeId)  
--VALUES 
--    ('17261', 500, 10, 1, 5),
--	('17263', 500, 10, 1, 5),
--	('17262', 500, 10, 1, 5); 
--GO

--INSERT INTO dimension.Account (AccountNumber, AllowedOverdraft, AccountID)          --Test
--VALUES 
--    ('17261', 500, 1),
--	('17263', 500,1),
--	('17262', 500,1); 
--GO

SELECT * FROM dimension.Account
GO

----------------------------------------------------------------------------------------------------------

--2.Integration. InsertFactAccountDetails_Incremental

CREATE OR ALTER PROCEDURE Integration.InsertFactAccountDetails_Incremental
AS
BEGIN
    WITH [Data] AS (
        SELECT 
            ad.AccountID,
            c.ID AS CustomerID,
            cu.ID AS CurrencyID,
            e.ID AS EmployeeID,
            ad.TransactionDate,
            ad.Amount,
            lt.[Name] AS TransactionType
        FROM BrainsterDB.dbo.AccountDetails AS ad
        INNER JOIN BrainsterDB.dbo.Account AS a ON ad.AccountId = a.ID
        INNER JOIN BrainsterDB.dbo.Customer AS c ON a.CustomerId = c.Id
        INNER JOIN BrainsterDB.dbo.Currency AS cu ON a.CurrencyId = cu.ID
        INNER JOIN BrainsterDB.dbo.Employee AS e ON ad.EmployeeID = e.ID
        INNER JOIN BrainsterDB.dbo.Location AS l ON ad.LocationId = l.Id
        INNER JOIN BrainsterDB.dbo.LocationType AS lt ON l.LocationTypeId = lt.Id
    )
    INSERT INTO fact.AccountDetails (CustomerKey,CurrencyKey,EmployeeKey,AccountKey,DateKey,CurrentBalance,InflowTransactionsQuantity,InflowAmount,OutflowTransactionsQuantity,OutflowAmount,OutflowTransactionsQuantityATM,OutflowAmountATM)
    SELECT
        c.CustomerKey,
        cu.CurrencyKey,
        e.EmployeeKey,
        a.AccountKey,
        d.DateKey,
        SUM(ad.Amount) OVER (PARTITION BY a.AccountID ORDER BY ad.TransactionDate) as CurrentBalance,
        COUNT(CASE WHEN ad.Amount > 0 THEN 1 END) as InflowTransactionsQuantity,
        SUM(CASE WHEN ad.Amount > 0 THEN ad.Amount ELSE 0 END) as InflowAmount,
        COUNT(CASE WHEN ad.Amount < 0 THEN 1 END) as OutflowTransactionsQuantity,
        SUM(CASE WHEN ad.Amount < 0 THEN ad.Amount ELSE 0 END) as OutflowAmount,
        COUNT(CASE WHEN ad.TransactionType = 'ATM' AND ad.Amount < 0 THEN 1 END) AS OutflowTransactionsQuantityATM,
        SUM(CASE WHEN ad.TransactionType = 'ATM' AND ad.Amount < 0 THEN ad.Amount ELSE 0 END) as OutflowAmountATM
    FROM [Data] AS ad
    INNER JOIN dimension.Customer AS c ON ad.CustomerID = c.CustomerID
    INNER JOIN dimension.Currency AS cu ON ad.CurrencyID = cu.CurrencyID
    INNER JOIN dimension.Employee AS e ON ad.EmployeeID = e.EmployeeID
    INNER JOIN dimension.Account AS a ON ad.AccountID = a.AccountID
    INNER JOIN dimension.[Date] AS d ON ad.TransactionDate = d.DateKey
    WHERE NOT EXISTS (
        SELECT 1
        FROM fact.AccountDetails AS f
        WHERE f.AccountKey = a.AccountKey
            AND f.DateKey = d.DateKey
    )
    GROUP BY
        c.CustomerKey,
        cu.CurrencyKey,
        e.EmployeeKey,
        a.AccountKey,
        d.DateKey,
        a.AccountID,
        ad.TransactionDate,
        ad.Amount,
        ad.TransactionType;
END;
GO

EXEC Integration.InsertFactAccountDetails_Incremental;                --EXECUTE
 
SELECT *
FROM 
	fact.AccountDetails
ORDER BY 
	AccountDetailsKey;
