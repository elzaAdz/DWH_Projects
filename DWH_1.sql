--1.Create database schemas.
CREATE SCHEMA dimension
GO

CREATE SCHEMA fact
GO

CREATE SCHEMA integration
GO

--2.Create the dimension tables.

DROP TABLE IF EXISTS dimension.Customer    
GO

CREATE TABLE dimension.Customer(
	CustomerKey int IDENTITY(1,1) NOT NULL,
	[CustomerId] [int]  NOT NULL,
	[FirstName] [nvarchar](100) NOT NULL,
	[LastName] [nvarchar](100) NOT NULL,
	[Gender] [nchar](1) NULL,
	[NationalIDNumber] [nvarchar](15) NULL,
	[DateOfBirth] [date] NULL,
	[RegionName] [nvarchar](100) NULL,
	[PhoneNumber] [nvarchar](20) NULL,
	[isActive] [bit] NOT NULL,
	CityName nvarchar(100) NULL,
	Region nvarchar(100) NULL,
	[Population] int NULL,
	EastWest char(1) NULL,
	CONSTRAINT [PK_Customer] PRIMARY KEY CLUSTERED (CustomerKey)
)
GO

--select * from dimension.Employee

DROP TABLE IF EXISTS dimension.Employee    
GO

CREATE TABLE dimension.Employee(
	EmployeeKey int IDENTITY(1,1) NOT NULL,
	[EmployeeID] [int] NOT NULL,
	[FirstName] [nvarchar](100) NOT NULL,
	[LastName] [nvarchar](100) NOT NULL,
	[NationalIDNumber] [nvarchar](15) NULL,
	[JobTitle] [nvarchar](50) NULL,
	[DateOfBirth] [date] NULL,
	[MaritalStatus] [nchar](1) NULL,
	[Gender] [nchar](1) NULL,
	[HireDate] [date] NULL,
	CityName nvarchar(100) NULL,
	Region nvarchar(100) NULL,
	[Population] int NULL,
	EastWest char(1) NULL,
	CONSTRAINT [PK_Employee] PRIMARY KEY CLUSTERED (EmployeeKey)
)
GO

--select * from dimension.Employee

DROP TABLE IF EXISTS dimension.Currency    
GO

CREATE TABLE dimension.Currency(
	CurrencyKey int IDENTITY(1,1) NOT NULL,
	[Currencyid] [int] NOT NULL,
	[Code] [nvarchar](5) NULL,
	[Name] [nvarchar](100) NULL,
	[ShortName] [nvarchar](20) NULL,
	[CountryName] [nvarchar](100) NULL,
    CONSTRAINT [PK_Currency] PRIMARY KEY CLUSTERED (CurrencyKey)
) 
GO

--select * from dimension.Currency

DROP TABLE IF EXISTS dimension.Account
GO

CREATE TABLE dimension.Account(
	AccountKey int IDENTITY(1,1) NOT NULL,
	AccountID int NOT NULL,
	AccountNumber nvarchar(20) NULL,
	AllowedOverdraft decimal(10,3) NULL,
	CONSTRAINT PK_Account PRIMARY KEY CLUSTERED (AccountKey)
)

--select * from dimension.Account

--https://www.mssqltips.com/sqlservertip/4054/creating-a-date-dimension-or-calendar-table-in-sql-server/
CREATE TABLE [dimension].[Date]
(
	[DateKey] Date NOT NULL
,	[Day] TINYINT NOT NULL
,	DaySuffix CHAR(2) NOT NULL
,	[Weekday] TINYINT NOT NULL
,	WeekDayName VARCHAR(10) NOT NULL
,	IsWeekend BIT NOT NULL
,	IsHoliday BIT NOT NULL
,	HolidayText VARCHAR(64) SPARSE
,	DOWInMonth TINYINT NOT NULL
,	[DayOfYear] SMALLINT NOT NULL
,	WeekOfMonth TINYINT NOT NULL
,	WeekOfYear TINYINT NOT NULL
,	ISOWeekOfYear TINYINT NOT NULL
,	[Month] TINYINT NOT NULL
,	[MonthName] VARCHAR(10) NOT NULL
,	[Quarter] TINYINT NOT NULL
,	QuarterName VARCHAR(6) NOT NULL
,	[Year] INT NOT NULL
,	MMYYYY CHAR(6) NOT NULL
,	MonthYear CHAR(7) NOT NULL
,	FirstDayOfMonth DATE NOT NULL
,	LastDayOfMonth DATE NOT NULL
,	FirstDayOfQuarter DATE NOT NULL
,	LastDayOfQuarter DATE NOT NULL
,	FirstDayOfYear DATE NOT NULL
,	LastDayOfYear DATE NOT NULL
,	FirstDayOfNextMonth DATE NOT NULL
,	FirstDayOfNextYear DATE NOT NULL
,	CONSTRAINT [PK_Date] PRIMARY KEY CLUSTERED 
	(
		[DateKey] ASC
	)
)
GO

--=========================================================================
--Creates Procedure for initial load Date Dimension
--=========================================================================
CREATE OR ALTER PROCEDURE [integration].[GenerateDimensionDate]
AS
BEGIN
	DECLARE
		@StartDate DATE = '2000-01-01'
	,	@NumberOfYears INT = 30
	,	@CutoffDate DATE;
	SET @CutoffDate = DATEADD(YEAR, @NumberOfYears, @StartDate);

	-- prevent set or regional settings from interfering with 
	-- interpretation of dates / literals
	SET DATEFIRST 7;
	SET DATEFORMAT mdy;
	SET LANGUAGE US_ENGLISH;

	-- this is just a holding table for intermediate calculations:
	CREATE TABLE #dim
	(
		[Date]       DATE        NOT NULL, 
		[day]        AS DATEPART(DAY,      [date]),
		[month]      AS DATEPART(MONTH,    [date]),
		FirstOfMonth AS CONVERT(DATE, DATEADD(MONTH, DATEDIFF(MONTH, 0, [date]), 0)),
		[MonthName]  AS DATENAME(MONTH,    [date]),
		[week]       AS DATEPART(WEEK,     [date]),
		[ISOweek]    AS DATEPART(ISO_WEEK, [date]),
		[DayOfWeek]  AS DATEPART(WEEKDAY,  [date]),
		[quarter]    AS DATEPART(QUARTER,  [date]),
		[year]       AS DATEPART(YEAR,     [date]),
		FirstOfYear  AS CONVERT(DATE, DATEADD(YEAR,  DATEDIFF(YEAR,  0, [date]), 0)),
		Style112     AS CONVERT(CHAR(8),   [date], 112),
		Style101     AS CONVERT(CHAR(10),  [date], 101)
	);

	-- use the catalog views to generate as many rows as we need
	INSERT INTO #dim ([date]) 
	SELECT
		DATEADD(DAY, rn - 1, @StartDate) as [date]
	FROM 
	(
		SELECT TOP (DATEDIFF(DAY, @StartDate, @CutoffDate)) 
			rn = ROW_NUMBER() OVER (ORDER BY s1.[object_id])
		FROM
			-- on my system this would support > 5 million days
			sys.all_objects AS s1
			CROSS JOIN sys.all_objects AS s2
		ORDER BY
			s1.[object_id]
	) AS x;
	-- select * from #dim

	INSERT dimension.[Date] ([DateKey], [Day], [DaySuffix], [Weekday], [WeekDayName], [IsWeekend], [IsHoliday], [HolidayText], [DOWInMonth], [DayOfYear], [WeekOfMonth], [WeekOfYear], [ISOWeekOfYear], [Month], [MonthName], [Quarter], [QuarterName], [Year], [MMYYYY], [MonthYear], [FirstDayOfMonth], [LastDayOfMonth], [FirstDayOfQuarter], [LastDayOfQuarter], [FirstDayOfYear], [LastDayOfYear], [FirstDayOfNextMonth], [FirstDayOfNextYear])
	SELECT
		--DateKey     = CONVERT(INT, Style112),
		[DateKey]        = [date],
		[Day]         = CONVERT(TINYINT, [day]),
		DaySuffix     = CONVERT(CHAR(2), CASE WHEN [day] / 10 = 1 THEN 'th' ELSE 
						CASE RIGHT([day], 1) WHEN '1' THEN 'st' WHEN '2' THEN 'nd' 
						WHEN '3' THEN 'rd' ELSE 'th' END END),
		[Weekday]     = CONVERT(TINYINT, [DayOfWeek]),
		[WeekDayName] = CONVERT(VARCHAR(10), DATENAME(WEEKDAY, [date])),
		[IsWeekend]   = CONVERT(BIT, CASE WHEN [DayOfWeek] IN (1,7) THEN 1 ELSE 0 END),
		[IsHoliday]   = CONVERT(BIT, 0),
		HolidayText   = CONVERT(VARCHAR(64), NULL),
		[DOWInMonth]  = CONVERT(TINYINT, ROW_NUMBER() OVER 
						(PARTITION BY FirstOfMonth, [DayOfWeek] ORDER BY [date])),
		[DayOfYear]   = CONVERT(SMALLINT, DATEPART(DAYOFYEAR, [date])),
		WeekOfMonth   = CONVERT(TINYINT, DENSE_RANK() OVER 
						(PARTITION BY [year], [month] ORDER BY [week])),
		WeekOfYear    = CONVERT(TINYINT, [week]),
		ISOWeekOfYear = CONVERT(TINYINT, ISOWeek),
		[Month]       = CONVERT(TINYINT, [month]),
		[MonthName]   = CONVERT(VARCHAR(10), [MonthName]),
		[Quarter]     = CONVERT(TINYINT, [quarter]),
		QuarterName   = CONVERT(VARCHAR(6), CASE [quarter] WHEN 1 THEN 'First' 
						WHEN 2 THEN 'Second' WHEN 3 THEN 'Third' WHEN 4 THEN 'Fourth' END), 
		[Year]        = [year],
		MMYYYY        = CONVERT(CHAR(6), LEFT(Style101, 2)    + LEFT(Style112, 4)),
		MonthYear     = CONVERT(CHAR(7), LEFT([MonthName], 3) + LEFT(Style112, 4)),
		FirstDayOfMonth     = FirstOfMonth,
		LastDayOfMonth      = MAX([date]) OVER (PARTITION BY [year], [month]),
		FirstDayOfQuarter   = MIN([date]) OVER (PARTITION BY [year], [quarter]),
		LastDayOfQuarter    = MAX([date]) OVER (PARTITION BY [year], [quarter]),
		FirstDayOfYear      = FirstOfYear,
		LastDayOfYear       = MAX([date]) OVER (PARTITION BY [year]),
		FirstDayOfNextMonth = DATEADD(MONTH, 1, FirstOfMonth),
		FirstDayOfNextYear  = DATEADD(YEAR,  1, FirstOfYear)
	FROM #dim
END
GO

delete from dimension.Date
GO
EXEC [integration].[GenerateDimensionDate]
GO
--select * from dimension.Date

--3.Create fact table.

DROP TABLE IF EXISTS fact.AccountDetails
GO

CREATE TABLE fact.AccountDetails(
	AccountDetailsKey bigint IDENTITY(1,1) not null,
	CustomerKey int not null,
	CurrencyKey int not null,
	EmployeeKey int not null,
	AccountKey int not null,
	DateKey date not null,
	CurrentBalance decimal(18,2) null,
	InflowTransactionsQuantity int not null,
	InflowAmount decimal(18,2) not null,
	OutflowTransactionsQuantity int not null,
	OutflowAmount decimal(18,2) not null,
	OutflowTransactionsQuantityATM int not null,
	OutflowAmountATM decimal(18,2) not null,
	CONSTRAINT PK_Account PRIMARY KEY CLUSTERED (AccountDetailsKey)
)
GO

SELECT * FROM fact.AccountDetails

--4.Add foreign keys.


ALTER TABLE fact.AccountDetails 
ADD CONSTRAINT FK_AccountDetails_Account
FOREIGN KEY (AccountKey)
REFERENCES dimension.Account(AccountKey)
GO

ALTER TABLE fact.AccountDetails
ADD CONSTRAINT FK_AccountDetails_Customer
FOREIGN KEY (CustomerKey)
REFERENCES dimension.Customer(CustomerKey)
GO

ALTER TABLE fact.AccountDetails 
ADD CONSTRAINT FK_AccountDetails_Currency
FOREIGN KEY (CurrencyKey)
REFERENCES dimension.Currency(CurrencyKey)
GO

ALTER TABLE fact.AccountDetails 
ADD CONSTRAINT FK_AccountDetails_Employee
FOREIGN KEY (EmployeeKey)
REFERENCES dimension.Employee(EmployeeKey)
GO

ALTER TABLE fact.AccountDetails 
ADD CONSTRAINT FK_AccountDetails_Date
FOREIGN KEY (DateKey)
REFERENCES dimension.[Date](DateKey)
GO

--Initial load for dimension tables.

CREATE PROCEDURE CurrencyInsert
AS
BEGIN
	INSERT INTO 
		dimension.Currency(Currencyid, Code, [Name], ShortName, CountryName)
	SELECT 
		c.id, c.Code,c.[Name], c.ShortName, c.CountryName
	FROM 
		BrainsterDB.dbo.Currency as c
END

EXEC CurrencyInsert
GO

CREATE PROCEDURE EmployeeInsert
AS
BEGIN
	INSERT INTO 
		dimension.Employee(EmployeeID, FirstName, LastName, NationalIDNumber, JobTitle, DateOfBirth, MaritalStatus, Gender, HireDate, CityName, Region, [Population], EastWest)
	SELECT 
		e.ID, e.FirstName,e.LastName,e.NationalIDNumber, e.JobTitle, e.DateOfBirth, e.MaritalStatus, e.Gender, e.HireDate, c.CityName, c.Region, c.[Population], c.EastWest
	FROM 
		BrainsterDB.dbo.Employee as e
		INNER JOIN BrainsterDB.dbo.City as c ON e.CityId=c.ID
END

EXEC EmployeeInsert
GO

CREATE PROCEDURE CustomerInsert
AS
BEGIN
	INSERT INTO 
		dimension.Customer(CustomerId, FirstName, LastName, Gender, NationalIDNumber, DateOfBirth, PhoneNumber, isActive, CityName, Region, [Population], EastWest)
	SELECT 
		c.ID, c.FirstName, c.LastName,c.Gender, c.NationalIDNumber, c.DateOfBirth, c.PhoneNumber,c.isActive, ci.CityName, ci.Region, ci.[Population], ci.EastWest
	FROM 
		BrainsterDB.dbo.Customer as c
		INNER JOIN BrainsterDB.dbo.City as ci ON c.CityId=ci.ID
END
 
EXEC CustomerInsert
GO

CREATE PROCEDURE AccountInsert
AS
BEGIN
	INSERT INTO 
		dimension.Account(AccountID, AccountNumber, AllowedOverdraft)
	SELECT 
	    a.Id, a.AccountNumber, a.AllowedOverdraft
	FROM
		BrainsterDB.dbo.Account as a	
END

EXEC AccountInsert
GO

--SELECT * FROM dimension.Account
--SELECT * FROM dimension.Currency
--SELECT * FROM dimension.Customer
--SELECT * FROM dimension.Employee

--Insertion-fact table.

CREATE PROCEDURE fact.InsertFactAccountDetails
AS
BEGIN
	WITH CTE 
	AS
	(
		SELECT a.CustomerId, a.CurrencyId, a.EmployeeId, a.id as AccID, ad.TransactionDate,
		ROW_NUMBER() OVER (PARTITION BY a.id, d.LastDayOfMonth ORDER BY ad.TransactionDate) as RN,
		d.DateKey, d.LastDayOfMonth, d.FirstDayOfMonth

		FROM BrainsterDB.dbo.AccountDetails as ad
		INNER JOIN BrainsterDB.dbo.Account as a ON ad.AccountId=a.id
		INNER JOIN dimension.[Date] as d ON ad.TransactionDate=d.DateKey
	
		--where a.id=1
	)
	insert into fact.AccountDetails([CustomerKey], [CurrencyKey], [EmployeeKey],AccountKey, [DateKey], [CurrentBalance], [InflowTransactionsQuantity], [InflowAmount], [OutflowTransactionsQuantity], [OutflowAmount], [OutflowTransactionsQuantityATM], [OutflowAmountATM])
	SELECT
		c.CustomerKey, cu.CurrencyKey, e.EmployeeKey, a.AccountKey, CTE.LastDayOfMonth,
		(
			SELECT
				SUM(ad.Amount)
			FROM 
				BrainsterDB.dbo.AccountDetails as ad
			WHERE 
				CTE.AccID=ad.AccountId AND
				ad.TransactionDate<=CTE.LastDayOfMonth
		) as CurrentBalance, 
		(
			SELECT
				COUNT(ad.Amount)
			FROM
				BrainsterDB.dbo.AccountDetails as ad
			WHERE
				ad.AccountId = cte.AccID
				AND ad.TransactionDate between cte.FirstDayOfMonth and cte.LastDayOfMonth
				AND ad.Amount > 0
		) as InflowTransactionsQuantity,
		(
			SELECT
				ISNULL(sum(ad.Amount),0)
			FROM
				BrainsterDB.dbo.AccountDetails as ad
			WHERE
				ad.AccountId = cte.AccID
				AND ad.TransactionDate between cte.FirstDayOfMonth and cte.LastDayOfMonth
				AND ad.Amount > 0
		) as InflowAmount,
		(
			SELECT
				COUNT(ad.Amount)
			FROM
				BrainsterDB.dbo.AccountDetails as ad
			WHERE
				ad.AccountId = cte.AccID
				AND ad.TransactionDate between cte.FirstDayOfMonth and cte.LastDayOfMonth
				AND ad.Amount < 0
		) as OutflowTransactionsQuantity,
		(
			SELECT
				ISNULL(sum(ad.Amount),0)
			FROM
				BrainsterDB.dbo.AccountDetails as ad
			WHERE
				ad.AccountId = cte.AccID
				and ad.TransactionDate between cte.FirstDayOfMonth and cte.LastDayOfMonth
				and ad.Amount < 0
		) as OutflowAmount,
		(
			SELECT
				COUNT(ad.Amount)
			FROM
				BrainsterDB.dbo.AccountDetails as ad
				INNER JOIN BrainsterDB.dbo.Location as l ON ad.LocationId=l.id
				INNER JOIN BrainsterDB.dbo.LocationType as lt ON l.LocationTypeId=lt.Id
			WHERE
				ad.AccountId = cte.AccID
				AND ad.TransactionDate between cte.FirstDayOfMonth and cte.LastDayOfMonth
				AND ad.Amount < 0
				AND lt.[Name]='ATM'
		) as OutflowTransactionsQuantityATM,
		(
			SELECT
				ISNULL(sum(ad.Amount),0)
			FROM
				BrainsterDB.dbo.AccountDetails as ad
				INNER JOIN BrainsterDB.dbo.Location as l ON ad.LocationId=l.id
				INNER JOIN BrainsterDB.dbo.LocationType as lt ON l.LocationTypeId=lt.Id
			WHERE
				ad.AccountId = cte.AccID
				and ad.TransactionDate between cte.FirstDayOfMonth and cte.LastDayOfMonth
				and ad.Amount < 0
				AND lt.[Name]='ATM'
		)as OutflowAmountATM
	FROM 
		CTE 
		LEFT OUTER JOIN dimension.Customer as c ON CTE.CustomerId=c.CustomerID
		LEFT OUTER JOIN dimension.Currency as cu ON CTE.CurrencyId=cu.Currencyid
		LEFT OUTER JOIN dimension.Employee as e ON CTE.EmployeeId=e.EmployeeID
		LEFT OUTER JOIN dimension.Account as a ON CTE.AccID=a.AccountID
	WHERE RN=1
END

truncate table fact.Account
EXEC fact.InsertFactAccountDetails
select * from fact.AccountDetails