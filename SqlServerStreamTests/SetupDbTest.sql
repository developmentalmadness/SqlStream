DECLARE @productversion VARCHAR(2)
SELECT @productversion = CONVERT(VARCHAR(2), SERVERPROPERTY('productversion'))

IF CONVERT(INT, REPLACE(@productversion, '.', '')) < 10 BEGIN
	RAISERROR ('Sql Server 2008 or later required.', 16, 1)
END
GO

IF OBJECT_ID(N'dbo.TVPTestProc') IS NOT NULL
	DROP PROCEDURE dbo.[TVPTestProc]
GO
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'StreamSchema' AND ss.name = N'dbo')
	DROP TYPE [dbo].[StreamSchema]
GO

IF OBJECT_ID(N'dbo.TestTable') IS NOT NULL
	DROP TABLE dbo.TestTable
GO

CREATE TABLE dbo.TestTable (
	[ID] INT NOT NULL,
	[ProductName] VARCHAR(255) NOT NULL,
	[Price] DECIMAL(9,3) NOT NULL
)

CREATE TYPE [dbo].[StreamSchema] AS TABLE(
	[ID] INT NOT NULL,
	[ProductName] VARCHAR(255) NOT NULL,
	[Price] DECIMAL(9,3) NOT NULL
)
GO

CREATE PROCEDURE dbo.[TVPTestProc]
	@stream dbo.StreamSchema READONLY,
	@userid INT,
	@resultCount INT = NULL OUTPUT
AS
	SET NOCOUNT ON

	INSERT INTO TestTable
		SELECT Id, ProductName, Price FROM @stream
		
	
	SELECT @resultCount = COUNT(*) FROM TestTable
GO
