SqlStream is an ADO.NET library intended to support Table-Valued Parameters (TVP) which were introduced in Sql Server 2008. The purpose is to provide support for TVPs with minimal setup and to be able to use TVPs as if you were writing to System.IO.Stream. 

While it is not actually possible to implement a class that inherits from System.IO.Stream, the semantics employed by SqlStream are the same.

For background see the Sql Server Central article (http://www.sqlservercentral.com/articles/SQL+Server+2008/66554/) that inspired this project.

To see how it is used, open the solution file (VS 2010), set SqlServerStreamTests as the startup project, hit F5 or click Debug -> Start Debugging, then once the NUnit GUI loads click "Run".

To actually use this library in a project you'll need to define the following:

1. A Sql Server User Defined Type (UDT): This will act as the TVP for your stored procedure.
2. A Stored Procedure which has one parameter which uses the UDT you defined in step #1.
3. Use the SqlStream<T> class like you would a SqlConnection object to setup your connection
4. Use the AddStructured<T> to add a structured SqlParameter to the internal SqlCommand of SqlStream<T>
5. Use the Map methods of the SqlStructuredParameterWrapper class to map your POCO or primitive type to the UDT you defined in step #1


Setup SQL Server:

```T-SQL
IF OBJECT_ID(N'dbo.TVPTestProc') IS NOT NULL
	DROP PROCEDURE dbo.[TVPTestProc]
GO
IF  EXISTS (SELECT * FROM sys.types st 
			JOIN sys.schemas ss ON st.schema_id = ss.schema_id 
			WHERE st.name = N'StreamSchema' AND ss.name = N'dbo')
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
```

Now use it like this:

```C#
int outputValue = 0;
using (SqlStream<StreamSchema> target = new SqlStream<StreamSchema>(
	new SqlStreamConnection("Server=(local);Database=tempdb;Trusted_Connection=Yes;"), 
	SqlStreamBehavior.CloseConnection, 10))
{
	target.StoredProcedureName = "dbo.TVPTestProc";

	// add structured parameter (with mapping)
	target.Parameters.AddStructured<StreamSchema>("@stream", "dbo.StreamSchema", target)
		.Map(src => src.Id, "Id", SqlDbType.Int)
		.Map(src => src.ProductName, "ProductName", SqlDbType.VarChar, 255)
		.Map(src => Convert.ToDecimal(src.Price), "Price", SqlDbType.Decimal, 9, 3);

	// add standard input parameter
	target.Parameters.Add("@userid", SqlDbType.Int).Value = 1;
	
	// add standard output parameter
	var output = target.Parameters.Add("@resultCount", SqlDbType.Int);
	output.Direction = ParameterDirection.InputOutput;
	output.Value = 0;

	for (int i = 0; i < expected; i++)
	{
		target.Write(new StreamSchema
		{
			Id = i,
			ProductName = String.Format("Product {0}", i),
			Price = (i + 1.0) - 0.01
		});
	}

	// make sure to wait for Close() or Dispose() before checking output parameters
	target.Close();
	outputValue = Convert.ToInt32(output.Value);
}
```