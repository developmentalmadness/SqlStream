using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using log4net.Appender;
using log4net.Config;
using Moq;
using NUnit.Framework;

namespace DevelopMENTALMadness.Data.Sql.Tests
{
	[TestFixture]
	public class SqlStreamTests
	{
		[TestFixtureSetUp]
		public void Init()
		{
			BasicConfigurator.Configure(new ConsoleAppender());

            string[] script = null;
            using (var file = File.OpenText(".\\SetupDbTest.sql"))
            {
                script = file.ReadToEnd().Split(new string[] { "\r\nGO\r\n" }, StringSplitOptions.None);
            }

            using (SqlConnection conn = new SqlConnection("Server=(local);Database=tempdb;Trusted_Connection=Yes;"))
            {
                conn.Open();

                foreach (string text in script)
                {
                    if (String.IsNullOrEmpty(text))
                        continue;

                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = text;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                }
            }
		}

        [Test]
        public void SqlStream_Integration_Test_With_Output_Parameter()
        {
            int actual = 0;
            int expected = 100;

            using (SqlStream<StreamSchema> target = new SqlStream<StreamSchema>(new SqlStreamConnection("Server=(local);Database=tempdb;Trusted_Connection=Yes;"), SqlStreamBehavior.CloseConnection, 10))
            {
                target.StoredProcedureName = "dbo.TVPTestProc";

                target.Parameters.AddStructured<StreamSchema>("@stream", "dbo.StreamSchema", target)
                    .Map(src => src.Id, "Id", SqlDbType.Int)
                    .Map(src => src.ProductName, "ProductName", SqlDbType.VarChar, 255)
                    .Map(src => Convert.ToDecimal(src.Price), "Price", SqlDbType.Decimal, 9, 3);

                target.Parameters.Add("@userid", SqlDbType.Int).Value = 1;
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

                // need to wait for Close() or Dispose() before checking output parameters
                target.Close();
                actual = Convert.ToInt32(output.Value);
            }

            Assert.AreEqual(expected, actual, "Data wasn't streamed.");
        }

		[Test]
		public void SqlStream_ExecuteNonQuery_Enumerates_Entire_Stream()
		{
			int actual = 0;
			int expected = 100;

			var connection = new Mock<ISqlStreamConnection>();
            connection.Setup(c => c.ExecuteNonQuery(It.IsAny<SqlCommand>())).Callback<SqlCommand>((cmd) =>
                {
                    var stream = cmd.Parameters["@stream"].Value as SqlStructuredParameterWrapper<StreamSchema>;
                    Assert.IsNotNull(stream, "@stream parameter cannot be null.");

                    foreach (var item in stream)
                        actual++;
                });

            using (SqlStream<StreamSchema> target = new SqlStream<StreamSchema>(connection.Object, SqlStreamBehavior.CloseConnection, 10))
            {
                target.Parameters.Add("@userid", SqlDbType.Int).Value = 1;
                target.Parameters.AddStructured<StreamSchema>("@stream", "dbo.StreamUDT", target)
                    .Map(src => src.Id, "Id", SqlDbType.Int)
                    .Map(src => src.ProductName, "ProductName", SqlDbType.VarChar, 255)
                    .Map(src => Convert.ToDecimal(src.Price), "Price", SqlDbType.Decimal, 9, 3);
                
                for (int i = 0; i < expected; i++)
                {
                    target.Write(new StreamSchema
                    {
                        Id = i,
                        ProductName = String.Format("Product {0}", i),
                        Price = (i + 1.0) - 0.01
                    });

                    //simulate I/O
                    Thread.Sleep(3);
                }
            }

			Assert.AreEqual(expected, actual, "Data wasn't streamed.");
		}
	}
}
