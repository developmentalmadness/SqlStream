using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Server;
using System.Data;

namespace DevelopMENTALMadness.Data.Sql.Tests
{
	public class StreamSchema : ISqlStreamRecord
	{
		private static SqlMetaData[] meta = new SqlMetaData[3];

		static StreamSchema()
		{
			meta[0] = new SqlMetaData("Id", SqlDbType.Int);
			meta[1] = new SqlMetaData("ProductName", SqlDbType.VarChar, 64);
			meta[2] = new SqlMetaData("Price", SqlDbType.Decimal, 9, 2);
		}

		public Int32 Id { get; set; }
		public String ProductName { get; set; }
		public Double Price { get; set; }

		#region ISqlStreamRecord Members

		public SqlDataRecord ToSqlDataRecord()
		{
			SqlDataRecord record = new SqlDataRecord(meta);
			record.SetInt32(0, Id);
			record.SetString(1, ProductName);
			record.SetDecimal(2, Convert.ToDecimal(Price));
			return record;
		}

		#endregion
	}
}
