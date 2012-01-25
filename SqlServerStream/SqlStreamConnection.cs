using System;
using System.Data;
using System.Data.SqlClient;

namespace DevelopMENTALMadness.Data.Sql
{
	public class SqlStreamConnection<T> : ISqlStreamConnection<T> where T : ISqlStreamRecord
	{
		public SqlStreamConnection()
			: this("SqlServer")
		{

		}

		public SqlStreamConnection(String connectionName)
		{
			ConnectionName = connectionName;
			StoredProcName = "dbo.SteamImport";
			ParameterName = "@stream";
			UserDefinedTypeName = "dbo.StreamSchema";
		}

		public String ConnectionName { get; private set; }

		public String StoredProcName { get; set; }
		public String ParameterName { get; set; }
		public String UserDefinedTypeName { get; set; }

		#region ISqlStreamConnection<T> Members

		public void ExecuteNonQuery(SqlStream<T> stream)
		{
			using (var connection = new SqlConnection(ConnectionName))
			{
				SqlCommand cmd = new SqlCommand(StoredProcName, connection);
				cmd.CommandType = CommandType.StoredProcedure;

				var p = new SqlParameter
				{
					ParameterName = ParameterName,
					TypeName = UserDefinedTypeName,
					SqlDbType = SqlDbType.Structured,
					Value = this
				};

				cmd.Parameters.Add(p);
				cmd.ExecuteNonQuery();
			}
		}

		#endregion
	}
}
