using System;
using System.Data;
using System.Data.SqlClient;

namespace DevelopMENTALMadness.Data.Sql
{
    public interface ISqlStreamConnection : IDisposable
    {
        void ExecuteNonQuery(SqlCommand cmd);
        SqlDataReader ExecuteReader(SqlCommand cmd);
        Object ExecuteScalar(SqlCommand cmd);
        void Open();
        void Close();
        ConnectionState State { get; }
    }

	public class SqlStreamConnection : ISqlStreamConnection
	{
        SqlConnection connection;

        public SqlStreamConnection()
			: this("SqlServer")
		{

        }

        public SqlStreamConnection(SqlConnection connection)
        {
            this.connection = connection;
        }

		public SqlStreamConnection(String connectionString)
		{
            this.connection = new SqlConnection(connectionString);
		}

        public void Open()
        {
            connection.Open();
        }

        public void Close()
        {
            connection.Close();
        }

        public void Dispose()
        {
            connection.Dispose();
        }

        public ConnectionState State { get { return connection.State; } }

        void ISqlStreamConnection.ExecuteNonQuery(SqlCommand cmd)
        {
            cmd.Connection = connection;
            if (connection.State != ConnectionState.Open)
                connection.Open();
            cmd.ExecuteNonQuery();
        }

        SqlDataReader ISqlStreamConnection.ExecuteReader(SqlCommand cmd)
        {
            cmd.Connection = connection;
            if (connection.State != ConnectionState.Open)
                connection.Open();
            return cmd.ExecuteReader();
        }

        object ISqlStreamConnection.ExecuteScalar(SqlCommand cmd)
        {
            cmd.Connection = connection;
            if (connection.State != ConnectionState.Open)
                connection.Open();
            return cmd.ExecuteScalar();
        }
    }
}
