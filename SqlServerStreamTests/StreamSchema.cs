using System;
using System.Data;
using Microsoft.SqlServer.Server;

namespace DevelopMENTALMadness.Data.Sql.Tests
{
	public class StreamSchema
	{
		public Int32 Id { get; set; }
		public String ProductName { get; set; }
		public Double Price { get; set; }
	}
}
