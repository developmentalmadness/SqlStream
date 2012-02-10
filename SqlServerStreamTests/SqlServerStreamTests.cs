﻿using System;
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
		}

		[Test]
		public void Test1()
		{
			int actual = 0;
			int expected = 100;

			var connection = new Mock<ISqlStreamConnection<StreamSchema>>();
			connection.Setup(c => c.ExecuteNonQuery(It.IsAny<SqlStream<StreamSchema>>())).Callback<SqlStream<StreamSchema>>((stream) =>
			{
				foreach (var item in stream)
					actual++;
			});

			SqlStream<StreamSchema> target = new SqlStream<StreamSchema>(connection.Object, 10);
			for (int i = 0; i < expected; i++)
			{
				target.Write(new StreamSchema
				{
					Id = i,
					ProductName = String.Format("Product {0}", i),
					Price = (Double)(Convert.ToDecimal(i + 1) - 0.01m)
				});

				//simulate I/O
				Thread.Sleep(3);
			}
			target.Close();

			Assert.AreEqual(expected, actual, "Data wasn't streamed.");
		}
	}
}
