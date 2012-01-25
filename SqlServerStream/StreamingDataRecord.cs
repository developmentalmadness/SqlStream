using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Server;
using System.Data;
using System.Collections;

namespace SqlServerStream
{
	
	public abstract class SqlStreamCursor<T> : IEnumerable<T> where T : SqlDataRecord
	{
		SqlMetaData[] columnStructure;

		public SqlStreamCursor()
		{
		}

		public IEnumerator<T> GetEnumerator()
		{
			while (!fileReader.EndOfStream)
			{
				inputRow = fileReader.ReadLine();
				inputColumns = inputRow.Split('\t');
				SqlDataRecord dataRecord = new SqlDataRecord(columnStructure);
				dataRecord.SetInt32(0, Int32.Parse(inputColumns[0]));
				dataRecord.SetString(1, inputColumns[1]);
				yield return dataRecord;
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}
}
