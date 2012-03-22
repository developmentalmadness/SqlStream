using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.SqlServer.Server;

namespace DevelopMENTALMadness.Data.Sql
{
    public static class SqlStructuredParameterHelper
    {
        public static SqlStructuredParameterWrapper<T> AddStructured<T>(this SqlParameterCollection collection, String name, String udtTypeName, IEnumerable<T> data) where T : class
        {
            var wrapper = new SqlStructuredParameterWrapper<T>(data);
            var p = new SqlParameter
            {
                SqlDbType = SqlDbType.Structured,
                ParameterName = name,
                TypeName = udtTypeName,
                Value = wrapper
            };

            collection.Add(p);

            return wrapper;
        }
    }

    public class SqlStructuredParameterWrapper<T> : IEnumerable<SqlDataRecord> where T : class
    {
        private IEnumerable<T> data;
        private List<SqlMetaData> meta = new List<SqlMetaData>();
        private List<Func<T, Object>> maps = new List<Func<T, Object>>();

        public SqlStructuredParameterWrapper(IEnumerable<T> data)
        {
            this.data = data;
        }

        public IEnumerator<SqlDataRecord> GetEnumerator()
        {
            var schema = meta.ToArray();
            foreach (T item in data)
            {
                var record = new SqlDataRecord(schema);
                for (int i = 0; i < maps.Count; i++)
                {
                    record.SetValue(i, maps[i].Invoke(item));
                }

                yield return record;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public SqlStructuredParameterWrapper<T> Map(Func<T, Object> map, String name, SqlDbType type)
        {
            
            meta.Add(new SqlMetaData(name, type));
            maps.Add(map);
            return this;
        }

        public SqlStructuredParameterWrapper<T> Map(Func<T, Object> map, String name, SqlDbType type, Int64 size)
        {
            meta.Add(new SqlMetaData(name, type, size));
            maps.Add(map);
            return this;
        }

        public SqlStructuredParameterWrapper<T> Map(Func<T, Object> map, String name, SqlDbType type, Byte precision, Byte scale)
        {
            meta.Add(new SqlMetaData(name, type, precision, scale));
            maps.Add(map);
            return this;
        }
    }
}
