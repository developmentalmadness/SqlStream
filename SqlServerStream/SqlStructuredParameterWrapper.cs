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
    /// <summary>
    /// Wraps a <see cref="System.Data.SqlClient.SqlParameter"/> with a <see cref="System.Data.SqlDbType"/> of SqlDbType.Structured
    /// and provides Map methods and extension methods for building the Parameters collection for a <see cref="System.Data.SqlClient.SqlCommand"/>.
    /// </summary>
    /// <exception cref="System.Exception">The order of the parameters passed to a Table-Valued Parameter is important. If your parameters aren't created in the same order it will result in an exception.</exception>
    /// <example>
    /// An array of primitive types:
    /// 
    /// SqlCommand cmd = new SqlCommand();
    /// cmd.Parameters.AddStructured&lt;long&gt;("@TVPParam", "dbo.UDTName", new List&lt;long&gt;{ /* pretend there's data here */ })
    ///     .Map(id = &gt; id, "IdColumn", SqlDbType.BigInt);
    /// 
    /// An array of POCO objects:
    /// 
    /// SqlCommand cmd = new SqlCommand();
    /// cmd.Parameters.AddStructured&lt;MyClass&gt;("@TVPParam", "dbo.UDTName", new List&lt;MyClass&gt;{ /* pretend there's data here */ })
    /// 	.Map(src =&gt; src.Id, "IdColumn", SqlDbType.BigInt)
    /// 	.Map(src =&gt; src.Property1, "Column1", SqlDbType.VarChar, 255)
    /// 	.Map(src =&gt; src.Property2, "Column2", SqlDbType.Decimal, 9, 3);
    /// 
    /// A complex Func&lt;T, Object%gt; used for the map function:
    /// 
    /// SqlCommand cmd = new SqlCommand();
    /// cmd.Parameters.AddStructured&lt;MyClass&gt;("@TVPParam", "dbo.UDTName", new List&lt;MyClass&gt;{ /* pretend there's data here */ })
    /// 	.Map(src =&gt; src.Id, "IdColumn", SqlDbType.BigInt)
    /// 	.Map(src =&gt; String.Format("{0} - ({1:C})", src.Property1), "Column1", SqlDbType.VarChar, 255)
    /// 	.Map(src =&gt; src.Property2, "Column2", SqlDbType.Decimal, 9, 3);
    /// 
    /// A multi-line Func&lt;T, Object%gt; used for the map function:
    /// 
    /// var someKey = "Foo";
    /// SqlCommand cmd = new SqlCommand();
    /// cmd.Parameters.AddStructured&lt;MyClass&gt;("@TVPParam", "dbo.UDTName", new List&lt;MyClass&gt;{ /* pretend there's data here */ })
    /// 	.Map(src =&gt; src.Id, "IdColumn", SqlDbType.BigInt)
    /// 	.Map(src =&gt; {
    /// 			var name = String.Format("{0} - ({1:C})", src.Property1);
    /// 			if(name.BeginsWith(someKey))
    /// 				name.Replace(someKey, "Bar");
    /// 			return name;
    /// 		}, "Column1", SqlDbType.VarChar, 255)
    /// 	.Map(src =&gt; src.Property2, "Column2", SqlDbType.Decimal, 9, 3);
    /// 
    /// </example>
    /// <typeparam name="T">The POCO class or primitive type being mapped to the TVP.</typeparam>
    public class SqlStructuredParameterWrapper<T> : IEnumerable<SqlDataRecord>
    {
        private IEnumerable<T> data;
        private List<SqlMetaData> meta = new List<SqlMetaData>();
        private List<Func<T, Object>> maps = new List<Func<T, Object>>();

        public SqlStructuredParameterWrapper(IEnumerable<T> data)
        {
            this.data = data;
        }

        IEnumerator<SqlDataRecord> IEnumerable<SqlDataRecord>.GetEnumerator()
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
            return ((IEnumerable<SqlDataRecord>)this).GetEnumerator();
        }

        /// <summary>
        /// Maps type property or array value to Sql UDT column (IMPORTANT: must match the order of columns in the UDT type)
        /// </summary>
        /// <exception cref="System.Exception">The order of the parameters passed to a Table-Valued Parameter is important. If your parameters aren't created in the same order it will result in an exception.</exception>
        /// <param name="map">Mapping function, the function to call when SqlCommand iterates over array and sets the value of the UDT column specified by <paramref name="name"/>.</param>
        /// <param name="name">The name of the UDT column that will be updated by <paramref name="map"/>.</param>
        /// <param name="type">The SqlDbType of the UDT column specified by <paramref name="name"/>.</param>
        /// <returns>a wrapper for the structured <see cref="System.Data.SqlClient.SqlParameter"/> that manages both the parameter and the <see cref="Microsoft.SqlServer.Server.SqlMetaData"/> definition.</returns>
        public SqlStructuredParameterWrapper<T> Map(Func<T, Object> map, String name, SqlDbType type)
        {
            meta.Add(new SqlMetaData(name, type));
            maps.Add(map);
            return this;
        }

        /// <summary>
        /// Maps type property or array value to Sql UDT column (IMPORTANT: must match the order of columns in the UDT type)
        /// </summary>
        /// <exception cref="System.Exception">The order of the parameters passed to a Table-Valued Parameter is important. If your parameters aren't created in the same order it will result in an exception.</exception>
        /// <param name="map">Mapping function, the function to call when SqlCommand iterates over array and sets the value of the UDT column specified by <paramref name="name"/>.</param>
        /// <param name="name">The name of the UDT column that will be updated by <paramref name="map"/>.</param>
        /// <param name="type">The SqlDbType of the UDT column specified by <paramref name="name"/>.</param>
        /// <param name="size">The size of the UDT column specificed by <paramref name="name"/>.</param>
        /// <returns>a wrapper for the structured <see cref="System.Data.SqlClient.SqlParameter"/> that manages both the parameter and the <see cref="Microsoft.SqlServer.Server.SqlMetaData"/> definition.</returns>
        public SqlStructuredParameterWrapper<T> Map(Func<T, Object> map, String name, SqlDbType type, Int64 size)
        {
            meta.Add(new SqlMetaData(name, type, size));
            maps.Add(map);
            return this;
        }

        /// <summary>
        /// Maps type property or array value to Sql UDT column (IMPORTANT: must match the order of columns in the UDT type)
        /// </summary>
        /// <exception cref="System.Exception">The order of the parameters passed to a Table-Valued Parameter is important. If your parameters aren't created in the same order it will result in an exception.</exception>
        /// <param name="map">Mapping function, the function to call when SqlCommand iterates over array and sets the value of the UDT column specified by <paramref name="name"/>.</param>
        /// <param name="name">The name of the UDT column that will be updated by <paramref name="map"/>.</param>
        /// <param name="type">The SqlDbType of the UDT column specified by <paramref name="name"/>.</param>
        /// <param name="precision">The precision of the UDT column specified by <paramref name="name"/>.</param>
        /// <param name="scale">The scale of the UDT column specified by <paramref name="name"/>.</param>
        /// <returns>a wrapper for the structured <see cref="System.Data.SqlClient.SqlParameter"/> that manages both the parameter and the <see cref="Microsoft.SqlServer.Server.SqlMetaData"/> definition.</returns>
        public SqlStructuredParameterWrapper<T> Map(Func<T, Object> map, String name, SqlDbType type, Byte precision, Byte scale)
        {
            meta.Add(new SqlMetaData(name, type, precision, scale));
            maps.Add(map);
            return this;
        }
    }

    public static class SqlParameterHelper
    {
        public static SqlParameter Add(this ICollection<SqlParameter> collection, String name, SqlDbType type)
        {
            var p = new SqlParameter(name, type);
            collection.Add(p);
            return p;
        }

        public static SqlParameter Add(this ICollection<SqlParameter> collection, String name, SqlDbType type, Int32 size)
        {
            var p = new SqlParameter(name, type, size);
            collection.Add(p);
            return p;
        }

        public static SqlParameter Add(this ICollection<SqlParameter> collection, String name, SqlDbType type, Byte precision, Byte scale)
        {
            var p = new SqlParameter(name, type)
            {
                Precision = precision,
                Scale = scale
            };
            collection.Add(p);
            return p;
        }

        /// <summary>
        /// Wraps a <see cref="System.Data.SqlClient.SqlParameter"/> with a <see cref="System.Data.SqlDbType"/> of SqlDbType.Structured
        /// and provides Map methods and extension methods for building the Parameters collection for a <see cref="System.Data.SqlClient.SqlCommand"/>.
        /// </summary>
        /// <exception cref="System.Exception">The order of the parameters passed to a Table-Valued Parameter is important. If your parameters aren't created in the same order it will result in an exception.</exception>
        /// <typeparam name="T">The POCO class or primitive type being mapped to the TVP.</typeparam>
        public static SqlStructuredParameterWrapper<T> AddStructured<T>(this ICollection<SqlParameter> collection, String name, String udtTypeName, IEnumerable<T> data)
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

        /// <summary>
        /// Wraps a <see cref="System.Data.SqlClient.SqlParameter"/> with a <see cref="System.Data.SqlDbType"/> of SqlDbType.Structured
        /// and provides Map methods and extension methods for building the Parameters collection for a <see cref="System.Data.SqlClient.SqlCommand"/>.
        /// </summary>
        /// <exception cref="System.Exception">The order of the parameters passed to a Table-Valued Parameter is important. If your parameters aren't created in the same order it will result in an exception.</exception>
        /// <typeparam name="T">The POCO class or primitive type being mapped to the TVP.</typeparam>
        public static SqlStructuredParameterWrapper<T> AddStructured<T>(this SqlParameterCollection collection, String name, String udtTypeName, IEnumerable<T> data)
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
}
