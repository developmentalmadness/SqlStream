using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using Common.Logging;
using Microsoft.SqlServer.Server;

namespace DevelopMENTALMadness.Data.Sql
{
    public enum SqlStreamBehavior
    {
        None = 0x00,
        CloseConnection = 0x01,
        ReturnResults = 0x02,
        ReturnScalar = 0x04
    }

	/// <summary>
	/// A bulk-import (streaming) implementation for use with Sql 2008
	/// based on CLR (ADO.NET) support for Table-Valued Parameters (TVP).
	/// <see cref="http://www.sqlservercentral.com/articles/SQL+Server+2008/66554/"/>
	/// </summary>
	/// <typeparam name="T">A class that implements ISqlStreamRecord</typeparam>
	public class SqlStream<T> : IDisposable, IEnumerable<T>
	{
		private static ILog logger = LogManager.GetCurrentClassLogger();
        ISqlStreamConnection connection;
        SqlCommand command;
        SqlDataReader reader;
        Object scalarResult = null;
        SqlStreamBehavior behavior;
        private bool closed = false;

		// stream buffer members
		private Queue<IEnumerable<T>> chunks = new Queue<IEnumerable<T>>();
		private List<T> buffer = new List<T>();
		private int bufferSize = 10;

		// internal events
		private ManualResetEventSlim enqueued = new ManualResetEventSlim(false);
		private ManualResetEventSlim completed = new ManualResetEventSlim(false);

		// lock-free members
		private int threadStarts = 0;
		private int queueLocked = 0;
		private int finalizeStream = 0;

        public SqlStream(ISqlStreamConnection connection)
            : this(connection, SqlStreamBehavior.None)
        {

        }

        public SqlStream(ISqlStreamConnection connection, SqlStreamBehavior behavior)
		{
			this.connection = connection;
            this.command = new SqlCommand();
            this.command.CommandType = CommandType.StoredProcedure;
            this.behavior = behavior;
			buffer = new List<T>(bufferSize);
		}

        public SqlStream(ISqlStreamConnection connection, SqlStreamBehavior behavior, int bufferSize)
            : this(connection, behavior)
        {
            buffer = new List<T>(bufferSize);
        }

        public SqlParameterCollection Parameters { get { return command.Parameters; } }
        public String StoredProcedureName { get { return command.CommandText; } set { command.CommandText = value; } }
        public Int32 CommandTimeout { get { return command.CommandTimeout; } set { command.CommandTimeout = value; } }

		/// <summary>
		/// Write record to stream buffer
		/// </summary>
		/// <remarks>
		/// This method assumes a single writer thread, the 
		/// thread locks are only currently in place to allow
		/// writing to the buffer and streaming to the database
		/// on separate threads.
		/// </remarks>
		/// <param name="item"></param>
		public void Write(T item)
		{
			buffer.Add(item);
			if (buffer.Count >= bufferSize)
			{
				logger.Debug("Buffer full, flushing");
				Flush();
			}	
		}

		public void Flush()
		{
			EnqueueCurrent();
			int alreadyStarted = Interlocked.CompareExchange(ref threadStarts, 1, 0);
			if (alreadyStarted == 0)
			{
				logger.Debug("Staring background thread.");
				ThreadPool.QueueUserWorkItem(new WaitCallback(Execute));
			}
		}

		public void Close()
		{
            if (!closed)
            {
                // flush buffer and wait
                logger.Debug("Closing stream, set finalize flag and wait");
                Interlocked.Increment(ref finalizeStream);
                Flush();
                completed.Wait();
                if (behavior == SqlStreamBehavior.CloseConnection)
                    connection.Close();
                logger.Debug("Finished closing stream.");
                closed = true;
            }
		}

        public SqlDataReader ExecuteReader()
        {
            Close();
            return reader;
        }

        public Object ExecuteScalar()
        {
            Close();
            return scalarResult;
        }

		#region IDisposable Members
		private bool disposed = false;

		public void Dispose()
		{
			if (disposed)
				throw new ObjectDisposedException(typeof(SqlStream<T>).Name);

			disposed = true;

			Close();

            if (behavior == SqlStreamBehavior.CloseConnection)
                connection.Dispose();
		}

		#endregion

		private void EnqueueCurrent()
		{
			var wait = new SpinWait();
			while (true)
			{
				// try and aquire lock
				logger.Debug("Try to aquire queue lock...");
				int alreadyLocked = Interlocked.CompareExchange(ref queueLocked, 1, 0);
				if (alreadyLocked == 1)
				{
					// wait and try again
					logger.Debug("Spinning queue lock...");
					wait.SpinOnce();
					continue;
				}

				// enqueue current buffer and create a new one
				logger.Debug("Queue lock aquired.");
				chunks.Enqueue(buffer);
				buffer = new List<T>();

				// release the lock
				logger.Debug("Queue lock released.");
				Interlocked.Decrement(ref queueLocked);
				break;
			}

			logger.Debug("Reset enqueued.");
			enqueued.Set();
		}

		private IEnumerable<T> DequeueNext()
		{
			var wait = new SpinWait();
			while (true)
			{
				// try and aquire lock
				logger.Debug("Try to aquire queue lock...");
				int alreadLocked = Interlocked.CompareExchange(ref queueLocked, 1, 0);
				if (alreadLocked == 1)
				{
					logger.Debug("Spinnning queue lock...");
					wait.SpinOnce();
					continue;
				}

				// try and dequeue next
				logger.Debug("Queue lock aquired.");
				IEnumerable<T> next = null;
				if (chunks.Count != 0)
				{
					logger.Debug("Item dequeued.");
					next = chunks.Dequeue();
				}
				else
				{
					logger.Debug("Queue is empty, nothing dequeued.");
				}

				// release lock
				logger.Debug("Queue lock released.");
				Interlocked.Decrement(ref queueLocked);

				return next;
			}
		}

		private void Execute(Object state)
		{
			logger.Debug("Background thread started.");

            if (connection.State != ConnectionState.Open)
                connection.Open();

            switch (behavior)
            {
                default:
                    connection.ExecuteNonQuery(command);
                    break;
                case SqlStreamBehavior.ReturnResults:

                    reader = connection.ExecuteReader(command);
                    break;
                case SqlStreamBehavior.ReturnScalar:
                    scalarResult = connection.ExecuteScalar(command);
                    break;
            }

            completed.Set();
        }

		#region IEnumerable<SqlDataRecord> Members

		IEnumerator<T> IEnumerable<T>.GetEnumerator()
		{
			while (finalizeStream == 0 || chunks.Count != 0)
			{
				enqueued.Wait();

				logger.Debug("Get next chunk.");
				var e = DequeueNext();
				foreach(var i in e)
				{
					yield return i;	
				}

				if (chunks.Count == 0)
				{
					enqueued.Reset();
					logger.Debug("Waiting for next chunk or finalize");
				}
			}

			logger.Debug("End of stream");
		}

		#endregion

		#region IEnumerable Members

		IEnumerator IEnumerable.GetEnumerator()
		{
			return ((IEnumerable<T>)this).GetEnumerator();
		}

		#endregion
	}
}
