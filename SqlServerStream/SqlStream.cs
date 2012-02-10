using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using Microsoft.SqlServer.Server;
using Common.Logging;

namespace DevelopMENTALMadness.Data.Sql
{
	public interface ISqlStreamRecord
	{
		SqlDataRecord ToSqlDataRecord();		
	}

	public interface ISqlStreamConnection<T> where T : ISqlStreamRecord
	{
		void ExecuteNonQuery(SqlStream<T> stream);
	}

	/// <summary>
	/// A bulk-import (streaming) implementation for use with Sql 2008
	/// based on CLR (ADO.NET) support for Table-Valued Parameters (TVP).
	/// <see cref="http://www.sqlservercentral.com/articles/SQL+Server+2008/66554/"/>
	/// </summary>
	/// <typeparam name="T">A class that implements ISqlStreamRecord</typeparam>
	public class SqlStream<T> : IDisposable, IEnumerable<SqlDataRecord> where T: ISqlStreamRecord
	{
		private static ILog logger = LogManager.GetCurrentClassLogger();
		ISqlStreamConnection<T> connection;

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

		public SqlStream(ISqlStreamConnection<T> connection)
		{
			this.connection = connection;
			buffer = new List<T>(bufferSize);
		}

		public SqlStream(ISqlStreamConnection<T> connection, int bufferSize)
		{
			this.connection = connection;
			this.bufferSize = bufferSize;
			buffer = new List<T>(bufferSize);
		}

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
				ThreadPool.QueueUserWorkItem(new WaitCallback(ExecuteNonQuery));
			}
		}

		public void Close()
		{
			// flush buffer and wait
			logger.Debug("Closing stream, set finalize flag and wait");
			Interlocked.Increment(ref finalizeStream);
			Flush();
			completed.Wait();
			logger.Debug("Finished closing stream.");
		}

		#region IDisposable Members
		private bool disposed = false;

		public void Dispose()
		{
			if (disposed)
				throw new ObjectDisposedException(typeof(SqlStream<T>).Name);

			disposed = true;

			Close();
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

		private void ExecuteNonQuery(Object state)
		{
			logger.Debug("Background thread started.");
			connection.ExecuteNonQuery(this);
		}

		#region IEnumerable<SqlDataRecord> Members

		IEnumerator<SqlDataRecord> IEnumerable<SqlDataRecord>.GetEnumerator()
		{
			while (finalizeStream == 0 || chunks.Count != 0)
			{
				enqueued.Wait();

				logger.Debug("Get next chunk.");
				var e = DequeueNext();
				foreach(var i in e)
				{
					yield return i.ToSqlDataRecord();	
				}

				if (chunks.Count == 0)
				{
					enqueued.Reset();
					logger.Debug("Waiting for next chunk or finalize");
				}
			}

			completed.Set();
			logger.Debug("End of stream");
		}

		#endregion

		#region IEnumerable Members

		IEnumerator IEnumerable.GetEnumerator()
		{
			return ((IEnumerable<SqlDataRecord>)this).GetEnumerator();
		}

		#endregion
	}
}
