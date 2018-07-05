using System;
using System.Collections.Generic;

namespace com.xxdb.route
{

	using com.xxdb.data;


	/// <summary>
	/// PartitionedTableAppender is used to append rows to a partitioned table
	/// across a cluster of DolphinDB instances.
	/// 
	/// <pre>
	/// {@code
	/// 
	/// PartitionedTableAppender tableAppender = new PartitionedTableAppender("Trades", "192.168.1.25", 8848);
	/// List<Entity> row = new ArrayList<>();
	/// row.add(BasicInt(1));
	/// row.add(BasicString('A'));
	/// int affected = tableAppender.append(row);
	/// 
	/// // append multiple rows at a time
	/// List<Entity> rows = new ArrayList<>();
	/// BasicIntVector vint = new BasicIntVector(Arrays.asList(1,2,3,4,5));
	/// BasicStringVector vstring = new BasicStringVector(Arrays.asList("A", "B", "C", "D", "E"));
	/// rows.add(vint);
	/// rows.add(vstring);
	/// affected = tableAppender.append(rows);
	/// 
	/// 
	/// // cleanup
	/// tableAppender.shutdownThreadPool();
	/// }
	/// </pre>
	/// </summary>
	public class PartitionedTableAppender
	{
		private static readonly int CORES = Runtime.Runtime.availableProcessors();
		private IDictionary<string, DBConnection> connectionMap = new Dictionary<string, DBConnection>();
		private BasicDictionary tableInfo;
		private TableRouter router;
		private BasicString tableName;
		private int partitionColumnIdx;
		private int cols;
		private Entity_DATA_CATEGORY[] columnCategories;
		private Entity_DATA_TYPE[] columnTypes;
		private int threadCount;
		private ExecutorService threadPool;

		/// 
		/// <param name="tableName"> name of the shared table </param>
		/// <param name="host"> host </param>
		/// <param name="port"> port </param>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public PartitionedTableAppender(String tableName, String host, int port) throws java.io.IOException
		public PartitionedTableAppender(string tableName, string host, int port) : this(tableName, host, port, 0)
		{
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public PartitionedTableAppender(String tableName, String host, int port, int threadCount) throws java.io.IOException
		public PartitionedTableAppender(string tableName, string host, int port, int threadCount)
		{
			this.tableName = new BasicString(tableName);
			DBConnection conn = new DBConnection();
			BasicAnyVector locations;
			AbstractVector partitionSchema;
			BasicTable colDefs;
			BasicIntVector typeInts;
			try
			{
				conn.connect(host, port);
				tableInfo = (BasicDictionary) conn.run("schema(" + tableName + ")");
				partitionColumnIdx = ((BasicInt) tableInfo.get(new BasicString("partitionColumnIndex"))).Int;
				if (partitionColumnIdx == -1)
				{
					throw new Exception("Table '" + tableName + "' is not partitioned");
				}
				locations = (BasicAnyVector) tableInfo.get(new BasicString("partitionSites"));
				int partitionType = ((BasicInt) tableInfo.get(new BasicString("partitionType"))).Int;
				partitionSchema = (AbstractVector) tableInfo.get(new BasicString("partitionSchema"));
				router = TableRouterFacotry.createRouter(Enum.GetValues(typeof(Entity_PARTITION_TYPE))[partitionType], partitionSchema, locations);
				colDefs = ((BasicTable) tableInfo.get(new BasicString("colDefs")));
				this.cols = colDefs.getColumn(0).rows();
				typeInts = (BasicIntVector) colDefs.getColumn("typeInt");
				this.columnCategories = new Entity_DATA_CATEGORY[this.cols];
				this.columnTypes = new Entity_DATA_TYPE[this.cols];
				for (int i = 0; i < cols; ++i)
				{
					this.columnTypes[i] = Enum.GetValues(typeof(Entity_DATA_TYPE))[typeInts.getInt(i)];
					this.columnCategories[i] = Entity.typeToCategory(this.columnTypes[i]);
				}
			}
			catch (IOException e)
			{
				throw e;
			}
			finally
			{
				conn.close();
			}

			this.threadCount = threadCount;
			if (this.threadCount <= 0)
			{
				this.threadCount = Math.Min(CORES, locations.rows());
			}
			if (this.threadCount > 0)
			{
				this.threadCount--;
			}
			threadPool = Executors.newFixedThreadPool(this.threadCount);
		}

		private string getDestination(Scalar partitioningColumn)
		{
			return router.route(partitioningColumn);
		}
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private com.xxdb.DBConnection getConnection(Entity partitioningColumn) throws java.io.IOException
		private DBConnection getConnection(Entity partitioningColumn)
		{
			if (!(partitioningColumn is Scalar))
			{
				throw new Exception("partitioning column value must be a scalar");
			}
			string dest = getDestination((Scalar) partitioningColumn);
			DBConnection conn = connectionMap[dest];
			if (conn == null)
			{
				conn = new DBConnection();
				string[] destParts = dest.Split(":", true);
				conn.connect(destParts[0], Convert.ToInt32(destParts[1]));
				connectionMap[dest] = conn;
			}

			return conn;
		}
		private static readonly BasicEntityFactory entityFactory = new BasicEntityFactory();
		private const int CHECK_RESULT_SINGLE_ROW = 1;
		private const int CHECK_RESULT_MULTI_ROWS = 2;
		private class BatchAppendTask : Callable<int?>
		{
			private readonly PartitionedTableAppender outerInstance;

			internal IList<IList<Scalar>> columns;
			internal DBConnection conn;
			internal BatchAppendTask(PartitionedTableAppender outerInstance, int cols, DBConnection conn)
			{
				this.outerInstance = outerInstance;
				this.columns = new List<>(cols);
				this.conn = conn;
			}
			public virtual int? call()
			{
				IList<Entity> args = new List<Entity>(1 + columns.Count);
				args.Add(outerInstance.tableName);
				for (int i = 0; i < columns.Count; ++i)
				{
					IList<Scalar> column = columns[i];
					Vector vector = entityFactory.createVectorWithDefaultValue(column[0].DataType, column.Count);
					for (int j = 0; j < column.Count; ++j)
					{
						try
						{
							vector.set(j, column[j]);
						}
						catch (Exception e)
						{
							throw new Exception(e);
						}
					}
					args.Add(vector);
				}
				try
				{
					return ((BasicInt)conn.run("tableInsert", args)).Int;
				}
				catch (IOException e)
				{
					throw new Exception(e);
				}
			}

			public virtual void appendColumn()
			{
				this.columns.Add(new List<>());
			}
			public virtual void appendToLastColumn(Scalar val)
			{
				columns[columns.Count - 1].Add(val);
			}
		}
		// assume input is sanity checked.
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private int appendBatch(List<Entity> subTable) throws java.io.IOException
		private int appendBatch(IList<Entity> subTable)
		{
			if (subTable[0].rows() == 0)
			{
				return 0;
			}
			IList<DBConnection> destConns = new List<DBConnection>();
			IDictionary<DBConnection, BatchAppendTask> conn2TaskMap = new Dictionary<DBConnection, BatchAppendTask>();
			AbstractVector partitioningColumnVector = (AbstractVector) subTable[partitionColumnIdx];
			int rows = partitioningColumnVector.rows();
			for (int i = 0; i < rows; ++i)
			{
				DBConnection conn = getConnection(partitioningColumnVector.get(i));
				if (!conn2TaskMap.ContainsKey(conn))
				{
					conn2TaskMap[conn] = new BatchAppendTask(this, this.cols, conn);
				}
				destConns.Add(conn);
			}
			for (int i = 0; i < this.cols; ++i)
			{
				// for each task, add a new last column.
				ISet<DBConnection> keySet = conn2TaskMap.Keys;
				foreach (DBConnection conn in keySet)
				{
					BatchAppendTask task = conn2TaskMap[conn];
					task.appendColumn();
				}
				// dispatch column to corresponding column
				AbstractVector column = (AbstractVector)subTable[i];
				for (int j = 0; j < rows; ++j)
				{
					DBConnection destConn = destConns[j];
					BatchAppendTask destTask = conn2TaskMap[destConn];
					destTask.appendToLastColumn(column.get(j));
				}
			}

			int affected = 0;
			BatchAppendTask savedTask = null;
			ISet<DBConnection> keySet = conn2TaskMap.Keys;
			IList<Future<int?>> futures = new List<Future<int?>>();
			foreach (DBConnection conn in keySet)
			{
				BatchAppendTask task = conn2TaskMap[conn];
				if (savedTask == null)
				{
					savedTask = task;
				}
				else
				{
					futures.Add(threadPool.submit(task));
				}
			}
			affected += savedTask.call().Value;
			for (int i = 0; i < futures.Count; ++i)
			{
				try
				{
					affected += futures[i].get();
				}
				catch (System.Exception e) when (e is InterruptedException || e is ExecutionException)
				{
					throw new Exception(e);
				}
			}
			return affected;
		}

		// assume input is sanity checked.
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private int appendSingle(List<Entity> row) throws java.io.IOException
		private int appendSingle(IList<Entity> row)
		{
			DBConnection conn = getConnection(row[partitionColumnIdx]);
			IList<Entity> args = new List<Entity>();
			args.Add(tableName);
			((List<Entity>)args).AddRange(row);
			return ((BasicInt)conn.run("tableInsert", args)).Int;
		}
		private void checkColumnType(int col, Entity_DATA_CATEGORY category, Entity_DATA_TYPE type)
		{
			Entity_DATA_CATEGORY expectCategory = this.columnCategories[col];
			Entity_DATA_TYPE expectType = this.columnTypes[col];
			if (category != expectCategory)
			{
				throw new Exception("column " + col + ", expect category " + expectCategory.name() + ", got category " + category.name());
			}
			else if (category == Entity_DATA_CATEGORY.TEMPORAL && type != expectType)
			{
				throw new Exception("column " + col + ", temporal column must have exactly the same type, expect " + expectType.name() + ", got " + type.name());
			}
		}
		private int check(IList<Entity> row)
		{
			if (row.Count != cols)
			{
				throw new Exception("expect " + cols + " columns of values, got " + row.Count + " columns of values");
			}
			for (int i = 1; i < cols; ++i)
			{
				if (row[i].rows() != row[i - 1].rows())
				{
					throw new Exception("all columns must have the same size");
				}
			}
			for (int i = 0; i < cols; ++i)
			{
				checkColumnType(i, row[i].DataCategory, row[i].DataType);
			}
			if (row[partitionColumnIdx] is AbstractVector)
			{
				return CHECK_RESULT_MULTI_ROWS;
			}
			else
			{
				return CHECK_RESULT_SINGLE_ROW;
			}
		}
		/// <summary>
		/// Append a list of columns to the table. </summary>
		/// <param name="row"> </param>
		/// <returns> number of rows appended. </returns>
		/// <exception cref="IOException"> </exception>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public int append(List<Entity> row) throws java.io.IOException
		public virtual int append(IList<Entity> row)
		{
			if (check(row) == CHECK_RESULT_SINGLE_ROW)
			{
				return appendSingle(row);
			}
			else
			{ // CHECK_RESULT_MULTI_ROWS
				return appendBatch(row);
			}
		}

		public virtual void shutdownThreadPool()
		{
			this.threadPool.shutdown();
		}
	}

}