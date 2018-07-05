using System;
using System.Collections.Generic;

namespace com.xxdb.route
{

	using com.xxdb.data;


	/// <summary>
	/// TableAppender is a used to append rows to a normal table.
	/// 
	/// <pre>
	/// {@code
	/// 
	/// TableAppender tableAppender = new PartitionedTableAppender("Trades", "192.168.1.25", 8848);
	/// List<Entity> row = new ArrayList<>();
	/// row.add(BasicInt(1));
	/// row.add(BasicString('A'));
	/// tableAppender.append(row);
	/// }
	/// </pre>
	/// </summary>
	public class TableAppender
	{
		private DBConnection conn;
		private BasicDictionary tableInfo;
		private BasicString tableName;
		private int cols;
		/// 
		/// <param name="tableName"> name of the shared table </param>
		/// <param name="host"> host </param>
		/// <param name="port"> port </param>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public TableAppender(String tableName, String host, int port) throws java.io.IOException
		public TableAppender(string tableName, string host, int port)
		{
			this.tableName = new BasicString(tableName);
			this.conn = new DBConnection();
			try
			{
				this.conn.connect(host, port);
				tableInfo = (BasicDictionary) conn.run("schema(" + tableName + ")");
				int partitionColumnIdx = ((BasicInt) tableInfo.get(new BasicString("partitionColumnIndex"))).Int;
				if (partitionColumnIdx != -1)
				{
					throw new Exception("Table '" + tableName + "' is partitioned");
				}
				BasicTable colDefs = ((BasicTable) tableInfo.get(new BasicString("colDefs")));
				this.cols = colDefs.getColumn(0).rows();
			}
			catch (IOException e)
			{
				throw e;
			}
		}

		/// <summary>
		/// Append a list of columns to the table. </summary>
		/// <param name="row"> </param>
		/// <returns> number of rows appended. </returns>
		/// <exception cref="IOException"> </exception>
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public int append(java.util.List<Entity> row) throws java.io.IOException
		public virtual int append(IList<Entity> row)
		{
			if (row.Count != cols)
			{
				throw new Exception("expect " + cols + " columns of values, got " + row.Count + " columns of values");
			}
			IList<Entity> args = new List<Entity>();
			args.Add(tableName);
			((List<Entity>)args).AddRange(row);
			BasicInt affected = (BasicInt)conn.run("tableInsert", args);
			return affected.Int;
		}
	}

}