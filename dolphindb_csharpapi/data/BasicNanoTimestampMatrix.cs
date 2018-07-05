using System;
using System.Collections.Generic;

namespace com.xxdb.data
{

	using ExtendedDataInput = com.xxdb.io.ExtendedDataInput;


	/// 
	/// <summary>
	/// Corresponds to DolphinDB nanotimestamp matrix
	/// 
	/// </summary>

	public class BasicNanoTimestampMatrix : BasicLongMatrix
	{
		public BasicNanoTimestampMatrix(int rows, int columns) : base(rows, columns)
		{
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public BasicNanoTimestampMatrix(int rows, int columns, java.util.List<long[]> listOfArrays) throws Exception
		public BasicNanoTimestampMatrix(int rows, int columns, IList<long[]> listOfArrays) : base(rows,columns, listOfArrays)
		{
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public BasicNanoTimestampMatrix(com.xxdb.io.ExtendedDataInput in) throws java.io.IOException
		public BasicNanoTimestampMatrix(ExtendedDataInput @in) : base(@in)
		{
		}

		public virtual void setNanoTimestamp(int row, int column, DateTime value)
		{
			setLong(row, column, Utils.countNanoseconds(value));
		}

		public virtual DateTime getNanoTimestamp(int row, int column)
		{
			return Utils.parseNanoTimestamp(getLong(row, column));
		}


		public override Scalar get(int row, int column)
		{
			return new BasicNanoTimestamp(getLong(row, column));
		}

		public override DATA_CATEGORY DataCategory
		{
			get
			{
				return DATA_CATEGORY.TEMPORAL;
			}
		}

		public override DATA_TYPE DataType
		{
			get
			{
				return DATA_TYPE.DT_NANOTIMESTAMP;
			}
		}

		public override Type ElementClass
		{
			get
			{
				return typeof(BasicNanoTimestamp);
			}
		}

	}

}