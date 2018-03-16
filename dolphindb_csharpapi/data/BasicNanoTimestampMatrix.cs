using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

	public class BasicNanoTimestampMatrix : BasicLongMatrix
	{
		public BasicNanoTimestampMatrix(int rows, int columns) : base(rows, columns)
		{
		}

		public BasicNanoTimestampMatrix(int rows, int columns, IList<long[]> listOfArrays) : base(rows,columns, listOfArrays)
		{
		}

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


		public override IScalar get(int row, int column)
		{
			return new BasicNanoTimestamp(getLong(row, column));
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_NANOTIMESTAMP;
		}

		public override Type getElementClass()
		{
			return typeof(BasicNanoTimestamp);
		}
	}
}