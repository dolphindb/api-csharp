using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
	public class BasicTimestampMatrix : BasicLongMatrix
	{
		public BasicTimestampMatrix(int rows, int columns) : base(rows, columns)
		{
		}


		public BasicTimestampMatrix(int rows, int columns, IList<long[]> listOfArrays) : base(rows,columns, listOfArrays)
		{
		}

		public BasicTimestampMatrix(ExtendedDataInput @in) : base(@in)
		{
		}

		public virtual void setTimestamp(int row, int column, DateTime value)
		{
			setLong(row, column, Utils.countMilliseconds(value));
		}

		public virtual DateTime getTimestamp(int row, int column)
		{
			return Utils.parseTimestamp(getLong(row, column));
		}


		public override IScalar get(int row, int column)
		{
			return new BasicTimestamp(getLong(row, column));
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_TIMESTAMP;
		}

		public override Type getElementClass()
		{
			return typeof(BasicTimestamp);
		}

	}

}