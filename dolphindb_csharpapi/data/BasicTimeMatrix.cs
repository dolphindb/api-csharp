using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
    
	public class BasicTimeMatrix : BasicIntMatrix
	{
		public BasicTimeMatrix(int rows, int columns) : base(rows, columns)
		{
		}

		public BasicTimeMatrix(int rows, int columns, IList<int[]> listOfArrays) : base(rows,columns, listOfArrays)
		{
		}

		public BasicTimeMatrix(ExtendedDataInput @in) : base(@in)
		{
		}

		public virtual void setTime(int row, int column, DateTime value)
		{
			setInt(row, column, Utils.countMilliseconds(value));
		}

		public virtual DateTime getTime(int row, int column)
		{
			return Utils.parseTime(getInt(row, column));
		}

		public override IScalar get(int row, int column)
		{
			return new BasicTime(getInt(row, column));
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_TIME;
		}

		public override Type getElementClass()
		{
			return typeof(BasicTime);
		}
	}

}