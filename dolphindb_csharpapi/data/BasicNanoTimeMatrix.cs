using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

	public class BasicNanoTimeMatrix : BasicLongMatrix
	{
		public BasicNanoTimeMatrix(int rows, int columns) : base(rows, columns)
		{
		}

		public BasicNanoTimeMatrix(int rows, int columns, IList<long[]> listOfArrays) : base(rows,columns, listOfArrays)
		{
		}

		public BasicNanoTimeMatrix(ExtendedDataInput @in) : base(@in)
		{
		}

		public virtual void setNanoTime(int row, int column, DateTime value)
		{
			setLong(row, column, Utils.countNanoseconds(value));
		}

		public virtual TimeSpan getNanoTime(int row, int column)
		{
			return Utils.parseNanoTime(getLong(row, column));
		}

		public override IScalar get(int row, int column)
		{
			return new BasicNanoTime(getLong(row, column));
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_NANOTIME;
		}

		public override Type getElementClass()
		{
			return typeof(BasicNanoTime);
		}
	}

}