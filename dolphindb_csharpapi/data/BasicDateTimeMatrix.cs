using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

	public class BasicDateTimeMatrix : BasicIntMatrix
	{
		public BasicDateTimeMatrix(int rows, int columns) : base(rows, columns)
		{
		}

		public BasicDateTimeMatrix(int rows, int columns, IList<int[]> listOfArrays) : base(rows,columns, listOfArrays)
		{
		}

		public BasicDateTimeMatrix(ExtendedDataInput @in) : base(@in)
		{
		}

		public virtual void setDateTime(int row, int column, DateTime value)
		{
			setInt(row, column, Utils.countSeconds(value));
		}

		public virtual DateTime getDateTime(int row, int column)
		{
			return Utils.parseDateTime(getInt(row, column));
		}

		public override IScalar get(int row, int column)
		{
			return new BasicDateTime(getInt(row, column));
		}

		public override Type getElementClass()
		{
			return typeof(BasicDateTime);
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_DATETIME;
		}
	}

}