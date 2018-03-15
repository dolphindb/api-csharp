using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

	public class BasicDateMatrix : BasicIntMatrix
	{
		public BasicDateMatrix(int rows, int columns) : base(rows, columns)
		{
		}


		public BasicDateMatrix(int rows, int columns, IList<int[]> listOfArrays) : base(rows,columns, listOfArrays)
		{
		}


		public BasicDateMatrix(ExtendedDataInput @in) : base(@in)
		{
		}

		public virtual void setDate(int row, int column, DateTime value)
		{
			setInt(row, column, Utils.countDays(value));
		}

		public virtual DateTime getDate(int row, int column)
		{
			return Utils.parseDate(getInt(row, column));
		}


		public override IScalar get(int row, int column)
		{
			return new BasicDate(getInt(row, column));
		}

		public override Type getElementClass()
		{

				return typeof(BasicDate);
		}

		public override DATA_CATEGORY getDataCategory()
		{
				return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_DATE;
		}
	}

}