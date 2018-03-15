using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{


	public class BasicLongMatrix : AbstractMatrix
	{
		private long[] values;

		public BasicLongMatrix(int rows, int columns) : base(rows, columns)
		{
			values = new long[rows * columns];
		}

		public BasicLongMatrix(int rows, int columns, IList<long[]> list) : base(rows,columns)
		{
			values = new long[rows * columns];
			if (list == null || list.Count != columns)
			{
				throw new Exception("input list of arrays does not have " + columns + " columns");
			}
			for (int i = 0; i < columns; ++i)
			{
				long[] array = list[i];
				if (array == null || array.Length != rows)
				{
					throw new Exception("The length of array " + (i + 1) + " doesn't have " + rows + " elements");
				}
				Array.Copy(array, 0, values, i * rows, rows);
			}
		}

		public BasicLongMatrix(ExtendedDataInput @in) : base(@in)
		{
		}

		public virtual void setLong(int row, int column, long value)
		{
			values[getIndex(row, column)] = value;
		}

		public virtual long getLong(int row, int column)
		{
			return values[getIndex(row, column)];
		}

		public override bool isNull(int row, int column)
		{
			return values[getIndex(row, column)] == long.MinValue;
		}

		public override void setNull(int row, int column)
		{
			values[getIndex(row, column)] = long.MinValue;
		}

		public override IScalar get(int row, int column)
		{
			return new BasicLong(values[getIndex(row, column)]);
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.INTEGRAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_LONG;
		}

		public override Type getElementClass()
		{
			return typeof(BasicLong);
		}

		protected internal override void readMatrixFromInputStream(int rows, int columns, ExtendedDataInput @in)
		{
			int size = rows * columns;
			values = new long[size];
			for (int i = 0; i < size; ++i)
			{
				values[i] = @in.readLong();
			}
		}

		protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
		{
			foreach (long value in values)
			{
				@out.writeLong(value);
			}
		}

	}

}