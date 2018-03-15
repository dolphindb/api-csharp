using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
    
	public class BasicShortMatrix : AbstractMatrix
	{
		private short[] values;

		public BasicShortMatrix(int rows, int columns) : base(rows, columns)
		{
			values = new short[rows * columns];
		}

		public BasicShortMatrix(int rows, int columns, IList<short[]> list) : base(rows,columns)
		{
			values = new short[rows * columns];
			if (list == null || list.Count != columns)
			{
				throw new Exception("input list of arrays does not have " + columns + " columns");
			}
			for (int i = 0; i < columns; ++i)
			{
				short[] array = list[i];
				if (array == null || array.Length != rows)
				{
					throw new Exception("The length of array " + (i + 1) + " doesn't have " + rows + " elements");
				}
				Array.Copy(array, 0, values, i * rows, rows);
			}
		}

		public BasicShortMatrix(ExtendedDataInput @in) : base(@in)
		{
		}

		public virtual void setShort(int row, int column, short value)
		{
			values[getIndex(row, column)] = value;
		}

		public virtual short getShort(int row, int column)
		{
			return values[getIndex(row, column)];
		}

		public override bool isNull(int row, int column)
		{
			return values[getIndex(row, column)] == short.MinValue;
		}

		public override void setNull(int row, int column)
		{
			values[getIndex(row, column)] = short.MinValue;
		}

		public override IScalar get(int row, int column)
		{
			return new BasicShort(values[getIndex(row, column)]);
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.INTEGRAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_SHORT;
		}

		public override Type getElementClass()
		{
			return typeof(BasicShort);
		}

		protected internal override void readMatrixFromInputStream(int rows, int columns, ExtendedDataInput @in)
		{
			int size = rows * columns;
			values = new short[size];
			for (int i = 0; i < size; ++i)
			{
				values[i] = @in.readShort();
			}
		}

		protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
		{
			foreach (short value in values)
			{
				@out.writeInt(value);
			}
		}
	}

}