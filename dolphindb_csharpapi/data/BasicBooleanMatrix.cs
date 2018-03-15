using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
	public class BasicBooleanMatrix : AbstractMatrix
	{
	    private byte[] values;

		public BasicBooleanMatrix(int rows, int columns) : base(rows, columns)
		{
			values = new byte[rows * columns];
		}

		public BasicBooleanMatrix(int rows, int columns, IList<byte[]> listOfArrays) : base(rows,columns)
		{
			values = new byte[rows * columns];
			if (listOfArrays == null || listOfArrays.Count != columns)
			{
				throw new Exception("input list of arrays does not have " + columns + " columns");
			}
			for (int i = 0; i < columns; ++i)
			{
				byte[] array = listOfArrays[i];
				if (array == null || array.Length != rows)
				{
					throw new Exception("The length of array " + (i + 1) + " doesn't have " + rows + " elements");
				}
				Array.Copy(array, 0, values, i * rows, rows);
			}
		}

		public BasicBooleanMatrix(ExtendedDataInput @in) : base(@in)
		{
		}

		public virtual void setBoolean(int row, int column, bool value)
		{
			values[getIndex(row, column)] = value ? (byte)1 : (byte)0;
		}

		public virtual bool getBoolean(int row, int column)
		{
			return values[getIndex(row, column)] == 1;
		}

		public override bool isNull(int row, int column)
		{
			return values[getIndex(row, column)] == byte.MinValue;
		}

		public override void setNull(int row, int column)
		{
			values[getIndex(row, column)] = byte.MinValue;
		}

		public override IScalar get(int row, int column)
		{
			return new BasicBoolean(values[getIndex(row, column)]);
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.LOGICAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_BOOL;
		}

		public override Type getElementClass()
		{
			return typeof(BasicBoolean);
		}

		protected internal override void readMatrixFromInputStream(int rows, int columns, ExtendedDataInput @in)
		{
			int size = rows * columns;
			values = new byte[size];
			for (int i = 0; i < size; ++i)
			{
				values[i] = @in.readByte();
			}
		}

		protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
		{
			foreach (byte value in values)
			{
				@out.writeByte(value);
			}
		}
	}

}