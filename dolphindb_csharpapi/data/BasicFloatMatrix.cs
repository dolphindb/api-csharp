using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

	public class BasicFloatMatrix : AbstractMatrix
	{
		private float[] values;

		public BasicFloatMatrix(int rows, int columns) : base(rows, columns)
		{
			values = new float[rows * columns];
		}

		public BasicFloatMatrix(int rows, int columns, IList<float[]> listOfArrays) : base(rows,columns)
		{
			values = new float[rows * columns];
			if (listOfArrays == null || listOfArrays.Count != columns)
			{
				throw new Exception("input list of arrays does not have " + columns + " columns");
			}
			for (int i = 0; i < columns; ++i)
			{
				float[] array = listOfArrays[i];
				if (array == null || array.Length != rows)
				{
					throw new Exception("The length of array " + (i + 1) + " doesn't have " + rows + " elements");
				}
				Array.Copy(array, 0, values, i * rows, rows);
			}
		}

		public BasicFloatMatrix(ExtendedDataInput @in) : base(@in)
		{
		}

		public virtual void setFloat(int row, int column, float value)
		{
			values[getIndex(row, column)] = value;
		}

		public virtual float getFloat(int row, int column)
		{
			return values[getIndex(row, column)];
		}

		public override bool isNull(int row, int column)
		{
			return values[getIndex(row, column)] == -float.MaxValue;
		}

		public override void setNull(int row, int column)
		{
			values[getIndex(row, column)] = -float.MaxValue;
		}

		public override IScalar get(int row, int column)
		{
			return new BasicFloat(values[getIndex(row, column)]);
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.FLOATING;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_FLOAT;
		}

		public override Type getElementClass()
		{
			return typeof(BasicFloat);
		}

		protected internal override void readMatrixFromInputStream(int rows, int columns, ExtendedDataInput @in)
		{
			int size = rows * columns;
			values = new float[size];
			for (int i = 0; i < size; ++i)
			{
				values[i] = @in.readFloat();
			}
		}

		protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
		{
			foreach (float value in values)
			{
				@out.writeFloat(value);
			}
		}
	}

}