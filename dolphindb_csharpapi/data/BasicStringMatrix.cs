using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{


	public class BasicStringMatrix : AbstractMatrix
	{
		private string[] values;
		private bool isSymbol;

		public BasicStringMatrix(int rows, int columns) : base(rows, columns)
		{
			values = new string[rows * columns];
			isSymbol = true;
		}

		public BasicStringMatrix(int rows, int columns, IList<string[]> list) : base(rows,columns)
		{
			values = new string[rows * columns];
			if (list == null || list.Count != columns)
			{
				throw new Exception("input list of arrays does not have " + columns + " columns");
			}
			for (int i = 0; i < columns; ++i)
			{
				string[] array = list[i];
				if (array == null || array.Length != rows)
				{
					throw new Exception("The length of array " + (i + 1) + " doesn't have " + rows + " elements");
				}
				Array.Copy(array, 0, values, i * rows, rows);
			}
			isSymbol = true;
		}

		public BasicStringMatrix(ExtendedDataInput @in) : base(@in)
		{
			isSymbol = true;
		}

		public virtual void setString(int row, int column, string value)
		{
			values[getIndex(row, column)] = value;
		}

		public virtual string getString(int row, int column)
		{
			return values[getIndex(row, column)];
		}

		public override bool isNull(int row, int column)
		{
			return values[getIndex(row, column)].Length == 0;
		}

		public override void setNull(int row, int column)
		{
			values[getIndex(row, column)] = "";
		}

		public override IScalar get(int row, int column)
		{
			return new BasicString(values[getIndex(row, column)]);
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.LITERAL;
		}

		public override DATA_TYPE getDataType()
		{
			return isSymbol ? DATA_TYPE.DT_SYMBOL : DATA_TYPE.DT_STRING;
		}

		public override Type getElementClass()
		{
			return typeof(BasicString);
		}

		protected internal override void readMatrixFromInputStream(int rows, int columns, ExtendedDataInput @in)
		{
			int size = rows * columns;
			values = new string[size];
			for (int i = 0; i < size; ++i)
			{
				values[i] = @in.readString();
			}
		}

		protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
		{
			foreach (string value in values)
			{
				@out.writeString(value);
			}
		}
	}

}