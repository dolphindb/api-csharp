using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
	public class BasicIntVector : AbstractVector
	{
		private int[] values;

		public BasicIntVector(int size) : this(DATA_FORM.DF_VECTOR, size)
		{
		}

		public BasicIntVector(IList<int?> list) : base(DATA_FORM.DF_VECTOR)
		{
			if (list != null)
			{
				values = new int[list.Count];
				for (int i = 0; i < list.Count; ++i)
				{
					values[i] = list[i].Value;
				}
			}
		}

		public BasicIntVector(int[] array) : base(DATA_FORM.DF_VECTOR)
		{
			values = array.Clone() as int[];
		}

		protected internal BasicIntVector(DATA_FORM df, int size) : base(df)
		{
			values = new int[size];
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: protected BasicIntVector(Entity_DATA_FORM df, dolphindb.io.ExtendedDataInput in) throws java.io.IOException
		protected internal BasicIntVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
		{
			int rows = @in.readInt();
			//if (rows != 1024)
				//assert(rows == 1024);
			int cols = @in.readInt();
			int size = rows * cols;
			values = new int[size];
			for (int i = 0; i < size; ++i)
			{
				values[i] = @in.readInt();
			}
		}

		public override IScalar get(int index)
		{
			return new BasicInt(values[index]);
		}

		public virtual int getInt(int index)
		{
			return values[index];
		}

		public override void set(int index, IScalar value)
		{
			values[index] = Convert.ToInt16(value.getString());
		}

		public virtual void setInt(int index, int value)
		{
			values[index] = value;
		}

		public override bool isNull(int index)
		{
			return values[index] == int.MinValue;
		}

		public override void setNull(int index)
		{
			values[index] = int.MinValue;
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.INTEGRAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_INT;
		}

		public override Type getElementClass()
		{
			return typeof(BasicInt);
		}

		public override int rows()
		{
			return values.Length;
		}

		protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
		{
			@out.writeIntArray(values);
		}
	}

}