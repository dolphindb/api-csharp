using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{


	public class BasicLongVector : AbstractVector
	{
		private long[] values;

		public BasicLongVector(int size) : this(DATA_FORM.DF_VECTOR, size)
		{
		}

		public BasicLongVector(IList<long?> list) : base(DATA_FORM.DF_VECTOR)
		{
			if (list != null)
			{
				values = new long[list.Count];
				for (int i = 0; i < list.Count; ++i)
				{
					values[i] = list[i].Value;
				}
			}
		}

		public BasicLongVector(long[] array) : base(DATA_FORM.DF_VECTOR)
		{
			values = array.Clone() as long[];
		}

		protected internal BasicLongVector(DATA_FORM df, int size) : base(df)
		{
			values = new long[size];
		}

		protected internal BasicLongVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
		{
			int rows = @in.readInt();
			int cols = @in.readInt();
			int size = rows * cols;
			values = new long[size];
			for (int i = 0; i < size; ++i)
			{
				values[i] = @in.readLong();
			}
		}

		public override IScalar get(int index)
		{
			return new BasicLong(values[index]);
		}

		public virtual long getLong(int index)
		{
			return values[index];
		}

		public override void set(int index, IScalar value)
		{
			values[index] = Convert.ToInt32(value.getString());
		}

		public virtual void setLong(int index, long value)
		{
			values[index] = value;
		}

		public override bool isNull(int index)
		{
			return values[index] == long.MinValue;
		}

		public override void setNull(int index)
		{
			values[index] = long.MinValue;
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.INTEGRAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_LONG;
		}

		public override int rows()
		{
			return values.Length;
		}

		public override Type getElementClass()
		{
			return typeof(BasicLong);
		}

		protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
		{
			@out.writeLongArray(values);
		}
	}

}