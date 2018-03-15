using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

	public class BasicShortVector : AbstractVector
	{
		private short[] values;

		public BasicShortVector(int size) : this(DATA_FORM.DF_VECTOR, size)
		{
		}

		public BasicShortVector(IList<short?> list) : base(DATA_FORM.DF_VECTOR)
		{
			if (list != null)
			{
				values = new short[list.Count];
				for (int i = 0; i < list.Count; ++i)
				{
					values[i] = list[i].Value;
				}
			}
		}

		public BasicShortVector(short[] array) : base(DATA_FORM.DF_VECTOR)
		{
			values = array.Clone() as short[];
		}

		protected internal BasicShortVector(DATA_FORM df, int size) : base(df)
		{
			values = new short[size];
		}

		protected internal BasicShortVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
		{
			int rows = @in.readInt();
			int cols = @in.readInt();
			int size = rows * cols;
			values = new short[size];
			for (int i = 0; i < size; ++i)
			{
				values[i] = @in.readShort();
			}
		}

		public virtual short getShort(int index)
		{
			return values[index];
		}

		public override IScalar get(int index)
		{
			return new BasicShort(values[index]);
		}

		public override void set(int index, IScalar value)
		{
			values[index] = short.Parse(value.ToString());
		}

		public virtual void setShort(int index, short value)
		{
			values[index] = value;
		}

		public override bool isNull(int index)
		{
			return values[index] == short.MinValue;
		}

		public override void setNull(int index)
		{
			values[index] = short.MinValue;
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.INTEGRAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_SHORT;
		}

		public override int rows()
		{
			return values.Length;
		}

		public override Type getElementClass()
		{
			return typeof(BasicShort);
		}

		protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
		{
			@out.writeShortArray(values);
		}
	}

}