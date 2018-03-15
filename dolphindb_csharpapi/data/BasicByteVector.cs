using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
	public class BasicByteVector : AbstractVector
	{
		private byte[] values;

		public BasicByteVector(int size) : this(DATA_FORM.DF_VECTOR, size)
		{
		}

		public BasicByteVector(IList<byte?> list) : base(DATA_FORM.DF_VECTOR)
		{
			if (list != null)
			{
				values = new byte[list.Count];
				for (int i = 0; i < list.Count; ++i)
				{
					values[i] = list[i].Value;
				}
			}
		}

		public BasicByteVector(byte[] array) : base(DATA_FORM.DF_VECTOR)
		{
			values = array.Clone() as byte[];
		}

		protected internal BasicByteVector(DATA_FORM df, int size) : base(df)
		{
			values = new byte[size];
		}

		protected internal BasicByteVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
		{
			int rows = @in.readInt();
			int cols = @in.readInt();
			int size = rows * cols;
			values = new byte[size];
			for (int i = 0; i < size; ++i)
			{
				values[i] = @in.readByte();
			}
		}

		public override IScalar get(int index)
		{
			return new BasicByte(values[index]);
		}

		public virtual byte getByte(int index)
		{
			return values[index];
		}

		public override void set(int index, IScalar value)
		{
			values[index] = byte.Parse(value.getString());
		}

		public virtual void setByte(int index, byte value)
		{
			values[index] = value;
		}

		public override bool isNull(int index)
		{
			return values[index] == byte.MinValue;
		}

		public override void setNull(int index)
		{
    		values[index] = byte.MinValue;
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.INTEGRAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_BYTE;
		}

		public override Type getElementClass()
		{
			return typeof(BasicByte);
		}

		public override int rows()
		{
			return values.Length;
		}

		protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
		{
			@out.write(values);
		}
	}

}