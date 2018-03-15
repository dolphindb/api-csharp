using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{


	public class BasicBooleanVector : AbstractVector
	{
		private byte[] values;

		public BasicBooleanVector(int size) : this(DATA_FORM.DF_VECTOR, size)
		{
		}

		public BasicBooleanVector(IList<byte?> list) : base(DATA_FORM.DF_VECTOR)
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

		public BasicBooleanVector(byte[] array) : base(DATA_FORM.DF_VECTOR)
		{
			values = array.Clone() as byte[];
		}

		protected internal BasicBooleanVector(DATA_FORM df, int size) : base(df)
		{
			values = new byte[size];
		}

		protected internal BasicBooleanVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
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
			return new BasicBoolean(values[index]);
		}

		public virtual bool getBoolean(int index)
		{
			return values[index] != 0;
		}

		public override void set(int index, IScalar value)
		{
			values[index] = value.getNumber().byteValue();
		}

		public virtual void setBoolean(int index, bool value)
		{
			values[index] = value ? (byte)1 : (byte)0;
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