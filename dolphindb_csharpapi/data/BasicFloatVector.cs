using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

	public class BasicFloatVector : AbstractVector
	{
		private float[] values;

		public BasicFloatVector(int size) : this(DATA_FORM.DF_VECTOR, size)
		{
		}

		public BasicFloatVector(IList<float?> list) : base(DATA_FORM.DF_VECTOR)
		{
			if (list != null)
			{
				values = new float[list.Count];
				for (int i = 0; i < list.Count; ++i)
				{
					values[i] = list[i].Value;
				}
			}
		}

		public BasicFloatVector(float[] array) : base(DATA_FORM.DF_VECTOR)
		{
			values = array.Clone() as float[];
		}

		protected internal BasicFloatVector(DATA_FORM df, int size) : base(df)
		{
			values = new float[size];
		}

		protected internal BasicFloatVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
		{
			int rows = @in.readInt();
			int cols = @in.readInt();
			int size = rows * cols;
			values = new float[size];
			for (int i = 0; i < size; ++i)
			{
				values[i] = @in.readFloat();
			}
		}

		public override IScalar get(int index)
		{
			return new BasicFloat(values[index]);
		}

		public virtual float getFloat(int index)
		{
			return values[index];
		}


		public override void set(int index, IScalar value)
		{
			values[index] = float.Parse(value.getString());
		}

		public virtual void setFloat(int index, float value)
		{
			values[index] = value;
		}

		public override bool isNull(int index)
		{
			return values[index] == -float.MaxValue;
		}

		public override void setNull(int index)
		{
			values[index] = -float.MaxValue;
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.FLOATING;
        }

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_FLOAT;
		}

		public override int rows()
		{
			return values.Length;
		}

		public override Type getElementClass()
		{
			return typeof(BasicFloat);
		}

		protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
		{
			@out.writeFloatArray(values);
		}
	}

}