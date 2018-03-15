using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
	public class BasicDoubleVector : AbstractVector
	{
		private double[] values;

		public BasicDoubleVector(int size) : this(DATA_FORM.DF_VECTOR, size)
		{
		}

		public BasicDoubleVector(IList<double?> list) : base(DATA_FORM.DF_VECTOR)
		{
			if (list != null)
			{
				values = new double[list.Count];
				for (int i = 0; i < list.Count; ++i)
				{
					values[i] = list[i].Value;
				}
			}
		}

		public BasicDoubleVector(double[] array) : base(DATA_FORM.DF_VECTOR)
		{
			values = array.Clone() as double[];
		}

		protected internal BasicDoubleVector(DATA_FORM df, int size) : base(df)
		{
			values = new double[size];
		}

		protected internal BasicDoubleVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
		{
			int rows = @in.readInt();
			int cols = @in.readInt();
			int size = rows * cols;
			values = new double[size];
			for (int i = 0; i < size; ++i)
			{
				values[i] = @in.readDouble();
			}
		}

		public override IScalar get(int index)
		{
			return new BasicDouble(values[index]);
		}

		public virtual double getDouble(int index)
		{
			return values[index];
		}

		public override void set(int index, IScalar value)
		{
			values[index] = double.Parse(value.ToString());
		}

		public virtual void setDouble(int index, double value)
		{
			values[index] = value;
		}

		public override bool isNull(int index)
		{
			return values[index] == -double.MaxValue;
		}

		public override void setNull(int index)
		{
			values[index] = -double.MaxValue;
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.FLOATING;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_DOUBLE;
		}

		public override Type getElementClass()
		{
			return typeof(BasicDouble);
		}

		public override int rows()
		{
			return values.Length;
		}

		protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
		{
			@out.writeDoubleArray(values);
		}

	}

}