using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
	public class BasicNanoTimeVector : BasicLongVector
	{

		public BasicNanoTimeVector(int size) : base(DATA_FORM.DF_VECTOR, size)
		{
		}

		public BasicNanoTimeVector(IList<long?> list) : base(list)
		{
		}

		public BasicNanoTimeVector(long[] array) : base(array)
		{
		}

		protected internal BasicNanoTimeVector(DATA_FORM df, int size) : base(df, size)
		{
		}

		protected internal BasicNanoTimeVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
		{
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_NANOTIME;
		}

		public override IScalar get(int index)
		{
			return new BasicNanoTime(getLong(index));
		}

		public virtual TimeSpan getNanoTime(int index)
		{
			if (isNull(index))
			{
				return TimeSpan.MinValue;
			}
			else
			{
				return Utils.parseNanoTime(getLong(index));
			}
		}

		public virtual void setNanoTime(int index, DateTime time)
		{
			setLong(index, Utils.countNanoseconds(time));
		}

		public override Type getElementClass()
		{
			return typeof(BasicNanoTime);
		}
	}

}