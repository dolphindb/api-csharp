using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
	public class BasicSecondVector : BasicIntVector
	{

		public BasicSecondVector(int size) : base(DATA_FORM.DF_VECTOR, size)
		{
		}

		public BasicSecondVector(IList<int?> list) : base(list)
		{
		}

		public BasicSecondVector(int[] array) : base(array)
		{
		}

		protected internal BasicSecondVector(DATA_FORM df, int size) : base(df, size)
		{
		}

		protected internal BasicSecondVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
		{
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_SECOND;
		}

		public override IScalar get(int index)
		{
			return new BasicSecond(getInt(index));
		}

		public virtual DateTime getSecond(int index)
		{
			if (isNull(index))
			{
				return DateTime.MinValue;
			}
			else
			{
				return Utils.parseSecond(getInt(index));
			}
		}

		public virtual void setSecond(int index, DateTime time)
		{
			setInt(index, Utils.countSeconds(time));
		}

		public override Type getElementClass()
		{
			return typeof(BasicSecond);
		}
	}

}