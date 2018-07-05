using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

	public class BasicMinuteVector : BasicIntVector
	{

		public BasicMinuteVector(int size) : base(DATA_FORM.DF_VECTOR, size)
		{
		}

		public BasicMinuteVector(IList<int?> list) : base(list)
		{
		}

		public BasicMinuteVector(int[] array) : base(array)
		{
		}

		protected internal BasicMinuteVector(DATA_FORM df, int size) : base(df, size)
		{
		}

		protected internal BasicMinuteVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
		{
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_MINUTE;
		}

		public override IScalar get(int index)
		{
			return new BasicMinute(getInt(index));
		}

		public virtual DateTime getMinute(int index)
		{
			if (isNull(index))
			{
				return DateTime.MinValue;
			}
			else
			{
				return Utils.parseMinute(getInt(index));
			}
		}

		public virtual void setMinute(int index, DateTime time)
		{
			setInt(index, Utils.countMinutes(time));
		}

		public override Type getElementClass()
		{
				return typeof(BasicMinute);
		}

	}

}