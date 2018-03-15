using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

	public class BasicDateTimeVector : BasicIntVector
	{

		public BasicDateTimeVector(int size) : base(size)
		{
		}

		public BasicDateTimeVector(IList<int?> list) : base(list)
		{
		}

		public BasicDateTimeVector(int[] array) : base(array)
		{
		}

		protected internal BasicDateTimeVector(DATA_FORM df, int size) : base(df,size)
		{
		}

		protected internal BasicDateTimeVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
		{
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_DATETIME;
		}

		public override IScalar get(int index)
		{
			return new BasicDateTime(getInt(index));
		}

		public virtual DateTime getDateTime(int index)
		{
			if (isNull(index))
			{
				return DateTime.MinValue;
			}
			else
			{
				return Utils.parseDateTime(getInt(index));
			}
		}

		public virtual void setDateTime(int index, DateTime dt)
		{
			setInt(index,Utils.countSeconds(dt));
		}

		public override Type getElementClass()
		{
			return typeof(BasicDateTime);
		}

	}

}