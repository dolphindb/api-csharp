using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{


	public class BasicTimestampVector : BasicLongVector
	{

		public BasicTimestampVector(int size) : base(size)
		{
		}

		public BasicTimestampVector(IList<long?> list) : base(list)
		{
		}

		public BasicTimestampVector(long[] array) : base(array)
		{
		}

		protected internal BasicTimestampVector(DATA_FORM df, int size) : base(df, size)
		{
		}

		protected internal BasicTimestampVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
		{
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_TIMESTAMP;
		}

		public override IScalar get(int index)
		{
			return new BasicTimestamp(getLong(index));
		}

		public virtual DateTime getTimestamp(int index)
		{
			if (isNull(index))
			{
				return DateTime.MinValue;
			}
			else
			{
				return Utils.parseTimestamp(getLong(index));
			}
		}

		public virtual void setTimestamp(int index, DateTime dt)
		{
			setLong(index, Utils.countMilliseconds(dt));
		}

		public override Type getElementClass()
		{
			return typeof(BasicTimestamp);
		}
	}

}