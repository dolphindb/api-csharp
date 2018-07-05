using System;
using System.Collections.Generic;

namespace com.xxdb.data
{

	using ExtendedDataInput = com.xxdb.io.ExtendedDataInput;


	/// 
	/// <summary>
	/// Corresponds to DolphinDB nanotimestamp vector
	/// 
	/// </summary>

	public class BasicNanoTimestampVector : BasicLongVector
	{

		public BasicNanoTimestampVector(int size) : base(size)
		{
		}

		public BasicNanoTimestampVector(IList<long?> list) : base(list)
		{
		}

		public BasicNanoTimestampVector(long[] array) : base(array)
		{
		}

		protected internal BasicNanoTimestampVector(DATA_FORM df, int size) : base(df, size)
		{
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: protected BasicNanoTimestampVector(Entity_DATA_FORM df, com.xxdb.io.ExtendedDataInput in) throws java.io.IOException
		protected internal BasicNanoTimestampVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
		{
		}

		public override DATA_CATEGORY DataCategory
		{
			get
			{
				return DATA_CATEGORY.TEMPORAL;
			}
		}

		public override DATA_TYPE DataType
		{
			get
			{
				return DATA_TYPE.DT_NANOTIMESTAMP;
			}
		}

		public override Scalar get(int index)
		{
			return new BasicNanoTimestamp(getLong(index));
		}

		public virtual DateTime getNanoTimestamp(int index)
		{
			if (isNull(index))
			{
				return null;
			}
			else
			{
				return Utils.parseNanoTimestamp(getLong(index));
			}
		}

		public virtual void setNanoTimestamp(int index, DateTime dt)
		{
			setLong(index, Utils.countNanoseconds(dt));
		}

		public override Type ElementClass
		{
			get
			{
				return typeof(BasicNanoTimestamp);
			}
		}
	}

}