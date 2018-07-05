using System;
using System.Collections.Generic;

namespace com.xxdb.data
{

	using ExtendedDataInput = com.xxdb.io.ExtendedDataInput;


	/// 
	/// <summary>
	/// Corresponds to DolphinDB nanotime vector
	/// 
	/// </summary>

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

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: protected BasicNanoTimeVector(Entity_DATA_FORM df, com.xxdb.io.ExtendedDataInput in) throws java.io.IOException
		protected internal BasicNanoTimeVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
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
				return DATA_TYPE.DT_NANOTIME;
			}
		}

		public override Scalar get(int index)
		{
			return new BasicNanoTime(getLong(index));
		}

		public virtual LocalTime getNanoTime(int index)
		{
			if (isNull(index))
			{
				return null;
			}
			else
			{
				return Utils.parseNanoTime(getLong(index));
			}
		}

		public virtual void setNanoTime(int index, LocalTime time)
		{
			setLong(index, Utils.countNanoseconds(time));
		}

		public override Type ElementClass
		{
			get
			{
				return typeof(BasicNanoTime);
			}
		}
	}

}