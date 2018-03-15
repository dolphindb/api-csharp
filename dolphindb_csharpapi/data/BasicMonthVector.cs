using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{


	public class BasicMonthVector : BasicIntVector
	{

		public BasicMonthVector(int size) : base(DATA_FORM.DF_VECTOR, size)
		{
		}

		public BasicMonthVector(IList<int?> list) : base(list)
		{
		}

		public BasicMonthVector(int[] array) : base(array)
		{
		}

		protected internal BasicMonthVector(DATA_FORM df, int size) : base(df, size)
		{
		}

		protected internal BasicMonthVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
		{
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_MONTH;
		}

		public override IScalar get(int index)
		{
			return new BasicMonth(getInt(index));
		}

		public virtual DateTime getMonth(int index)
		{
			return Utils.parseMonth(getInt(index));
		}

		public virtual void setMonth(int index, DateTime month)
		{
			setInt(index, Utils.countMonths(month));
		}

		public override Type getElementClass()
		{
			return typeof(DateTime);
		}

	}

}