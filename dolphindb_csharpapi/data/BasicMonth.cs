
using dolphindb.io;
using System;

namespace dolphindb.data
{

	public class BasicMonth : BasicInt
	{
		private static string format ="yyyy.MMM";

		public BasicMonth(int year, int month) : base(year * 12 + month)
		{
		}
		public BasicMonth(DateTime value) : base(value.Year * 12 + value.Month - 1)
		{
		}

		public BasicMonth(ExtendedDataInput @in) : base(@in)
		{
		}

		protected internal BasicMonth(int value) : base(value)
		{
		}

		public new DateTime getValue()
		{
				if (isNull())
				{
					return DateTime.MinValue;
				}
				else
				{
					return Utils.parseMonth(base.getValue());
				}
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_MONTH;
		}

		public override object getTemporal()
		{
			return getValue();
		}

		public override string getString()
		{
				if (isNull())
				{
					return "";
				}
				else
				{
					return this.getValue().ToString(format);
				}

		}

		public override bool Equals(object o)
		{
			if (!(o is BasicMonth) || o == null)
			{
				return false;
			}
			else
			{
				return base.getValue() == ((BasicInt)o).getValue();
			}
		}
	}

}