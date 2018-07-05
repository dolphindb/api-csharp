using dolphindb.io;
using System;

namespace dolphindb.data
{
	public class BasicDate : BasicInt
	{
		private static string format = "yyyy.MM.dd";

		public BasicDate(DateTime value) : base(Utils.countDays(value))
		{
		}

		public BasicDate(ExtendedDataInput @in) : base(@in)
		{
		}

		protected internal BasicDate(int value) : base(value)
		{
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_DATE;
		}

		public new  DateTime getValue()
		{
            if (isNull())
            {
                return DateTime.MinValue;
            }
            else
            {
                return Utils.parseDate(base.getValue());
            }
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
					return getValue().ToString(format);
				}
		}

		public override bool Equals(object o)
		{
			if (!(o is BasicDate) || o == null)
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