using dolphindb.io;
using System;

namespace dolphindb.data
{

	public class BasicDateTime : BasicInt
	{
		private static string format = "yyyy.MM.dd'T'HH:mm:ss";

		public BasicDateTime(DateTime value) : base(Utils.countSeconds(value))
		{
		}

		public BasicDateTime(ExtendedDataInput @in) : base(@in)
		{
		}

		protected internal BasicDateTime(int value) : base(value)
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

		public new DateTime getValue()
		{
				if (isNull())
				{
					return DateTime.MinValue;
				}
				else
				{
					return Utils.parseDateTime(base.getValue());
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
					return this.getValue().ToString(format);
				}
		}

		public override bool Equals(object o)
		{
			if (!(o is BasicDateTime) || o == null)
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