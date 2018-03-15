using dolphindb.io;
using System;

namespace dolphindb.data
{

	public class BasicSecond : BasicInt
	{
		private static string format = "HH:mm:ss";

		public BasicSecond(DateTime value) : base(Utils.countSeconds(value))
		{
		}

		public BasicSecond(ExtendedDataInput @in) : base(@in)
		{
		}

		protected internal BasicSecond(int value) : base(value)
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

		public new DateTime getValue()
		{
				if (isNull())
				{
					return DateTime.MinValue;
				}
				else
				{
					return Utils.parseSecond(base.getValue());
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
			if (!(o is BasicSecond) || o == null)
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