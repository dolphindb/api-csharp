using dolphindb.io;
using System;

namespace dolphindb.data
{
	public class BasicNanoTimestamp : BasicLong
	{
		private static string format = "yyyy.MM.dd'T'HH:mm:ss.fffffff";

		public BasicNanoTimestamp(DateTime value) : base(Utils.countNanoseconds(value))
		{
		}

		public BasicNanoTimestamp(ExtendedDataInput @in) : base(@in)
		{
		}

        public BasicNanoTimestamp(long value) : base(value)
		{
		}

		public override DATA_CATEGORY getDataCategory()
		{
				return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
				return DATA_TYPE.DT_NANOTIMESTAMP;
		}

        public override object getObject()
        {
            return getValue();
        }
        public new DateTime getValue()
		{
				if (isNull())
				{
					return DateTime.MinValue;
				}
				else
				{
					return Utils.parseNanoTimestamp(base.getValue());
				}
		}
        public override object getTemporal()
        {
            return getValue();
        }

        public long getInternalValue()
        {
            return base.getValue();
        }

        public override string getString()
        {
            if (isNull())
            {
                return "";
            }
            else
            {
                DateTime dateTime = getValue();
                long last = (long)base.getValue() % 100;
                return dateTime.ToString(format)+last.ToString("00");
            }
        }

        public override void setObject(object value)
        {
            if (value != null && value.GetType() == Type.GetType("System.DateTime"))
            {
                base.setObject(Utils.countNanoseconds(Convert.ToDateTime(value)));
            }
        }
    }

}