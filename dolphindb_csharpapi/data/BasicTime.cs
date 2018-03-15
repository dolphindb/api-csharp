using dolphindb.io;
using System;

namespace dolphindb.data
{
	public class BasicTime : BasicInt
	{
		private static string format = "HH:mm:ss.SSS";

		public BasicTime(DateTime value) : base(Utils.countMilliseconds(value))
		{
		}

		public BasicTime(ExtendedDataInput @in) : base(@in)
		{
		}

		protected internal BasicTime(int value) : base(value)
		{
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_TIME;
		}

		public new  DateTime getValue()
		{
				if (isNull())
				{
					return DateTime.MinValue;
				}
				else
				{
					return Utils.parseTime(base.getValue());
				}

		}
        public override object getTemporal()
        {
            return this.getValue();
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
            if (!(o is BasicTimestamp) || o == null)
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