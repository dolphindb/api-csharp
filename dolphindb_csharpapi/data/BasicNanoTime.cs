using dolphindb.io;
using System;

namespace dolphindb.data
{

	public class BasicNanoTime : BasicLong
	{
		private static string format = "HH:mm:ss.SSSSSSSSS";

		public BasicNanoTime(DateTime value) : base(Utils.countNanoseconds(value))
		{
		}


		public BasicNanoTime(ExtendedDataInput @in) : base(@in)
		{
		}

		protected internal BasicNanoTime(long value) : base(value)
		{
		}

		public override DATA_CATEGORY getDataCategory()
		{
				return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_NANOTIME;
		}

		public virtual DateTime getValue()
		{
				if (isNull())
				{
					return DateTime.MinValue;
				}
				else
				{
					return Utils.parseNanoTime(base.getValue());
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
            if (!(o is BasicMinute) || o == null)
            {
                return false;
            }
            else
            {
                return base.getValue() == ((BasicLong)o).getValue();
            }
        }
    }

}