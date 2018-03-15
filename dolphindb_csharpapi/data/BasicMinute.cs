using dolphindb.io;
using System;

namespace dolphindb.data
{
    public class BasicMinute : BasicInt
    {
        private static string format = "HH:mm'm'";

        public BasicMinute(DateTime value) : base(Utils.countMinutes(value))
        {
        }

        public BasicMinute(ExtendedDataInput @in) : base(@in)
        {
        }

        protected internal BasicMinute(int value) : base(value)
        {
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_MINUTE;
        }

        public new DateTime getValue()
        {
            if (isNull())
            {
                return DateTime.MinValue;
            }
            else
            {
                return Utils.parseMinute(base.getValue());
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
                return base.getValue() == ((BasicInt)o).getValue();
            }
        }
    }

}