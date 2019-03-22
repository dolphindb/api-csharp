using dolphindb.io;
using System;

namespace dolphindb.data
{
    public class BasicTimestamp : BasicLong
    {
        private static string format = "yyyy.MM.dd'T'HH:mm:ss.fff";

        public BasicTimestamp(DateTime value) : base(Utils.countMilliseconds(value))
        {
        }

        public BasicTimestamp(ExtendedDataInput @in) : base(@in)
        {
        }

        protected internal BasicTimestamp(long value) : base(value)
        {
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_TIMESTAMP;
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
                return Utils.parseTimestamp(base.getValue());
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
                DateTime dt = getValue();

                return dt.ToString(format);
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
                return base.getValue() == ((BasicLong)o).getValue();
            }
        }
    }

}