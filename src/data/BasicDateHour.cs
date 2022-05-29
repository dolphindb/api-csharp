using dolphindb.io;
using System;

namespace dolphindb.data
{

    public class BasicDateHour : BasicInt
    {
        private static string format = "yyyy.MM.dd'T'HH";

        public BasicDateHour(DateTime value) : base(Utils.countHours(value))
        {
        }

        public BasicDateHour(ExtendedDataInput @in) : base(@in)
        {
        }

        public BasicDateHour(int value) : base(value)
        {
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_DATEHOUR;
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
                return Utils.parseDateHour(base.getValue());
            }
        }

        public override object getTemporal()
        {
            return getValue();
        }

        public int getInternalValue()
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
                return getValue().ToString(format);
            }
        }

        public override bool Equals(object o)
        {
            if (!(o is BasicDateHour) || o == null)
            {
                return false;
            }
            else
            {
                return base.getValue() == ((BasicInt)o).getValue();
            }
        }

        public override void setObject(object value)
        {
            if (value != null && value.GetType() == Type.GetType("System.DateTime"))
            {
                base.setObject(Utils.countHours(Convert.ToDateTime(value)));
            }
        }
    }

}