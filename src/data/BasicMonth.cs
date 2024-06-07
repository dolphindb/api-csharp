
using dolphindb.io;
using System;

namespace dolphindb.data
{

    public class BasicMonth : BasicInt
    {
        private static string format = "yyyy.MM";

        public BasicMonth(int year, int month) : base(year * 12 + month)
        {
        }
        public BasicMonth(DateTime value) : base(value.Year * 12 + value.Month - 1)
        {
        }

        public BasicMonth(ExtendedDataInput @in) : base(@in)
        {
        }

        public BasicMonth(int value) : base(value)
        {
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
                return getValue().ToString(format)+"M";
            }

        }

        public override void setObject(object value)
        {
            if (value != null && value.GetType() == Type.GetType("System.DateTime"))
            {
                var v = (DateTime)value;
                base.setObject(v.Year * 12 + v.Month - 1);
            }
        }
    }

}