using dolphindb.io;
using System;

namespace dolphindb.data
{
    public class BasicMinute : BasicInt
    {
        private static string format = "c";

        public BasicMinute(TimeSpan value) : base(Utils.countMinutes(value))
        {
            checkTimeSpanToMinute(value);
        }

        public BasicMinute(ExtendedDataInput @in) : base(@in)
        {
        }

        public BasicMinute(int value) : base(value)
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
        public override object getObject()
        {
            return getValue();
        }

        public new TimeSpan getValue()
        {
            if (isNull())
            {
                return TimeSpan.MinValue;
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
                int hours = (base.getValue() / 60) % 24;
                TimeSpan ts = new TimeSpan(hours, base.getValue() % 60, 0);
                return ts.ToString(format);
            }
        }

        public override void setObject(object value)
        {
            if (value != null && value.GetType() == Type.GetType("System.TimeSpan"))
            {
                checkTimeSpanToMinute((TimeSpan)value);
                base.setObject(Utils.countMinutes((TimeSpan)value));
            }
        }

        public static void checkTimeSpanToMinute(TimeSpan value){
            if(value.Days != 0){
                throw new TimeoutException("To convert BasicMinute, TimeSpan's days must equal zero. ");
            }
        }
    }

}