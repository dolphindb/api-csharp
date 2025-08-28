using dolphindb.io;
using System;

namespace dolphindb.data
{
    public class BasicTime : BasicInt
    {
        private static string format = @"hh\:mm\:ss\.fff";

        public BasicTime(TimeSpan value) : base((int)Utils.countMilliseconds(value.Hours,value.Minutes,value.Seconds,value.Milliseconds))
        {
            checkTimeSpanToTime(value);
        }

        public BasicTime(ExtendedDataInput @in) : base(@in)
        {
        }

        public BasicTime(int value) : base(value)
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
                return Utils.parseTime(base.getValue());
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

        public override void setObject(object value)
        {
            if (value != null && value.GetType() == Type.GetType("System.TimeSpan"))
            {
                var t = (TimeSpan)value;
                checkTimeSpanToTime(t);
                base.setObject(Utils.countMilliseconds(t.Hours,t.Minutes,t.Seconds,t.Milliseconds));
            }
        }
        public static void checkTimeSpanToTime(TimeSpan value){
            if(value.Days != 0){
                throw new TimeoutException("To convert BasicTime, TimeSpan's days must equal zero. ");
            }
        }
    }

}