using dolphindb.io;
using System;

namespace dolphindb.data
{

    public class BasicNanoTime : BasicLong
    {
        private static string format =  "HH:mm:ss.fffffff";// if format='c，it automatically omits the zeros, making it impossible to display all 9 digits.

        public BasicNanoTime(TimeSpan value) : base(Utils.countNanoseconds(value))
        {
            checkTimeSpanToNanoTime(value);
        }


        public BasicNanoTime(ExtendedDataInput @in) : base(@in)
        {
        }

        public BasicNanoTime(long value) : base(value)
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
                return Utils.parseNanoTime(base.getValue());
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
                DateTime dateTime = Utils.parseNanoTimestamp(base.getValue());
                long last = (long)base.getValue() % 100;
                return dateTime.ToString(format) + last.ToString("00");
            }
        }

        public override void setObject(object value)
        {
            if (value != null && value.GetType() == Type.GetType("System.TimeSpan"))
            {
                checkTimeSpanToNanoTime((TimeSpan)value);
                base.setObject(Utils.countNanoseconds((TimeSpan)value));
            }
        }

        public static void checkTimeSpanToNanoTime(TimeSpan value){
            if(value.Days != 0){
                throw new TimeoutException("To convert BasicNanoTime, TimeSpan's days must equal zero. ");
            }
        }
    }

}