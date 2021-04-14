﻿using dolphindb.io;
using System;

namespace dolphindb.data
{

    public class BasicNanoTime : BasicLong
    {
        private static string format = "c";

        public BasicNanoTime(TimeSpan value) : base(Utils.countNanoseconds(value))
        {
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
                return getValue().ToString(format);
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

        public override void setObject(object value)
        {
            if (value != null && value.GetType() == Type.GetType("System.TimeSpan"))
            {
                base.setObject(Utils.countNanoseconds((TimeSpan)value));
            }
        }
    }

}