﻿using dolphindb.io;
using System;

namespace dolphindb.data
{

	public class BasicSecond : BasicInt
	{
		private static string format = "c";

		public BasicSecond(TimeSpan value) : base(Utils.countSeconds(value))
		{
		}

		public BasicSecond(ExtendedDataInput @in) : base(@in)
		{
		}

        public BasicSecond(int value) : base(value)
		{
		}

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_SECOND;
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
					return Utils.parseSecond(base.getValue());
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
				checkTimeSpanToSecond((TimeSpan)value);
                base.setObject(Utils.countSeconds((TimeSpan)value));
            }
        }

        public static void checkTimeSpanToSecond(TimeSpan value){
            if(value.Days != 0){
                throw new TimeoutException("To convert BasicSecond, TimeSpan's days must equal zero. ");
            }
        }
    }

}