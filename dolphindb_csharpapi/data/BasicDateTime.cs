using dolphindb.io;
using System;

namespace dolphindb.data
{

    public class BasicDateTime : BasicInt
    {
        private static string format = "yyyy.MM.dd'T'HH:mm:ss";

        public BasicDateTime(DateTime value) : base(Utils.countSeconds(value))
        {
        }

        public BasicDateTime(ExtendedDataInput @in) : base(@in)
        {
        }

        public BasicDateTime(int value) : base(value)
        {
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_DATETIME;
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
                return Utils.parseDateTime(base.getValue());
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
            if (!(o is BasicDateTime) || o == null)
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
            if(value != null && value.GetType() == Type.GetType("System.DateTime"))
            {
                base.setObject(Utils.countSeconds(Convert.ToDateTime(value)));
            }
        }
    }

}