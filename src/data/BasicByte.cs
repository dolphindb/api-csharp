using System;
using dolphindb.io;

namespace dolphindb.data
{

    public class BasicByte : AbstractScalar, IComparable<BasicByte>
    {
        private byte value;
        static string formatStr = "0";

        public BasicByte(byte value)
        {
            this.value = value;
        }

        public BasicByte(ExtendedDataInput @in)
        {
            value = @in.readByte();
        }

        public virtual byte getValue()
        {
            return value;
        }

        public override bool isNull()
        {
            return value == 128;
        }

        public override void setNull()
        {
            value = 128;
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.INTEGRAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_BYTE;
        }

        public override Number getNumber()
        {
            if (isNull())
                return null;
            else
                return new Number(value);
        }


        public override object getTemporal()
        {
            throw new Exception("Imcompatible data type");
        }


        public override string ToString()
        {
            return getString();
        }
        public override string getString()
        {
            if (isNull())
            {
                return "";
            }
            else if (value > 31 && value < 127)
            {
                return "'" + ((char)value).ToString() + "'";
            }
            else
            {
                return value.ToString(formatStr);
            }
        }

        public override bool Equals(object o)
        {
            if (!(o is BasicByte) || o == null)
            {
                return false;
            }
            else
            {
                return value == ((BasicByte)o).value;
            }
        }

        public override int GetHashCode()
        {
            return (new byte?(value)).GetHashCode();
        }

        public override void writeScalarToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeByte(value);
        }

        public virtual int CompareTo(BasicByte o)
        {
            return value.CompareTo(o.value);
        }

        public override object getObject()
        {
            return getValue();
        }

        public override void setObject(object value)
        {
            this.value = Convert.ToByte(value);
        }

        public override int hashBucket(int buckets)
        {
            if (value > 0)
                return value % buckets;
            else
                return -1;
            
        }
    }

}