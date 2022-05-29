using System;
using dolphindb.io;

namespace dolphindb.data
{
    public class BasicInt128 : AbstractScalar
    {

        protected Long2 value;

        public BasicInt128(long high, long low)
        {
            value = new Long2(high, low);
        }

        public BasicInt128(ExtendedDataInput @in)
        {
            value = @in.readLong2();
        }

        public static BasicInt128 fromString(String num)
        {
            if (num.Length != 32)
                throw new Exception("Invalid int128 string.");
            long high = parseLongFromScale16(num.Substring(0, 16));
            long low = parseLongFromScale16(num.Substring(16));
            return new BasicInt128(high, low);
        }

        public Long2 getLong2()
        {
            return value;
        }


        public override bool isNull()
        {
            return value.isNull();
        }


        public override void setNull()
        {
            value.setNull();
        }

        public long getMostSignicantBits()
        {
            return value.high;
        }

        public long getLeastSignicantBits()
        {
            return value.low;
        }

        public override Number getNumber()
        {
            throw new Exception("Imcompatible data type");
        }

        public override object getObject()
        {
            throw new NotImplementedException();
        }

        public override void setObject(object value)
        {
            throw new NotImplementedException();
        }
        public override Object getTemporal()
        {
            throw new Exception("Imcompatible data type");
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.BINARY;
        }
        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_INT128;
        }

        public override String getString()
        {
            return String.Format("{0:x16}", value.high) + String.Format("{0:x16}", value.low);
        }

        public bool equals(Object o)
        {
            if (!(o is BasicInt128) || o == null)
			    return false;
		    else
			    return value.equals(((BasicInt128)o).value);
        }


        public int hashCode()
        {
            return value.hashCode();
        }


        protected override void writeScalarToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeLong2(value);
        }

        public Long2 getValue()
        {
            return value;
        }

        protected static long parseLongFromScale16(string str)
        {
            long ret = 0;
            for(int i = 0; i < str.Length; ++i)
            {
                ret *= 16;
                char data = str[i];
                if(data >= '0' && data <= '9')
                {
                    ret += data - '0';
                }else if(data >= 'a' && data <= 'f')
                {
                    ret += data - 'a' + 10;
                }
                else if (data >= 'A' && data <= 'F')
                {
                    ret += data - 'A' + 10;
                }
                else
                {
                    throw new Exception("Invalid character in hexadecimal string. ");
                }
            }
            return ret;
        }

        public override bool Equals(object o)
        {
            if (!(o is BasicInt128) || o == null)
            {
                return false;
            }
            else
            {
                return getValue().equals(((BasicInt128)o).getValue());
            }
        }
        public override int hashBucket(int buckets)
        {
            return value.hashBucket(buckets);
        }
    }
}
