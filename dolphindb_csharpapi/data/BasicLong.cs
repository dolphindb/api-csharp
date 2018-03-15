using dolphindb.data;
using dolphindb.io;
using System;

namespace dolphindb.data
{
    /// 
    /// <summary>
    /// Corresponds to DolphindB long scalar
    /// 
    /// </summary>

    public class BasicLong : AbstractScalar, IComparable<BasicLong>
    {
        private long value;

        public BasicLong(long value)
        {
            this.value = value;
        }

        public BasicLong(ExtendedDataInput @in)
        {
            value = @in.readLong();
        }

        public virtual long getValue()
        {
            return value;
        }

        public override bool isNull()
        {
            return value == long.MinValue;
        }

        public override void setNull()
        {
            value = long.MinValue;
        }

        public override DATA_CATEGORY getDataCategory()
        {
                return DATA_CATEGORY.INTEGRAL;
        }

        public override DATA_TYPE getDataType()
        {

                return DATA_TYPE.DT_LONG;
        }

        public override Number getNumber()
        {
            if (isNull())
            {
                return null;
            }
            else
            {
                return new Number(value);
            }
        }

        public override object getTemporal()
        {
            throw new Exception("Imcompatible data type");
        }

        public override string getString()
        {
            if (isNull())
            {
                return "";
            }
            else
            {
                return value.ToString();
            }
        }

        public override bool Equals(object o)
        {
            if (!(o is BasicLong) || o == null)
            {
                return false;
            }
            else
            {
                return value == ((BasicLong)o).value;
            }
        }

        public override int GetHashCode()
        {
            return (new long?(value)).GetHashCode();
        }

        protected override void writeScalarToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeLong(value);
        }

        public virtual int CompareTo(BasicLong o)
        {
            return value.CompareTo(o.value);
        }
    }

}