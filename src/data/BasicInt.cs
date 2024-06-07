﻿using System;
using dolphindb.io;
namespace dolphindb.data
{
    /// <summary>
    /// Corresponds to DolphinDB int scalar
    /// </summary>

    public class BasicInt : AbstractScalar, IComparable<BasicInt>
    {
        private int value;
        static string formatStr = "0";

        public BasicInt(int value)
        {
            this.value = value;
        }

        public BasicInt(ExtendedDataInput @in)
        {
            value = @in.readInt();
        }

        public virtual int getValue()
        {
            return value;
        }

        public int getInt()
        {
            return value;
        }

        public override bool isNull()
        {
            return value == int.MinValue;
        }

        public override void setNull()
        {
            value = int.MinValue;
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.INTEGRAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_INT;
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

        public override bool Equals(object o)
        {
            if (!(o is BasicInt) || o == null)
            {
                return false;
            }
            else
            {
                return value == ((BasicInt)o).value;
            }
        }

        public override int GetHashCode()
        {
            return (new int?(value)).GetHashCode();
        }

        public override void writeScalarToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeInt(value);
        }

        public int CompareTo(BasicInt o)
        {
            return value.CompareTo(o.value);
        }

        public override string getString()
        {
            return value.ToString(formatStr);
        }
        public override object getObject()
        {
            return getValue();
        }

        public override string ToString()
        {
            return getString();
        }

        public override void setObject(object value)
        {
            this.value = Convert.ToInt32(value);
        }

        public override int hashBucket(int buckets)
        {
            if (value >= 0)
                return value % buckets;
            else if (value == int.MinValue)
                return -1;
            else
            {
                return (int)((4294967296L + value) % buckets);
            }
        }
    }

}