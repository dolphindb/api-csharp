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
            return String.Format("%016x%016x", value.high, value.low);
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
    }
}
