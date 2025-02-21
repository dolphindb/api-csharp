using System;
using System.Collections.Generic;
using dolphindb.io;

namespace dolphindb.data
{
    public class BasicComplex : AbstractScalar
    {

        protected Double2 value;

        public BasicComplex(double real, double image)
        {
            value = new Double2(real, image);
        }

        public BasicComplex(ExtendedDataInput @in)
        {
            value = @in.readDouble2();
        }

        public Double2 getDouble2()
        {
            return value;
        }


        public override bool isNull()
        {
            return value.isNull();
        }


        public override void setNull()
        {
            value = new Double2(-double.MaxValue, -double.MaxValue);
        }

        public double getReal() { 
            return value.x;
        }

        public double getImage(){
            return value.y;
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
            return DATA_TYPE.DT_COMPLEX;
        }

        public override String getString()
        {
            if (isNull())
            {
                return "";
            }
            else
            {
                return value.x.ToString() + (value.y >= 0 ? "+" : "") + value.y.ToString() + "i";
            }
        }

        public bool equals(Object o)
        {
            if (!(o is BasicComplex) || o == null)
                return false;
            else
                return value.equals(((BasicComplex)o).value);
        }

        public override void writeScalarToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeDouble2(value);
        }

        public Double2 getValue()
        {
            return value;
        }

        public override int hashBucket(int buckets)
        {
            return value.hashBucket(buckets);
        }

        public override bool Equals(object o)
        {
            if (!(o is BasicComplex) || o == null)
            {
                return false;
            }
            else
            {
                return getValue().equals(((BasicComplex)o).getValue());
            }
        }

        public override int GetHashCode()
        {
            return value.GetHashCode();
        }
    }
}
