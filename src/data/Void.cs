using System;
using dolphindb.io;

namespace dolphindb.data
{
    public class Void : AbstractScalar
    {

        public override bool isNull()
        {
            return true;
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.NOTHING;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_VOID;
        }

        public override string getString()
        {
            return "";
        }

        public override bool Equals(object o)
        {
            if (!(o is Void) || o == null)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        public override int GetHashCode()
        {
            return 0;
        }


        public override void writeScalarToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeBoolean(true); //explicit null value
        }

        public override object getObject()
        {
            return null;
        }

        public override void setObject(object value)
        {
            return;
        }

        public override int hashBucket(int buckets)
        {
            throw new NotImplementedException();
        }
    }

}