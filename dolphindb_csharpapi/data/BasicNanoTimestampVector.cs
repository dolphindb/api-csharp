using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

    public class BasicNanoTimestampVector : BasicLongVector
    {

        public BasicNanoTimestampVector(int size) : base(size)
        {
        }

        public BasicNanoTimestampVector(IList<long?> list) : base(list)
        {
        }

        public BasicNanoTimestampVector(long[] array) : base(array)
        {
        }

        protected internal BasicNanoTimestampVector(DATA_FORM df, int size) : base(df, size)
        {
        }

        protected internal BasicNanoTimestampVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
        {
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_NANOTIMESTAMP;
        }

        public override IScalar get(int index)
        {
            return new BasicNanoTimestamp(getLong(index));
        }

        public virtual DateTime getNanoTimestamp(int index)
        {
            if (isNull(index))
            {
                return DateTime.MinValue;
            }
            else
            {
                return Utils.parseNanoTimestamp(getLong(index));
            }
        }

        public virtual void setNanoTimestamp(int index, DateTime dt)
        {
            setLong(index, Utils.countNanoseconds(dt));
        }

        public override Type getElementClass()
        {
            return typeof(BasicNanoTimestamp);
        }
    }

}