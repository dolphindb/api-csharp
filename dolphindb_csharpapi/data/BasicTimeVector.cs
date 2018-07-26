using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

    public class BasicTimeVector : BasicIntVector
    {

        public BasicTimeVector(int size) : base(DATA_FORM.DF_VECTOR, size)
        {
        }

        public BasicTimeVector(IList<int?> list) : base(list)
        {
        }

        public BasicTimeVector(int[] array) : base(array)
        {
        }

        protected internal BasicTimeVector(DATA_FORM df, int size) : base(df, size)
        {
        }

        protected internal BasicTimeVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
        {
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_TIME;
        }

        public override IScalar get(int index)
        {
            return new BasicTime(getInt(index));
        }

        public virtual TimeSpan getTime(int index)
        {
            if (isNull(index))
            {
                return TimeSpan.MinValue;
            }
            else
            {
                return Utils.parseTime(getInt(index));
            }
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() == DATA_TYPE.DT_TIME)
            {
                setTime(index, ((BasicTime)value).getValue());
            }
        }

        public virtual void setTime(int index, TimeSpan time)
        {
            setInt(index, (int)Utils.countMilliseconds(time.Hours,time.Minutes,time.Seconds,time.Milliseconds));
        }

        public override Type getElementClass()
        {
            return typeof(BasicTime);
        }
    }

}