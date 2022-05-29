using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
    public class BasicSecondVector : BasicIntVector
    {

        public BasicSecondVector(int size) : base(DATA_FORM.DF_VECTOR, size)
        {
        }

        public BasicSecondVector(IList<int?> list) : base(list)
        {
        }

        public BasicSecondVector(int[] array) : base(array)
        {
        }

        protected internal BasicSecondVector(DATA_FORM df, int size) : base(df, size)
        {
        }

        protected internal BasicSecondVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
        {
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_SECOND;
        }

        public override IScalar get(int index)
        {
            return new BasicSecond(getInt(index));
        }

        public virtual TimeSpan getSecond(int index)
        {
            if (isNull(index))
            {
                return TimeSpan.MinValue;
            }
            else
            {
                return Utils.parseSecond(getInt(index));
            }
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() == DATA_TYPE.DT_SECOND)
            {
                setSecond(index, ((BasicSecond)value).getValue());
            }
            else
                throw new Exception("The value must be a second scalar. ");
        }

        public virtual void setSecond(int index, TimeSpan time)
        {
            BasicSecond.checkTimeSpanToSecond(time);
            setInt(index, Utils.countSeconds(time));
        }

        public override Type getElementClass()
        {
            return typeof(BasicSecond);
        }

        public override object getList()
        {
            return base.getList();
        }

        //set second : 06:07:11
        public override void set(int index, string value)
        {
            TimeSpan ts = new TimeSpan();
            long v = 0;
            if (!long.TryParse(value, out v))
            {
                if (TimeSpan.TryParse(value, out ts))
                {
                    BasicSecond.checkTimeSpanToSecond(ts);
                    base.set(index, new BasicLong(Utils.countSeconds(ts)));
                    return;
                }
            }
            base.set(index, value);
        }

        public override void add(object value)
        {
            if (value is TimeSpan)
            {
                BasicSecond.checkTimeSpanToSecond((TimeSpan)value);
                base.add(Utils.countSeconds((TimeSpan)value));
                return;
            }
            base.add(value);
        }
        public override IVector getSubVector(int[] indices)
        {
            return new BasicSecondVector(getSubArray(indices));
        }
    }

}