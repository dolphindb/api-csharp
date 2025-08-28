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
            BasicTime.checkTimeSpanToTime(time);
            setInt(index, (int)Utils.countMilliseconds(time.Hours,time.Minutes,time.Seconds,time.Milliseconds));
        }

        public override Type getElementClass()
        {
            return typeof(BasicTime);
        }

        public override void set(int index, string value)
        {
            TimeSpan ts = new TimeSpan();
            int v = 0;
            if (!int.TryParse(value, out v))
            {
                var tmp = value.Split('.');
                if (tmp.Length >= 2)
                {
                    int milli = 0;
                    if (int.TryParse(tmp[1], out milli))
                    {
                        if (TimeSpan.TryParse(tmp[0], out ts))
                        {
                            base.set(index, new BasicInt(Utils.countMilliseconds(ts) + milli));
                            return;
                        }
                    }
                }
            }
            else
            {
                if (0<=v && v<=10085477)//Utils.countMilliseconds(TimeSpan.MaxValue)
                {
                    values[index] = v;
                    return;
                }
            }
            setNull(index);
            Console.WriteLine("[Warning]:Invalid value " + value + " for set(int index, string value) function and the value is set to null. Valid value must be [mm:ss] or [ss] format.");
        }

        public override void add(object value)
        {
            if (value is TimeSpan)
            {
                BasicTime.checkTimeSpanToTime((TimeSpan)value);
                base.add(Utils.countMilliseconds((TimeSpan)value));
            }
            else if(value is DateTime)
            {
                base.add(Utils.countMilliseconds(((DateTime)value).TimeOfDay));
            }
            else if(value is string)
            {
                TimeSpan ts = new TimeSpan();
                if (TimeSpan.TryParse((string)value, out ts))
                {
                    BasicTime.checkTimeSpanToTime(ts);
                    int data = new BasicInt(Utils.countMilliseconds(ts)).getInt();
                    base.add(data);
                    return;
                }
            }
            else
            { 
                base.add(value);
            }
        }

        public override IVector getSubVector(int[] indices)
        {
            return new BasicTimeVector(getSubArray(indices));
        }

        public override IEntity getEntity(int index)
        {
            return new BasicTime(values[index]);
        }
    }

}