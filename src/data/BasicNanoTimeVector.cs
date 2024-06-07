using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
	public class BasicNanoTimeVector : BasicLongVector
	{

		public BasicNanoTimeVector(int size) : base(DATA_FORM.DF_VECTOR, size)
		{
		}

		public BasicNanoTimeVector(IList<long?> list) : base(list)
		{
		}

		public BasicNanoTimeVector(long[] array) : base(array)
		{
		}

		protected internal BasicNanoTimeVector(DATA_FORM df, int size) : base(df, size)
		{
		}

		protected internal BasicNanoTimeVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
		{
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.TEMPORAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_NANOTIME;
		}

		public override IScalar get(int index)
		{
			return new BasicNanoTime(getLong(index));
		}

		public virtual TimeSpan getNanoTime(int index)
		{
			if (isNull(index))
			{
				return TimeSpan.MinValue;
			}
			else
			{
				return Utils.parseNanoTime(getLong(index));
			}
		}

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() == DATA_TYPE.DT_NANOTIME)
            {
                setNanoTime(index, ((BasicNanoTime)value).getValue());
            }
            else
                throw new Exception("The value must be a nanoTime scalar. ");
        }

        public virtual void setNanoTime(int index, TimeSpan time)
		{
            BasicNanoTime.checkTimeSpanToNanoTime(time);
			setLong(index, Utils.countNanoseconds(time));
		}

		public override Type getElementClass()
		{
			return typeof(BasicNanoTime);
		}

        public override object getList()
        {
            return base.getList();
        }
        //set nanotime : 06:07:11.123456789
        public override void set(int index, string value)
        {
            TimeSpan ts = new TimeSpan();
            long v = 0;
            if (!long.TryParse(value ,out v))
            {
                var tmp = value.Split('.');
                if (tmp.Length >= 2)
                {
                    long nano = 0;
                    if(long.TryParse(tmp[1],out nano))
                    {
                        if (TimeSpan.TryParse(tmp[0], out ts))
                        {
                            BasicNanoTime.checkTimeSpanToNanoTime(ts);
                            base.set(index, new BasicLong(Utils.countNanoseconds(ts) + nano));
                            return;
                        }
                    }
                }
            }
            else
            {
                if(0<=v && v <= long.MaxValue/100) //TimeSpan.MaxValue NanoSecond
                {
                    values[index] = v;
                    return;
                }
            }
            setNull(index);
            Console.WriteLine("[Warning]:Invalid value " + value + " for set(int index, string value) function and the value is set to null.Valid value must be [HH:mm:ss.fffffffff] or [fffffffff] format.");
        }


        public override void add(object value)
        {   
            if (value is TimeSpan)
            {
                BasicNanoTime.checkTimeSpanToNanoTime((TimeSpan)value);
                base.add(Utils.countNanoseconds((TimeSpan)value));
                return;
            }else if(value is string)
            {
                TimeSpan ts = new TimeSpan();
                long v = 0;
                if (!long.TryParse((string)value, out v))
                {
                    var tmp = ((string)value).Split('.');
                    if (tmp.Length >= 2)
                    {
                        long nano = 0;
                        if (long.TryParse(tmp[1], out nano))
                        {
                            if (TimeSpan.TryParse(tmp[0], out ts))
                            {
                                BasicNanoTime.checkTimeSpanToNanoTime(ts);
                                long data = new BasicLong(Utils.countNanoseconds(ts) + nano).getLong();
                                base.add(data);
                                return;
                            }
                        }
                    }
                }
            }
            base.add(value);
        }

        public override IVector getSubVector(int[] indices)
        {
            return new BasicNanoTimeVector(getSubArray(indices));
        }

        public override IEntity getEntity(int index)
        {
            return new BasicNanoTime(values[index]);
        }
    }

}