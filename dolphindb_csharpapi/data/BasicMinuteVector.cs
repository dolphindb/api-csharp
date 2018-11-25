using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

    public class BasicMinuteVector : BasicIntVector
    {

        public BasicMinuteVector(int size) : base(DATA_FORM.DF_VECTOR, size)
        {
        }

        public BasicMinuteVector(IList<int?> list) : base(list)
        {
        }

        public BasicMinuteVector(int[] array) : base(array)
        {
        }

        protected internal BasicMinuteVector(DATA_FORM df, int size) : base(df, size)
        {
        }

        protected internal BasicMinuteVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
        {
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_MINUTE;
        }

        public override IScalar get(int index)
        {
            return new BasicMinute(getInt(index));
        }

        public virtual TimeSpan getMinute(int index)
        {
            if (isNull(index))
            {
                return TimeSpan.MinValue;
            }
            else
            {
                return Utils.parseMinute(getInt(index));
            }
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() == DATA_TYPE.DT_MINUTE)
            {
                setMinute(index, ((BasicMinute)value).getValue());
            }
        }

        public virtual void setMinute(int index, TimeSpan time)
        {
            setInt(index, Utils.countMinutes(time));
        }

        public override Type getElementClass()
        {
            return typeof(BasicMinute);
        }

        public override object getList()
        {
            return base.getList();
        }

        public override void set(int index, string value)
        {
            if (value.EndsWith("m"))
            {
                var tmp = value.Remove(value.Length - 1);
                var hourMinute = tmp.Split(':');
                int hour = 0, minute = 0;
                if (hourMinute.Length >= 2)
                {
                    int.TryParse(hourMinute[0], out hour);
                    int.TryParse(hourMinute[1], out minute);
                }
                if (hour > 0 && minute > 0)
                {
                    base.set(index, new BasicInt(Utils.countMinutes(hour, minute)));
                    return;
                }
            }
            base.set(index, value);
        }
    }

}