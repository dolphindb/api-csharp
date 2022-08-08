using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

    public class BasicDateHourVector : BasicIntVector
    {

        public BasicDateHourVector(int size) : base(size)
        {
        }

        public BasicDateHourVector(IList<int?> list) : base(list)
        {
        }

        public BasicDateHourVector(int[] array) : base(array)
        {
        }

        protected BasicDateHourVector(int[] array, bool copy) : base(array, copy)
        {

        }

        protected internal BasicDateHourVector(DATA_FORM df, int size) : base(df, size)
        {
        }

        protected internal BasicDateHourVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
        {
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_DATEHOUR;
        }

        public override IScalar get(int index)
        {
            return new BasicDateHour(getInt(index));
        }

        public virtual DateTime getDateTime(int index)
        {
            if (isNull(index))
            {
                return DateTime.MinValue;
            }
            else
            {
                return Utils.parseDateHour(getInt(index));
            }
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() == DATA_TYPE.DT_DATEHOUR)
            {
                setDateTime(index, ((BasicDateHour)value).getValue());
            }
            else
                throw new Exception("The value must be a dateTime scalar. ");
        }

        public virtual void setDateTime(int index, DateTime dt)
        {
            setInt(index, Utils.countHours(dt));
        }

        public override Type getElementClass()
        {
            return typeof(BasicDateHour);
        }
        public override object getList()
        {
            return base.getList();
        }
        public override void set(int index, string value)
        {
            DateTime dtm = new DateTime();
            if (DateTime.TryParse(value, out dtm))
            {
                base.set(index, new BasicInt(Utils.countHours(dtm)));
                return;
            }
            base.set(index, value);
        }

        public override void add(object value)
        {
            if (value is DateTime)
            {
                base.add(Utils.countHours((DateTime)value));
            }
            else if (value is String)
            {
                DateTime dtm = new DateTime();
                if (DateTime.TryParse(value.ToString(), out dtm))
                {
                    base.add(Utils.countHours(dtm));
                    return;
                }
            }
            else
                throw new Exception("The value must be a dateTime scalar. ");

        }
        public override IVector getSubVector(int[] indices)
        {
            return new BasicDateHourVector(getSubArray(indices), false);
        }

        public override IEntity getEntity(int index)
        {
            return new BasicDateHour(values[index]);
        }
    }

}