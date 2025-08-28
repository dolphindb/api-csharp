using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
    public class BasicDateVector : BasicIntVector
    {

        public BasicDateVector(int size) : base(size)
        {
        }

        public BasicDateVector(IList<int?> list) : base(list)
        {
        }

        public BasicDateVector(int[] array) : base(array)
        {
        }

        protected internal BasicDateVector(DATA_FORM df, int size) : base(df, size)
        {
        }

        protected internal BasicDateVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
        {
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_DATE;
        }

        public override IScalar get(int index)
        {
            return new BasicDate(getInt(index));
        }

        public virtual DateTime getDate(int index)
        {
            if (isNull(index))
            {
                return DateTime.MinValue;
            }
            else
            {
                return Utils.parseDate(getInt(index));
            }
        }

        public virtual void setDate(int index, DateTime date)
        {
            setInt(index, Utils.countDays(date));
        }

        public override Type getElementClass()
        {
            return typeof(BasicDate);
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() == DATA_TYPE.DT_DATE)
            {
                setInt(index, ((BasicDate)value).getInt());
            }
            else
                throw new Exception("The value must be a date scalar. ");
        }

        public override void set(int index, string value)
        {
            int v;
            DateTime dtm = new DateTime();
            if(DateTime.TryParse(value,out dtm))
            {
                base.set(index, new BasicInt(Utils.countDays(dtm)));
                return;
            }else if(int.TryParse(value,out v))
            {
                if(0<=v && v <= 2932896) //Utils.countDays(DateTime.MaxValue)
                {
                    values[index] = v;
                    return;
                }
            }
            setNull(index);
            Console.WriteLine("[Warning]:Invalid value " + value + " for set(int index, string value) function and the value is set to null. The value format must be compatible with the DateTime.TryParse()");
        }

        public override void add(object value)
        {
            if(value is DateTime)
            {
                base.add(Utils.countDays((DateTime)value));
            }
            else if (value is String)
            {
                DateTime dtm = new DateTime();
                if (DateTime.TryParse(value.ToString(), out dtm))
                {
                    base.add(Utils.countDays(dtm));
                    return;
                }
            }
        }

        public override IEntity getEntity(int index)
        {
            return new BasicDate(values[index]);
        }
        
        public override IVector getSubVector(int[] indices)
        {
            return new BasicDateVector(getSubArray(indices));
        }
    }

}