using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{


    public class BasicMonthVector : BasicIntVector
    {

        public BasicMonthVector(int size) : base(DATA_FORM.DF_VECTOR, size)
        {
        }

        public BasicMonthVector(IList<int?> list) : base(list)
        {
        }

        public BasicMonthVector(int[] array) : base(array)
        {
        }

        protected internal BasicMonthVector(DATA_FORM df, int size) : base(df, size)
        {
        }

        protected internal BasicMonthVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
        {
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_MONTH;
        }

        public override IScalar get(int index)
        {
            return new BasicMonth(getInt(index));
        }

        public virtual DateTime getMonth(int index)
        {
            return Utils.parseMonth(getInt(index));
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() == DATA_TYPE.DT_MONTH)
            {
                setMonth(index, ((BasicMonth)value).getValue());
            }
            else
                throw new Exception("The value must be a month scalar. ");
        }

        public virtual void setMonth(int index, DateTime month)
        {
            setInt(index, Utils.countMonths(month));
        }

        public override Type getElementClass()
        {
            return typeof(DateTime);
        }

        public override object getList()
        {
            return base.getList();
        }
        //set with 2018.01M
        public override void set(int index, string value)
        {
            if (value.EndsWith("M"))
            {
                var tmp = value.Remove(value.Length - 1);
                var yearMonth = tmp.Split('.');
                int year=0, month=0;
                if (yearMonth.Length >= 2)
                {
                    int.TryParse(yearMonth[0],out year);
                    int.TryParse(yearMonth[1], out month);
                }
                if (year > 0 && month > 0)
                {
                    base.set(index, new BasicInt(Utils.countMonths(year, month)));
                    return;
                }
                    
            }
            base.set(index, value);
        }

        public override void add(object value)
        {
            if(value is String)
            {
                string monthstr = value.ToString();
                if (monthstr.EndsWith("M"))
                {
                    var tmp = monthstr.Remove(monthstr.Length - 1);
                    var yearMonth = tmp.Split('.');
                    int year = 0, month = 0;
                    if (yearMonth.Length >= 2)
                    {
                        int.TryParse(yearMonth[0], out year);
                        int.TryParse(yearMonth[1], out month);
                    }
                    if (year > 0 && month > 0)
                    {
                        base.add(Utils.countMonths(year, month));
                        return;
                    }

                }

            }
            base.add(value);
        }

        public override IVector getSubVector(int[] indices)
        {
            return new BasicMonthVector(getSubArray(indices));
        }

    }

}