﻿using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

    public class BasicDateTimeVector : BasicIntVector
    {

        public BasicDateTimeVector(int size) : base(size)
        {
        }

        public BasicDateTimeVector(IList<int?> list) : base(list)
        {
        }

        public BasicDateTimeVector(int[] array) : base(array)
        {
        }

        protected internal BasicDateTimeVector(DATA_FORM df, int size) : base(df, size)
        {
        }

        protected internal BasicDateTimeVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
        {
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_DATETIME;
        }

        public override IScalar get(int index)
        {
            return new BasicDateTime(getInt(index));
        }

        public virtual DateTime getDateTime(int index)
        {
            if (isNull(index))
            {
                return DateTime.MinValue;
            }
            else
            {
                return Utils.parseDateTime(getInt(index));
            }
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() == DATA_TYPE.DT_DATETIME)
            {
                setDateTime(index, ((BasicDateTime)value).getValue());
            }
            else
                throw new Exception("The value must be a dateTime scalar. ");
        }

        public virtual void setDateTime(int index, DateTime dt)
        {
            setInt(index, Utils.countSeconds(dt));
        }

        public override Type getElementClass()
        {
            return typeof(BasicDateTime);
        }
        public override object getList()
        {
            return base.getList();
        }
        public override void set(int index, string value)
        {
            int v;
            DateTime dtm = new DateTime();
            if (DateTime.TryParse(value, out dtm))
            {
                base.set(index, new BasicInt(Utils.countSeconds(dtm)));
                return;
            }
            else if (int.TryParse(value, out v))
            {
                if (v >= 0)
                {
                    values[index] = v; //DateTime.MaxValue second range is larger than int
                    return;
                }
            }
            setNull(index);
            Console.WriteLine("[Warning]:Invalid value " + value + " for set(int index, string value) function and the value is set to null. The value format must be compatible with the DateTime.TryParse()");
        }

        public override void add(object value)
        {
            if (value is DateTime)
            {
                base.add(Utils.countSeconds((DateTime)value));
            }
            else if (value is String)
            {
                DateTime dtm = new DateTime();
                if (DateTime.TryParse(value.ToString(), out dtm))
                {
                    base.add(Utils.countSeconds(dtm));
                    return;
                }
            }
            else
                throw new Exception("The value must be a dateTime scalar. ");

        }
        public override IVector getSubVector(int[] indices)
        {
            return new BasicDateTimeVector(getSubArray(indices));
        }

        public override IEntity getEntity(int index)
        {
            return new BasicDateTime(values[index]);
        }
    }

}