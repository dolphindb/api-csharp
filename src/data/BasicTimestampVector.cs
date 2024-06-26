﻿using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{


    public class BasicTimestampVector : BasicLongVector
    {

        public BasicTimestampVector(int size) : base(size)
        {
        }

        public BasicTimestampVector(IList<long?> list) : base(list)
        {
        }

        public BasicTimestampVector(long[] array) : base(array)
        {
        }

        protected internal BasicTimestampVector(DATA_FORM df, int size) : base(df, size)
        {
        }

        protected internal BasicTimestampVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
        {
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_TIMESTAMP;
        }

        public override IScalar get(int index)
        {
            return new BasicTimestamp(getLong(index));
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() == DATA_TYPE.DT_TIMESTAMP)
            {
                setTimestamp(index, ((BasicTimestamp)value).getValue());
            }
            else
                throw new Exception("The value must be a timestamp scalar. ");
        }

        public virtual DateTime getTimestamp(int index)
        {
            if (isNull(index))
            {
                return DateTime.MinValue;
            }
            else
            {
                return Utils.parseTimestamp(getLong(index));
            }
        }

        public virtual void setTimestamp(int index, DateTime dt)
        {
            setLong(index, Utils.countMilliseconds(dt));
        }

        public override Type getElementClass()
        {
            return typeof(BasicTimestamp);
        }

        public override object getList()
        {
            return base.getList();
        }

        //set timestamp : 2018.02.02T06:07:11.123
        public override void set(int index, string value)
        {
            DateTime dt = new DateTime();
            long v = 0;
            if (!long.TryParse(value, out v))
            {
                if (DateTime.TryParse(value, out dt))
                {
                    base.set(index, new BasicLong(Utils.countMilliseconds(dt)));
                    return;
                }
            }
            else
            {
                //Utils.countMilliseconds(DateTime.MaxValue)>long.MaxValue>int.MaxValue.
                //If use long.MaxValue as border, there will occur error when parse to string
                if (0<=v && v<=int.MaxValue)
                {
                    values[index] = v;
                    return;
                }
            }
            setNull(index);
            Console.WriteLine("[Warning]:Invalid value " + value + " for set(int index, string value) function and the value is set to null. Valid value must be [yyyy.MM.ddTHH:mm:ss.fff] or [fff] format.");
        }

        public override void add(object value)
        {
            if (value is DateTime)
            {
                base.add(Utils.countMilliseconds((DateTime)value));
            }
            else if (value is String)
            {
                DateTime dtm = new DateTime();
                if (DateTime.TryParse(value.ToString(), out dtm))
                {
                    base.add(Utils.countMilliseconds(dtm));
                    return;
                }
            }
            else
                throw new Exception("The value must be a timeStamp scalar. ");

        }

        public override IVector getSubVector(int[] indices)
        {
            return new BasicTimestampVector(getSubArray(indices));
        }

        public override IEntity getEntity(int index)
        {
            return new BasicTimestamp(values[index]);
        }
    }

}