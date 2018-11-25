﻿using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

    public class BasicNanoTimestampVector : BasicLongVector
    {

        public BasicNanoTimestampVector(int size) : base(size)
        {
        }

        public BasicNanoTimestampVector(IList<long?> list) : base(list)
        {
        }

        public BasicNanoTimestampVector(long[] array) : base(array)
        {
        }

        protected internal BasicNanoTimestampVector(DATA_FORM df, int size) : base(df, size)
        {
        }

        protected internal BasicNanoTimestampVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in)
        {
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_NANOTIMESTAMP;
        }

        public override IScalar get(int index)
        {
            return new BasicNanoTimestamp(getLong(index));
        }

        public virtual DateTime getNanoTimestamp(int index)
        {
            if (isNull(index))
            {
                return DateTime.MinValue;
            }
            else
            {
                return Utils.parseNanoTimestamp(getLong(index));
            }
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() == DATA_TYPE.DT_NANOTIMESTAMP)
            {
                setNanoTimestamp(index, ((BasicNanoTimestamp)value).getValue());
            }
        }

        public virtual void setNanoTimestamp(int index, DateTime dt)
        {
            setLong(index, Utils.countNanoseconds(dt));
        }

        public override Type getElementClass()
        {
            return typeof(BasicNanoTimestamp);
        }

        public override object getList()
        {
            return base.getList();
        }

        //set nanotime : 2018.02.02T06:07:11.123456789
        public override void set(int index, string value)
        {

            long v = 0;
            if (!long.TryParse(value, out v))
            {
                int lastDot = value.LastIndexOf('.');
                if (lastDot > 0)
                {
                    DateTime dtm = new DateTime();
                    string dt = value.Substring(0, lastDot);
                    string snano = value.Substring(lastDot+1);
                    long nano = 0;
                    if (long.TryParse(snano, out nano))
                    {
                        if (DateTime.TryParse(dt, out dtm))
                        {
                            base.set(index, new BasicLong(Utils.countNanoseconds(dtm) + nano));
                            return;
                        }
                    }


                }
            }
            base.set(index, value);
        }
    }

}