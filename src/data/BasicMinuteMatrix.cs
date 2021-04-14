﻿using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{


    public class BasicMinuteMatrix : BasicIntMatrix
    {
        public BasicMinuteMatrix(int rows, int columns) : base(rows, columns)
        {
        }

        public BasicMinuteMatrix(int rows, int columns, IList<int[]> listOfArrays) : base(rows, columns, listOfArrays)
        {
        }

        public BasicMinuteMatrix(ExtendedDataInput @in) : base(@in)
        {
        }

        public virtual void setMinute(int row, int column, TimeSpan value)
        {
            setInt(row, column, Utils.countMinutes(value));
        }

        public virtual TimeSpan getMinute(int row, int column)
        {
            return Utils.parseMinute(getInt(row, column));
        }


        public override IScalar get(int row, int column)
        {
            return new BasicMinute(getInt(row, column));
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_MINUTE;
        }

        public override Type getElementClass()
        {
            return typeof(BasicMinute);
        }

    }

}