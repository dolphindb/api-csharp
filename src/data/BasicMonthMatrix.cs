using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
    public class BasicMonthMatrix : BasicIntMatrix
    {
        public BasicMonthMatrix(int rows, int columns) : base(rows, columns)
        {
        }

        public BasicMonthMatrix(int rows, int columns, IList<int[]> listOfArrays) : base(rows, columns, listOfArrays)
        {
        }

        public BasicMonthMatrix(ExtendedDataInput @in) : base(@in)
        {
        }

        public virtual void setMonth(int row, int column, DateTime value)
        {
            setInt(row, column, Utils.countMonths(value));
        }

        public virtual DateTime getMonth(int row, int column)
        {
            return Utils.parseMonth(getInt(row, column));
        }


        public override IScalar get(int row, int column)
        {
            return new BasicMonth(getInt(row, column));
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_MONTH;
        }

        public override Type getElementClass()
        {
            return typeof(BasicMonth);
        }

    }

}