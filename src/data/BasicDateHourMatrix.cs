using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

    public class BasicDateHourMatrix : BasicIntMatrix
    {
        public BasicDateHourMatrix(int rows, int columns) : base(rows, columns)
        {
        }

        public BasicDateHourMatrix(int rows, int columns, IList<int[]> listOfArrays) : base(rows, columns, listOfArrays)
        {
        }

        public BasicDateHourMatrix(ExtendedDataInput @in) : base(@in)
        {
        }

        public virtual void setDateTime(int row, int column, DateTime value)
        {
            setInt(row, column, Utils.countHours(value));
        }

        public virtual DateTime getDateTime(int row, int column)
        {
            return Utils.parseDateHour(getInt(row, column));
        }

        public override IScalar get(int row, int column)
        {
            return new BasicDateHour(getInt(row, column));
        }

        public override Type getElementClass()
        {
            return typeof(BasicDateHour);
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_DATEHOUR;
        }
    }

}