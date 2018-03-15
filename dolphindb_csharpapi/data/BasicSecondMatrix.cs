using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
    public class BasicSecondMatrix : BasicIntMatrix
    {
        public BasicSecondMatrix(int rows, int columns) : base(rows, columns)
        {
        }

        public BasicSecondMatrix(int rows, int columns, IList<int[]> listOfArrays) : base(rows, columns, listOfArrays)
        {
        }

        public BasicSecondMatrix(ExtendedDataInput @in) : base(@in)
        {
        }

        public virtual void setSecond(int row, int column, DateTime value)
        {
            setInt(row, column, Utils.countSeconds(value));
        }

        public virtual DateTime getSecond(int row, int column)
        {
            return Utils.parseSecond(getInt(row, column));
        }

        public override IScalar get(int row, int column)
        {
            return new BasicSecond(getInt(row, column));
        }

        public override DATA_CATEGORY getDataCategory()
        {

            return DATA_CATEGORY.TEMPORAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_SECOND;
        }

        public override Type getElementClass()
        {
            return typeof(BasicSecond);
        }
    }

}