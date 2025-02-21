using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

    public class BasicComplexMatrix : AbstractMatrix
    {
        private BasicComplexVector value;

        public BasicComplexMatrix(int rows, int columns) : base(rows, columns)
        {
            this.value = new BasicComplexVector(rows * columns);
        }

        public BasicComplexMatrix(int rows, int columns, IList<Double2[]> list) : base(rows, columns)
        {
            List<Double2> tmp = new List<Double2>();
            foreach (Double2[] data in list)
            {
                tmp.AddRange(data);
            }
            if (tmp.Count != rows * columns)
            {
                throw new Exception("the total size of list must be equal to rows * columns");
            }
            value = new BasicComplexVector(tmp);
        }

        public BasicComplexMatrix(ExtendedDataInput @in) : base(@in)
        {
        }

        public override bool isNull(int row, int column)
        {
            return value.isNull(getIndex(row, column));
        }

        public override void setNull(int row, int column)
        {
            value.setNull(getIndex(row, column));
        }

        public override IScalar get(int row, int column)
        {
            return this.value.get(getIndex(row, column));
        }

        public void set(int row, int column, IScalar value)
        {
            this.value.set(getIndex(row, column), value);
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.BINARY;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_COMPLEX;
        }

        public override Type getElementClass()
        {
            return typeof(BasicComplex);
        }

        protected internal override void readMatrixFromInputStream(int rows, int columns, ExtendedDataInput @in)
        {
            this.value = new BasicComplexVector(rows * columns);
            value.deserialize(0, rows * columns, @in);
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            this.value.writeVectorToOutputStream(@out);
        }
    }

}