using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{


    public class BasicDecimal64Matrix : AbstractMatrix
    {
        private BasicDecimal64Vector value;

        public BasicDecimal64Matrix(int rows, int columns, int scale) : base(rows, columns)
        {
            this.value = new BasicDecimal64Vector(rows * columns, scale);
        }

        public BasicDecimal64Matrix(int rows, int columns, IList<decimal[]> list, int scale) : base(rows, columns)
        {
            List<decimal> tmp = new List<decimal>();
            foreach (decimal[] data in list)
            {
                tmp.AddRange(data);
            }
            if (tmp.Count != rows * columns)
            {
                throw new Exception("the total size of list must be equal to rows * columns");
            }
            value = new BasicDecimal64Vector(tmp, scale);
        }

        public BasicDecimal64Matrix(int rows, int columns, IList<string[]> list, int scale) : base(rows, columns)
        {
            List<string> tmp = new List<string>();
            foreach (string[] data in list)
            {
                tmp.AddRange(data);
            }
            if (tmp.Count != rows * columns)
            {
                throw new Exception("the total size of list must be equal to rows * columns");
            }
            value = new BasicDecimal64Vector(tmp, scale);
        }

        public BasicDecimal64Matrix(ExtendedDataInput @in) : base(@in)
        {
        }

        public virtual void setDecimal(int row, int column, decimal value)
        {
            this.value.setDecimal(getIndex(row, column), value);
        }

        public virtual decimal getDecimal(int row, int column)
        {
            return value.getDecimal(getIndex(row, column));
        }
        
        public void setString(int row, int column, string value)
        {
            this.value.set(getIndex(row, column), value);
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
            return DATA_CATEGORY.DENARY;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_DECIMAL64;
        }

        public override Type getElementClass()
        {
            return typeof(BasicDecimal64);
        }

        protected internal override void readMatrixFromInputStream(int rows, int columns, ExtendedDataInput @in)
        {
            int scale = @in.readInt();
            this.value = new BasicDecimal64Vector(rows * columns, scale);
            value.deserialize(0, rows * columns, @in);
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            this.value.writeVectorToOutputStream(@out);
        }

        public int getScale()
        {
            return value.getScale();
        }
    }

}