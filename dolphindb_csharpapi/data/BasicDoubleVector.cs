using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Linq;
namespace dolphindb.data
{
    public class BasicDoubleVector : AbstractVector
    {
        private List<double> values;

        public BasicDoubleVector(int size) : this(DATA_FORM.DF_VECTOR, size)
        {
        }

        public BasicDoubleVector(IList<double?> list) : base(DATA_FORM.DF_VECTOR)
        {
            if (list != null)
            {
                values = list.Where(x => x != null).Cast<double>().ToList();
                //values = new double[list.Count];
                //for (int i = 0; i < list.Count; ++i)
                //{
                //    values[i] = list[i].Value;
                //}
            }
        }

        public BasicDoubleVector(double[] array) : base(DATA_FORM.DF_VECTOR)
        {
            values = new List<double>(array.Length);
            values.AddRange(array);
            //values = array.Clone() as double[];
        }

        protected internal BasicDoubleVector(DATA_FORM df, int size) : base(df)
        {
            values = new List<double>(size);
            values.AddRange(new double[size]);
        }

        protected internal BasicDoubleVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
        {
            int rows = @in.readInt();
            int cols = @in.readInt();
            int size = rows * cols;
            values = new List<double>(size);
            values.AddRange(new double[size]);
            for (int i = 0; i < size; ++i)
            {
                values[i] = @in.readDouble();
            }
        }

        public override IScalar get(int index)
        {
            return new BasicDouble(values[index]);
        }

        public virtual double getDouble(int index)
        {
            return values[index];
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() == DATA_TYPE.DT_DOUBLE)
            {
                setDouble(index, ((BasicDouble)value).getValue());
            }
                
        }

        public virtual void setDouble(int index, double value)
        {
            values[index] = value;
        }

        public override bool isNull(int index)
        {
            return values[index] == -double.MaxValue;
        }

        public override void setNull(int index)
        {
            values[index] = -double.MaxValue;
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.FLOATING;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_DOUBLE;
        }

        public override Type getElementClass()
        {
            return typeof(BasicDouble);
        }

        public override int rows()
        {
            return values.Count;
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeDoubleArray(values.ToArray());
        }

        public override object getList()
        {
            return values;
        }

        public override void set(int index, string value)
        {
            double v;
            if (double.TryParse(value, out v))
            {
                values[index] = v;
            }
            else
            {
                setNull(index);
            }
        }

        public override void add(object value)
        {
            values.Add(Convert.ToDouble(value));
        }

        public override void addRange(object list)
        {
            List<double> data = (List<double>)list;
            values.AddRange(data);
        }
    }
}