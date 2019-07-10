using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Linq;

namespace dolphindb.data
{
    public class BasicIntVector : AbstractVector
    {
        private List<int> values;

        public BasicIntVector(int size) : this(DATA_FORM.DF_VECTOR, size)
        {
        }

        public BasicIntVector(IList<int?> list) : base(DATA_FORM.DF_VECTOR)
        {
            if (list != null)
            {
                
                values = list.Where(x => x != null).Cast<int>().ToList();
                //values = new int[list.Count];
                //for (int i = 0; i < list.Count; ++i)
                //{
                //    values[i] = list[i].Value;
                //}
            }
        }

        public BasicIntVector(int[] array) : base(DATA_FORM.DF_VECTOR)
        {
            values = new List<int>(array.Length);
            values.AddRange(array);
        }

        protected internal BasicIntVector(DATA_FORM df, int size) : base(df)
        {
            values = new List<int>(size);
            values.AddRange(new int[size]);
        }

        protected internal BasicIntVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
        {
            int rows = @in.readInt();
            //if (rows != 1024)
            //assert(rows == 1024);
            int cols = @in.readInt();
            int size = rows * cols;
            values = new List<int>(size);
            values.AddRange(new int[size]);
            for (int i = 0; i < size; ++i)
            {
                values[i] = @in.readInt();
            }
        }

        public override IScalar get(int index)
        {
            return new BasicInt(values[index]);
        }

        public virtual int getInt(int index)
        {
            return values[index];
        }

        public override void set(int index, IScalar value)
        {
            values[index] = Convert.ToInt32(value.getString());
        }

        public virtual void setInt(int index, int value)
        {
            values[index] = value;
        }

        public override bool isNull(int index)
        {
            return values[index] == int.MinValue;
        }

        public override void setNull(int index)
        {
            values[index] = int.MinValue;
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.INTEGRAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_INT;
        }

        public override Type getElementClass()
        {
            return typeof(BasicInt);
        }

        public override int rows()
        {
            return values.Count;
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeIntArray(values.ToArray());
        }

        public override object getList()
        {
            return values;
        }

        public override void set(int index, string value)
        {
            int v;
            if (int.TryParse(value, out v))
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
            values.Add(Convert.ToInt32(value));
        }

        public override void addRange(object list)
        {
            List<int> data = (List<int>)list;
            values.AddRange(data);
        }
    }

}