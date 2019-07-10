using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Linq;

namespace dolphindb.data
{

    public class BasicShortVector : AbstractVector
    {
        private List<short> values;

        public BasicShortVector(int size) : this(DATA_FORM.DF_VECTOR, size)
        {
        }

        public BasicShortVector(IList<short?> list) : base(DATA_FORM.DF_VECTOR)
        {
            if (list != null)
            {
                values = list.Where(x => x != null).Cast<short>().ToList();
                //values = new short[list.Count];
                //for (int i = 0; i < list.Count; ++i)
                //{
                //    values[i] = list[i].Value;
                //}
            }
        }

        public BasicShortVector(short[] array) : base(DATA_FORM.DF_VECTOR)
        {
            values = new List<short>(array.Length);
            values.AddRange(array);
            //values = array.Clone() as short[];
        }

        protected internal BasicShortVector(DATA_FORM df, int size) : base(df)
        {
            values = new List<short>(size);
            values.AddRange(new short[size]);
        }

        protected internal BasicShortVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
        {
            int rows = @in.readInt();
            int cols = @in.readInt();
            int size = rows * cols;
            values = new List<short>(size);
            values.AddRange(new short[size]);
            for (int i = 0; i < size; ++i)
            {
                values[i] = @in.readShort();
            }
        }

        public virtual short getShort(int index)
        {
            return values[index];
        }

        public override IScalar get(int index)
        {
            return new BasicShort(values[index]);
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() == DATA_TYPE.DT_SHORT)
            {
                setShort(index, ((BasicShort)value).getValue());
            }
                
        }

        public virtual void setShort(int index, short value)
        {
            values[index] = value;
        }

        public override bool isNull(int index)
        {
            return values[index] == short.MinValue;
        }

        public override void setNull(int index)
        {
            values[index] = short.MinValue;
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.INTEGRAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_SHORT;
        }

        public override int rows()
        {
            return values.Count;
        }

        public override Type getElementClass()
        {
            return typeof(BasicShort);
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeShortArray(values.ToArray());
        }

        public override object getList()
        {
            return values;
        }

        public override void set(int index, string value)
        {
            short v;
            if (short.TryParse(value, out v))
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
            values.Add(Convert.ToInt16(value));
        }

        public override void addRange(object list)
        {
            List<short> data = (List<short>)list;
            values.AddRange(data);
        }

    }

}