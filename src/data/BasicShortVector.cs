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
            else
                throw new Exception("The value must be a short scalar. ");
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

        public override  int hashBucket(int index, int buckets)
        {
            short value = values[index];
            if (value >= 0)
                return value % buckets;
            else if (value == short.MinValue)
                return -1;
            else
            {
                return (int)((4294967296l + value) % buckets);
            }
        }

        public override IVector getSubVector(int[] indices)
        {
            int length = indices.Length;
            short[] sub = new short[length];
            for (int i = 0; i < length; ++i)
                sub[i] = values[indices[i]];
            return new BasicShortVector(sub);
        }

        public override int asof(IScalar value)
        {
            short target;
            try
            {
                target = value.getNumber().shortValue();
            }
            catch (Exception ex)
            {
                throw ex;
            }

            int start = 0;
            int end = values.Count - 1;
            int mid;
            while (start <= end)
            {
                mid = (start + end) / 2;
                if (values[mid] <= target)
                    start = mid + 1;
                else
                    end = mid - 1;
            }
            return end;
        }

        protected override void writeVectorToBuffer(ByteBuffer buffer)
        {
            foreach (short val in values)
            {
                buffer.WriteShort(val);
            }
        }

        public override void deserialize(int start, int count, ExtendedDataInput @in)
        {
            if (start + count > values.Count)
            {
                values.AddRange(new short[start + count - values.Count]);
            }
            for (int i = 0; i < count; ++i)
            {
                values[start + i] = @in.readShort();
            }
        }

        public override void serialize(int start, int count, ExtendedDataOutput @out)
        {
            for(int i = 0; i < count; ++i)
            {
                @out.writeShort(values[start + i]);
            }
        }

        public override int getUnitLength()
        {
            return 2;
        }

        public override int serialize(int indexStart, int offect, int targetNumElement, out int numElement, out int partial, ByteBuffer @out)
        {
            targetNumElement = Math.Min((@out.remain() / getUnitLength()), targetNumElement);
            for (int i = 0; i < targetNumElement; ++i)
            {
                @out.WriteShort(values[indexStart + i]);
            }
            numElement = targetNumElement;
            partial = 0;
            return targetNumElement * 2;
        }

        public override void append(IScalar value)
        {
            values.Add(((BasicShort)value).getValue());
        }

        public override void append(IVector value)
        {
            values.AddRange(((BasicShortVector)value).getdataArray());
        }

        public List<short> getdataArray()
        {
            return values;
        }

        public override IEntity getEntity(int index)
        {
            return new BasicShort(values[index]);
        }
    }

}