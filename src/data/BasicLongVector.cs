using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Linq;

namespace dolphindb.data
{


    public class BasicLongVector : AbstractVector
    {
        protected List<long> values;

        public BasicLongVector(int size) : this(DATA_FORM.DF_VECTOR, size)
        {
        }

        public BasicLongVector(IList<long?> list) : base(DATA_FORM.DF_VECTOR)
        {
            if (list != null)
            {
                values = list.Where(x => x != null).Cast<long>().ToList();
                //values = new long[list.Count];
                //for (int i = 0; i < list.Count; ++i)
                //{
                //    values[i] = list[i].Value;
                //}
            }
        }

        public BasicLongVector(long[] array) : base(DATA_FORM.DF_VECTOR)
        {
            values = new List<long>(array.Length);
            values.AddRange(array);
            //values = array.Clone() as long[];
        }

        protected internal BasicLongVector(DATA_FORM df, int size) : base(df)
        {
            values = new List<long>(size);
            values.AddRange(new long[size]);
        }

        protected internal BasicLongVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
        {
            int rows = @in.readInt();
            int cols = @in.readInt();
            int size = rows * cols;
            values = new List<long>(size);
            values.AddRange(new long[size]);
            for (int i = 0; i < size; ++i)
            {
                values[i] = @in.readLong();
            }
        }

        public override IScalar get(int index)
        {
            return new BasicLong(values[index]);
        }

        public virtual long getLong(int index)
        {
            return values[index];
        }

        public override void set(int index, IScalar value)
        {
            if (value.getString() == null || value.getString() == "")
                setNull(index);
            else
                values[index] = Convert.ToInt64(value.getString());
        }

        public virtual void setLong(int index, long value)
        {
            values[index] = value;
        }

        public override bool isNull(int index)
        {
            return values[index] == long.MinValue;
        }

        public override void setNull(int index)
        {
            values[index] = long.MinValue;
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.INTEGRAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_LONG;
        }

        public override int rows()
        {
            return values.Count;
        }

        public override Type getElementClass()
        {
            return typeof(BasicLong);
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeLongArray(values.ToArray());
        }

        public override object getList()
        {
            return values;
        }

        public override void set(int index, string value)
        {
            long v;
            if (long.TryParse(value, out v))
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
            values.Add(Convert.ToInt64(value));
        }

        public override void addRange(object list)
        {
            List<long> data = (List<long>)list;
            values.AddRange(data);
        }

        public override int hashBucket(int index, int buckets)
        {
            long value = values[index];
            if (value >= 0)
                return (int)(value % buckets);
            else if (value == long.MinValue)
                return -1;
            else
            {
                return (int)(((long.MaxValue % buckets) + 2 + ((long.MaxValue + value) % buckets)) % buckets);
            }
        }

        public override IVector getSubVector(int[] indices)
        {
            int length = indices.Length;
            long[] sub = new long[length];
            for (int i = 0; i < length; ++i)
                sub[i] = values[indices[i]];
            return new BasicLongVector(sub);
        }

        protected long[] getSubArray(int[] indices)
        {
            int length = indices.Length;
            long[] sub = new long[length];
            for (int i = 0; i < length; ++i)
                sub[i] = values[indices[i]];
            return sub;
        }

        public override int asof(IScalar value)
        {
            long target;
            try
            {
                target = value.getNumber().longValue();
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

        public override void deserialize(int start, int count, ExtendedDataInput @in)
        {
            if (start + count > values.Count)
            {
                values.AddRange(new long[start + count - values.Count]);
            }
            for (int i = 0; i < count; ++i)
            {
                values[start + i] = @in.readLong();
            }
        }

        public override void serialize(int start, int count, ExtendedDataOutput @out)
        {
            for (int i = 0; i < count; ++i)
            {
                @out.writeLong(values[start + i]);
            }
        }

        public override int getUnitLength()
        {
            return 8;
        }

        public override int serialize(int indexStart, int offect, int targetNumElement, out int numElement, out int partial, ByteBuffer @out)
        {
            targetNumElement = Math.Min((@out.remain() / getUnitLength()), targetNumElement);
            for (int i = 0; i < targetNumElement; ++i)
            {
                @out.WriteLong(values[indexStart + i]);
            }
            numElement = targetNumElement;
            partial = 0;
            return targetNumElement * 8;
        }

        public override void append(IScalar value)
        {
            values.Add(((BasicLong)value).getValue());
        }

        public override void append(IVector value)
        {
            values.AddRange(((BasicLongVector)value).getdataArray());
        }

        public List<long> getdataArray()
        {
            return values;
        }

        public override IEntity getEntity(int index)
        {
            return new BasicLong(values[index]);
        }

        public override int getExtraParamForType()
        {
            throw new NotImplementedException();
        }
    }

}