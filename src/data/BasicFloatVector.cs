using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Linq;

namespace dolphindb.data
{

    public class BasicFloatVector : AbstractVector
    {
        private List<float> values;

        public BasicFloatVector(int size) : this(DATA_FORM.DF_VECTOR, size)
        {
        }

        public BasicFloatVector(IList<float?> list) : base(DATA_FORM.DF_VECTOR)
        {
            if (list != null)
            {
                values = list.Where(x => x != null).Cast<float>().ToList();
                //values = new float[list.Count];
                //for (int i = 0; i < list.Count; ++i)
                //{
                //    values[i] = list[i].Value;
                //}
            }
        }

        public BasicFloatVector(float[] array) : base(DATA_FORM.DF_VECTOR)
        {
            values = new List<float>(array.Length);
            values.AddRange(array);
            //values = array.Clone() as float[];
        }

        protected internal BasicFloatVector(DATA_FORM df, int size) : base(df)
        {
            values = new List<float>(size);
            values.AddRange(new float[size]);
        }

        protected internal BasicFloatVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
        {
            int rows = @in.readInt();
            int cols = @in.readInt();
            int size = rows * cols;
            values = new List<float>(size);
            values.AddRange(new float[size]);
            for (int i = 0; i < size; ++i)
            {
                values[i] = @in.readFloat();
            }
        }

        public override IScalar get(int index)
        {
            return new BasicFloat(values[index]);
        }

        public virtual float getFloat(int index)
        {
            return values[index];
        }


        public override void set(int index, IScalar value)
        {
            if (value.getDataType() == DATA_TYPE.DT_FLOAT)
            {
                setFloat(index, ((BasicFloat)value).getValue());
            }
            else
                throw new Exception("The value must be a float scalar. ");
        }

        public virtual void setFloat(int index, float value)
        {
            values[index] = value;
        }

        public override bool isNull(int index)
        {
            return values[index] == -float.MaxValue;
        }

        public override void setNull(int index)
        {
            values[index] = -float.MaxValue;
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.FLOATING;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_FLOAT;
        }

        public override int rows()
        {
            return values.Count;
        }

        public override Type getElementClass()
        {
            return typeof(BasicFloat);
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeFloatArray(values.ToArray());
        }

        public override object getList()
        {
            return values;
        }

        public override void set(int index, string value)
        {
            float v;
            if (float.TryParse(value, out v))
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
            values.Add((float)value);
        }

        public override void addRange(object list)
        {
            List<float> data = (List<float>)list;
            values.AddRange(data);
        }

        public override IVector getSubVector(int[] indices)
        {
            int length = indices.Length;
            float[] sub = new float[length];
            for (int i = 0; i < length; ++i)
                sub[i] = values[indices[i]];
            return new BasicFloatVector(sub);
        }

        public override int asof(IScalar value)
        {
            float target;
            target = value.getNumber().floatValue();

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
                values.AddRange(new float[start + count - values.Count]);
            }
            for (int i = 0; i < count; ++i)
            {
                values[start + i] = @in.readFloat();
            }
        }

        public override void serialize(int start, int count, ExtendedDataOutput @out)
        {
            for (int i = 0; i < count; ++i)
            {
                @out.writeFloat(values[start + i]);
            }
        }

        public override int getUnitLength()
        {
            return 4;
        }

        public override int serialize(int indexStart, int offect, int targetNumElement, out int numElement, out int partial, ByteBuffer @out)
        {
            targetNumElement = Math.Min((@out.remain() / getUnitLength()), targetNumElement);
            for (int i = 0; i < targetNumElement; ++i)
            {
                @out.WriteFloat(values[indexStart + i]);
            }
            numElement = targetNumElement;
            partial = 0;
            return targetNumElement * 4;
        }

        public override void append(IScalar value)
        {
            values.Add(((BasicFloat)value).getValue());
        }

        public override void append(IVector value)
        {
            values.AddRange(((BasicFloatVector)value).getdataArray());
        }

        public List<float> getdataArray()
        {
            return values;
        }

        public override IEntity getEntity(int index)
        {
            return new BasicFloat(values[index]);
        }
    }

}