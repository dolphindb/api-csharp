using dolphindb.io;
using dolphindb.data;
using System;
using System.Collections.Generic;
using System.Text;

namespace dolphindb.data
{
    public class BasicDecimal32Vector : AbstractVector
    {
        private int scale_;
        private List<int> values_;

        internal BasicDecimal32Vector(DATA_FORM df, int size, int scale) : base(df)
        {
            BasicDecimal32.checkScale(scale);
            scale_ = scale;
            values_ = new List<int>(size);
            values_.AddRange(new int[size]);
        }

        public BasicDecimal32Vector(int size, int scale) : this(DATA_FORM.DF_VECTOR, size, scale)
        {
        }

        public BasicDecimal32Vector(string[] data, int scale) : base(DATA_FORM.DF_VECTOR)
        {
            BasicDecimal32.checkScale(scale);
            scale_ = scale;

            int length = data.Length;
            List<int> newData = new List<int>(length);
            for (int i = 0; i < length; i++)
            {
                newData.Add(BasicDecimal32.parseString(data[i], ref scale_));
            }
            values_ = newData;
        }

        public BasicDecimal32Vector(List<string> list, int scale) : this(list.ToArray(), scale)
        {
        }

        public BasicDecimal32Vector(decimal[] data, int scale) : base(DATA_FORM.DF_VECTOR)
        {
            BasicDecimal32.checkScale(scale);
            scale_ = scale;

            int length = data.Length;
            values_ = new List<int> { };
            for (int i = 0; i < length; i++)
            {
                append(new BasicDecimal32(data[i], scale));
            }
        }

        public BasicDecimal32Vector(List<decimal> list, int scale) : this(list.ToArray(), scale)
        {
        }


        public BasicDecimal32Vector(DATA_FORM df, ExtendedDataInput @input, int extra) : base(df)
        {
            int rows = @input.readInt();
            int cols = @input.readInt();
            int size = rows * cols;
            if (extra != -1)
                scale_ = extra;
            else
                scale_ = input.readInt();
            values_ = new List<int>(size);
            values_.AddRange(new int[size]);
            for (int i = 0; i < size; ++i)
            {
                values_[i] = @input.readInt();
            }
        }

        public override void add(object value)
        {
            if(value is string)
            {
                values_.Add(BasicDecimal32.parseString((string)value, ref scale_));
            }else if(value is decimal)
            {
                values_.Add(new BasicDecimal32((decimal)value, scale_).getRawData());
            }
            else
            {
                throw new Exception("the type of value must be string or decimal.");
            }
        }

        internal void addRange(List<int> list)
        {
            values_.AddRange(list);
        }

        public override void addRange(object list)
        {
            if (list is string[])
            {
                foreach (string s in (string[])list)
                {
                    add(s);
                }
            }
            else if (list is List<string>)
            {
                foreach (string s in (List<string>)list)
                {
                    add(s);
                }
            }
            else if (list is decimal[])
            {
                foreach (decimal s in (decimal[])list)
                {
                    add(s);
                }
            }
            else if (list is List<decimal>)
            {
                foreach (decimal s in (List<decimal>)list)
                {
                    add(s);
                }
            }
            else
            {
                throw new Exception("the type of list must be string[], List<string>, decimal[] or List<decimal>.");
            }
        }

        public override void append(IScalar value)
        {
            if(value.getDataType() == DATA_TYPE.DT_DECIMAL32 && value.getScale() == scale_)
            {
                values_.Add(((BasicDecimal32)value).getRawData());
            }
            else
            {
                add(value.getString());
            }
        }

        public override void append(IVector value)
        {
            BasicDecimal32Vector v = (BasicDecimal32Vector)value;
            if (v.getScale() == scale_)
            {
                addRange(v.getDataArray());
            }
            else
            {
                for (int i = 0; i < value.rows(); ++i)
                {
                    append((IScalar)value.get(i));
                }
            }
        }


        internal List<int> getDataArray()
        {
            return new List<int>(values_);
        }

        public override void deserialize(int start, int count, ExtendedDataInput @in)
        {
            if (start + count > values_.Count)
            {
                values_.AddRange(new int[start + count - values_.Count]);
            }
            for (int i = 0; i < count; ++i)
            {
                values_[start + i] = @in.readInt();
            }
        }

        public override IScalar get(int index)
        {
            if (isNull(index))
            {
                BasicDecimal32 bd32 = new BasicDecimal32("1", scale_);
                bd32.setNull();
                return bd32;
            }
            else
            {
                return new BasicDecimal32(values_[index], scale_);
            }
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.DENARY;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_DECIMAL32;
        }

        public override Type getElementClass()
        {
            return typeof(BasicDecimal32);
        }

        public override IEntity getEntity(int index)
        {
            return new BasicDecimal32(values_[index], scale_);
        }

        public override int getUnitLength()
        {
            return 4;
        }

        public override bool isNull(int index)
        {
            return values_[index] == int.MinValue;
        }

        public override int rows()
        {
            return values_.Count;
        }

        public override void serialize(int start, int count, ExtendedDataOutput @out)
        {
            for (int i = 0; i < count; i++)
            {
                @out.writeInt(values_[i + start]);
            }
        }

        public override int serialize(int indexStart, int offset, int targetNumElement, out int numElement, out int partial, ByteBuffer @out)
        {
            targetNumElement = Math.Min((@out.remain() / getUnitLength()), targetNumElement);
            for (int i = 0; i < targetNumElement; ++i)
            {
                @out.WriteInt(values_[indexStart + i]);
            }
            numElement = targetNumElement;
            partial = 0;
            return targetNumElement * 4;
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() != DATA_TYPE.DT_DECIMAL32)
            {
                throw new Exception("the type of value must be BasicDecimal32. ");
            }
            if (value.isNull())
            {
                values_[index] = int.MinValue;
            }
            else
            {
                int originScale = value.getScale();
                if(originScale == scale_)
                {
                    values_[index] = ((BasicDecimal32)value).getRawData();
                }
                else if (originScale > scale_)   
                {
                    values_[index] = new BasicDecimal32(value.getString(), scale_).getRawData();
                }
                else
                {
                    values_[index] = ((BasicDecimal32)value).getRawData() * BasicDecimal32.pow10(scale_ - originScale);
                }
            }
        }

        public override int getScale()
        {
            return scale_;
        }


        public override void set(int index, string value)
        {
            values_[index] = BasicDecimal32.parseString(value, ref scale_);
        }

        public override void setNull(int index)
        {
            values_[index] = int.MinValue;
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeInt(scale_);
            @out.writeIntArray(values_.ToArray());
        }

        public override int getExtraParamForType()
        {
            return scale_;
        }

        public override IVector getSubVector(int[] indices)
        {
            BasicDecimal32Vector ret = new BasicDecimal32Vector(indices.Length, scale_);
            for (int i = 0; i < indices.Length; ++i)
            {
                int index = indices[i];
                ret.set(i, get(index));
            }
            return ret;
        }

        public decimal getDecimal(int index)
        {
            return Utils.createBasicDecimal32(values_[index], scale_).getDecimalValue();
        }

        public void setDecimal(int index, decimal value)
        {
            set(index, new BasicDecimal32(value, scale_));
        }

        public override int hashBucket(int index, int buckets)
        {
            throw new NotImplementedException();
        }

        public override int asof(IScalar value)
        {
            throw new NotImplementedException();
        }
    }
}
