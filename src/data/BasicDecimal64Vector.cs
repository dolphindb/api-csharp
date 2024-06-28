using dolphindb.data;
using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Text;

namespace dolphindb.data
{
    public class BasicDecimal64Vector : AbstractVector
    {
        private int scale_;
        private List<long> values_;

        internal BasicDecimal64Vector(DATA_FORM df, int size, int scale) : base(df)
        {
            BasicDecimal64.checkScale(scale);
            scale_ = scale;
            values_ = new List<long>(size);
            values_.AddRange(new long[size]);
        }

        public BasicDecimal64Vector(int size, int scale) : this(DATA_FORM.DF_VECTOR, size, scale)
        {
        }

        public BasicDecimal64Vector(string[] data, int scale) : base(DATA_FORM.DF_VECTOR)
        {
            BasicDecimal64.checkScale(scale);
            scale_ = scale;

            int length = data.Length;
            List<long> newData = new List<long>(length);
            for (int i = 0; i < length; i++)
            {
                newData.Add(BasicDecimal64.parseString(data[i], ref scale_));
            }
            values_ = newData;
        }

        public BasicDecimal64Vector(List<string> list, int scale) : this(list.ToArray(), scale) { }

        public BasicDecimal64Vector(decimal[] data, int scale) : base(DATA_FORM.DF_VECTOR)
        {
            BasicDecimal64.checkScale(scale);
            scale_ = scale;

            int length = data.Length;
            values_ = new List<long>();
            for (int i = 0; i < length; i++)
            {
                append(new BasicDecimal64(data[i], scale));
            }
        }

        public BasicDecimal64Vector(List<decimal> list, int scale) : this(list.ToArray(), scale)
        {
        }

        public BasicDecimal64Vector(DATA_FORM df, ExtendedDataInput @input, int extra) : base(df)
        {
            int rows = @input.readInt();
            int cols = @input.readInt();
            int size = rows * cols;
            if (extra != -1)
                scale_ = extra;
            else
                scale_ = input.readInt();
            values_ = new List<long>(size);
            values_.AddRange(new long[size]);
            for (int i = 0; i < size; ++i)
            {
                values_[i] = @input.readLong();
            }
        }

        public override void add(object value)
        {
            if (value is string)
            {
                values_.Add(BasicDecimal64.parseString((string)value, ref scale_));
            }
            else if (value is decimal)
            {
                values_.Add(new BasicDecimal64((decimal)value, scale_).getRawData());
            }
            else
            {
                throw new Exception("the type of value must be string or decimal.");
            }
        }

        internal void addRange(List<long> list)
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
            if(value.getDataType() == DATA_TYPE.DT_DECIMAL64 && value.getScale() == scale_)
            {
                values_.Add(((BasicDecimal64)value).getRawData());
            }else{
                add(value.getString());
            }
        }

        public override void append(IVector value)
        {
            BasicDecimal64Vector v = (BasicDecimal64Vector)value;
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


        internal List<long> getDataArray()
        {
            return new List<long>(values_);
        }

        public override void deserialize(int start, int count, ExtendedDataInput @in)
        {
            for(int i = 0; i < count; ++i)
            {
                values_[start + i] = @in.readLong(); 
            }
        }

        public override IScalar get(int index)
        {
            if (isNull(index))
            {
                BasicDecimal64 bd32 = new BasicDecimal64("1", scale_);
                bd32.setNull();
                return bd32;
            }
            else
            {
                return new BasicDecimal64(values_[index], scale_);
            }
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

        public override IEntity getEntity(int index)
        {
            return new BasicDecimal64(values_[index], scale_);
        }

        public override int getUnitLength()
        {
            return 8;
        }

        public override bool isNull(int index)
        {
            return values_[index] == long.MinValue;
        }

        public override int rows()
        {
            return values_.Count;
        }

        public override void serialize(int start, int count, ExtendedDataOutput @out)
        {
            for(int i = 0; i < count; i++)
            {
                @out.writeLong(values_[i + start]);
            }
        }

        public override int serialize(int indexStart, int offect, int targetNumElement, out int numElement, out int partial, ByteBuffer @out)
        {
            targetNumElement = Math.Min((@out.remain() / getUnitLength()), targetNumElement);
            for (int i = 0; i < targetNumElement; ++i)
            {
                @out.WriteLong(values_[indexStart + i]);
            }
            numElement = targetNumElement;
            partial = 0;
            return targetNumElement * 4;
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() != DATA_TYPE.DT_DECIMAL64)
            {
                throw new Exception("value type must be BasicDecimal64. ");
            }
            if (value.isNull())
            {
                values_[index] = long.MinValue;
            }
            else
            {
                int originScale = value.getScale();
                if (originScale == scale_)
                {
                    values_[index] = ((BasicDecimal64)value).getRawData();
                }
                else if (originScale > scale_)
                {
                    values_[index] = new BasicDecimal64(value.getString(), scale_).getRawData();
                }
                else
                {
                    values_[index] = ((BasicDecimal64)value).getRawData() * BasicDecimal64.pow10(scale_ - originScale);
                }
            }
        }

        public override int getScale()
        {
            return scale_;
        }


        public override void set(int index, string value)
        {
            values_[index] = BasicDecimal64.parseString(value, ref scale_);
        }

        public override void setNull(int index)
        {
            values_[index] = long.MinValue;
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeInt(scale_);
            @out.writeLongArray(values_.ToArray());
        }

        public override int getExtraParamForType()
        {
            return scale_;
        }

        public override IVector getSubVector(int[] indices)
        {
            BasicDecimal64Vector ret = new BasicDecimal64Vector(indices.Length, scale_);
            for (int i = 0; i < indices.Length; ++i)
            {
                int index = indices[i];
                ret.set(i, get(index));
            }
            return ret;
        }
        public decimal getDecimal(int index)
        {
            return Utils.createBasicDecimal64(values_[index], scale_).getDecimalValue();
        }

        public void setDecimal(int index, decimal value)
        {
            set(index, new BasicDecimal64(value, scale_));
        }
    }
}
