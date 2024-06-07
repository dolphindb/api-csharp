using dolphindb.data;
using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Numerics;
using System.Text;

namespace dolphindb.data
{
    public class BasicDecimal128Vector : AbstractVector
    {
        private int scale_;
        private List<byte> values_; // TODO: big、littleEndian

        internal BasicDecimal128Vector(DATA_FORM df, int size, int scale) : base(df)
        {
            BasicDecimal128.checkScale(scale);
            scale_ = scale;
            values_ = new List<byte>(size * 16);
            values_.AddRange(new byte[size * 16]);
        }

        public BasicDecimal128Vector(int size, int scale) : this(DATA_FORM.DF_VECTOR, size, scale)
        {
        }

        public BasicDecimal128Vector(string[] data, int scale) : base(DATA_FORM.DF_VECTOR)
        {
            BasicDecimal128.checkScale(scale);
            scale_ = scale;

            int length = data.Length;
            List<byte> newData = new List<byte>(length * 16);
            for (int i = 0; i < length; i++)
            {
                newData.AddRange(BasicDecimal128.getBigIntegerBytes(BasicDecimal128.parseString(data[i], ref scale_)));
            }
            values_ = newData;
        }

        public BasicDecimal128Vector(List<string> list, int scale) : this(list.ToArray(), scale) { }

        public BasicDecimal128Vector(decimal[] data, int scale) : base(DATA_FORM.DF_VECTOR)
        {
            BasicDecimal128.checkScale(scale);
            scale_ = scale;

            int length = data.Length;
            values_ = new List<byte>();
            for (int i = 0; i < length; i++)
            {
                append(new BasicDecimal128(data[i], scale));
            }
        }

        public BasicDecimal128Vector(List<decimal> list, int scale) : this(list.ToArray(), scale) { }

        public BasicDecimal128Vector(DATA_FORM df, ExtendedDataInput @input, int extra) : base(df)
        {
            int rows = @input.readInt();
            int cols = @input.readInt();
            int size = rows * cols;
            if (extra != -1)
                scale_ = extra;
            else
                scale_ = input.readInt();
            values_ = new List<byte>(size);
            byte[] tmp = new byte[size * 16];
            for (int i = 0; i < size * 16; ++i)
            {
                tmp[i] = @input.readByte();
            }
            BasicDecimal128.reserveBytes(tmp, !input.isLittleEndian());
            values_.AddRange(tmp);
        }

        public override void add(object value)
        {
            if(value is string)
            {
                values_.AddRange(BasicDecimal128.getBigIntegerBytes(BasicDecimal128.parseString((string)value, ref scale_)));
            }else if(value is decimal)
            {
                values_.AddRange(BasicDecimal128.getBigIntegerBytes(new BasicDecimal128((decimal)value, scale_).getRawData()));
            }
            else
            {
                throw new Exception("the type of value must be string or decimal.");
            }
        }

        internal void addRange(List<byte> list)
        {
            values_.AddRange(list);
        }

        public override void addRange(object list)
        {
            if (list is string[])
            {
                foreach(string s in (string[])list)
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
            }else if(list is decimal[])
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
            if(value.getDataType() == DATA_TYPE.DT_DECIMAL128 && value.getScale() == scale_)
            {
                values_.AddRange(BasicDecimal128.getBigIntegerBytes(((BasicDecimal128)value).getRawData()));
            }else{
                add(value.getString());
            }
        }

        public override void append(IVector value)
        {
            BasicDecimal128Vector v = (BasicDecimal128Vector)value;
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


        internal List<byte> getDataArray()
        {
            return new List<byte>(values_);
        }

        public override void deserialize(int start, int count, ExtendedDataInput @in)
        {
            for (int i = 0; i < count; ++i)
            {
                byte[] bytes = new byte[16];
                @in.readFully(bytes);
                BasicDecimal128.reserveBytes(bytes, !@in.isLittleEndian());
                for(int j = 0; j < 16; ++j)
                {
                    values_[(i + start) * 16 + j] = bytes[j];
                }
            }
        }

        public override IScalar get(int index)
        {
            if (isNull(index))
            {
                BasicDecimal128 bd32 = new BasicDecimal128("1", scale_);
                bd32.setNull();
                return bd32;
            }
            else
            {
                byte[] bytes = new byte[16];
                for(int i = 0; i < 16; ++i)
                {
                    bytes[i] = values_[index* 16 + i];
                }
                return new BasicDecimal128(BasicDecimal128.createBigIntegerByBytes(bytes), scale_);
            }
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.DENARY;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_DECIMAL128;
        }

        public override Type getElementClass()
        {
            return typeof(BasicDecimal128);
        }

        public override IEntity getEntity(int index)
        {
            return get(index);
        }

        public override int getUnitLength()
        {
            return 16;
        }

        public override bool isNull(int index)
        {
            byte[] bytes = new byte[16];
            for(int i = 0; i < 16; ++i)
            {
                bytes[i] = values_[index * 16 + i];
            }
            return BasicDecimal128.createBigIntegerByBytes(bytes) == BasicDecimal128.NULL;
        }

        public override int rows()
        {
            return values_.Count / 16;
        }

        public override void serialize(int start, int count, ExtendedDataOutput @out)
        {
            byte[] bytes = new byte[count * 16];  
            for(int i = 0; i < count; ++i)
            {
                for(int j = 0; j < 16; ++j)
                {
                    bytes[i * 16 + j] = values_[(i + start) * 16 + j];
                }
            }
            BasicDecimal128.reserveBytes(bytes, !@out.isLittleEndian());
            @out.write(bytes);
        }

        public override int serialize(int indexStart, int offect, int targetNumElement, out int numElement, out int partial, ByteBuffer @out)
        {
            targetNumElement = Math.Min((@out.remain() / getUnitLength()), targetNumElement);
            byte[] bytes = new byte[targetNumElement * 16];
            for(int i = 0; i < targetNumElement * 16; ++i)
            {
                bytes[i] = values_[indexStart * 16 + i];
            }
            BasicDecimal128.reserveBytes(bytes, !@out.isLittleEndian);
            @out.WriteBytes(bytes);
            numElement = targetNumElement;
            partial = 0;
            return targetNumElement * 16;
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() != DATA_TYPE.DT_DECIMAL128)
            {
                throw new Exception("value type must be BasicDecimal128. ");
            }

            List<byte> bytes;
            if (value.isNull())
            {
                bytes = BasicDecimal128.getBigIntegerBytes(BasicDecimal128.NULL);
            }
            else
            {
                int originScale = value.getScale();
                BigInteger convertData;
                if (originScale > scale_)
                {
                    convertData = ((BasicDecimal128)value).getRawData() / BasicDecimal128.pow10(originScale - scale_);
                }
                else
                {
                    convertData = ((BasicDecimal128)value).getRawData() * BasicDecimal128.pow10(scale_ - originScale);
                }
                bytes = BasicDecimal128.getBigIntegerBytes(convertData);
            }
            for (int i = 0; i < bytes.Count; ++i)
            {
                values_[index * 16 + i] = bytes[i];
            }
        }

        public override int getScale()
        {
            return scale_;
        }


        public override void set(int index, string value)
        {
            set(index, new BasicDecimal128(value, scale_));
        }

        public override void setNull(int index)
        {
            set(index, new BasicDecimal128(BasicDecimal128.NULL, scale_));
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeInt(scale_);
            @out.write(BasicDecimal128.reserveBytes(values_.ToArray(), !@out.isLittleEndian()));
        }

        public override int getExtraParamForType()
        {
            return scale_;
        }

        public override IVector getSubVector(int[] indices)
        {
            BasicDecimal128Vector ret = new BasicDecimal128Vector(indices.Length, scale_);
            for (int i = 0; i < indices.Length; ++i)
            {
                int index = indices[i];
                ret.set(i, get(index));
            }
            return ret;
        }
        public decimal getDecimal(int index)
        {
            return ((BasicDecimal128)get(index)).getDecimalValue();
        }
    }
}
