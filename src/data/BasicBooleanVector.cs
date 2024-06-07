using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Linq;

namespace dolphindb.data
{


    public class BasicBooleanVector : AbstractVector
    {
        private List<byte> values;

        public BasicBooleanVector(int size) : this(DATA_FORM.DF_VECTOR, size)
        {
        }

        public BasicBooleanVector(IList<byte?> list) : base(DATA_FORM.DF_VECTOR)
        {
            if (list != null)
            {
                values = list.Where(x => x != null).Cast<byte>().ToList();
                //values = new byte[list.Count];
                //for (int i = 0; i < list.Count; ++i)
                //{
                //    values[i] = list[i].Value;
                //}
            }
        }

        public BasicBooleanVector(byte[] array) : base(DATA_FORM.DF_VECTOR)
        {
            values = new List<byte>(array.Length);
            values.AddRange(array);
            //array.Clone() as byte[];
        }

        protected internal BasicBooleanVector(DATA_FORM df, int size) : base(df)
        {
           
            values = new List<byte>(size);
            values.AddRange(new byte[size]);
        }

        protected internal BasicBooleanVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
        {
            int rows = @in.readInt();
            int cols = @in.readInt();
            int size = rows * cols;
            values = new List<byte>(size);
            values.AddRange(new byte[size]);
            for (int i = 0; i < size; ++i)
            {
                values[i] = @in.readByte();
            }
        }

        public override IScalar get(int index)
        {
            return new BasicBoolean(values[index]);
        }

        public virtual bool getBoolean(int index)
        {
            return values[index] != 0;
        }

        public override void set(int index, IScalar value)
        {
            if(value.getNumber()==null)
                values[index] = (byte)0;
            else
            {
                values[index] = value.getNumber().byteValue();
            }
        }

        public virtual void setBoolean(int index, bool value)
        {
            values[index] = value ? (byte)1 : (byte)0;
        }

        public override bool isNull(int index)
        {
            return values[index] == 128;
        }

        public override void setNull(int index)
        {
            values[index] = 128;
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.LOGICAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_BOOL;
        }

        public override Type getElementClass()
        {
            return typeof(BasicBoolean);
        }

        public override int rows()
        {
            return values.Count;
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            @out.write(values.ToArray());
        }

        public override void set(int index, string value)
        {
            bool v1;
            long v2;
            if (bool.TryParse(value, out v1))
            {
                values[index] = v1 ? (byte)1 : (byte)0;
            }else if(long.TryParse(value, out v2))
            {
                values[index] = v2 > 0 ? (byte)1 : (byte)0;
            }
            else
            {
                values[index] = (byte)0;
            }
        }

        public override void add(object value)
        {
            values.Add(Convert.ToByte(value));
        }

        public override void addRange(object list)
        {
            List<byte> data = (List<byte>)list;
            values.AddRange(data);
        }

        public override IVector getSubVector(int[] indices)
        {
            int length = indices.Length;
            byte[] sub = new byte[length];
            for (int i = 0; i < length; ++i)
                sub[i] = values[indices[i]];
            return new BasicBooleanVector(sub);
        }

        public override int asof(IScalar value)
        {
            throw new Exception("BasicBooleanVector.asof not supported.");
        }

        public override void deserialize(int start, int count, ExtendedDataInput @in)
        {
            if(start + count > values.Count)
            {
                values.AddRange(new byte[start + count - values.Count]);
            }
            for (int i = 0; i < count; ++i)
            {
                values[start + i] = @in.readByte();
            }
        }

        public override void serialize(int start, int count, ExtendedDataOutput @out)
        {
            for (int i = 0; i < count; ++i)
            {
                @out.writeByte(values[start + i]);
            }
        }

        public override int getUnitLength()
        {
            return 1;
        }

        public override int serialize(int indexStart, int offect, int targetNumElement, out int numElement, out int partial, ByteBuffer @out)
        {
            targetNumElement = Math.Min((@out.remain() / getUnitLength()), targetNumElement);
            for (int i = 0; i < targetNumElement; ++i)
            {
                @out.WriteByte(values[indexStart + i]);
            }
            numElement = targetNumElement;
            partial = 0;
            return targetNumElement;
        }

        public override void append(IScalar value)
        {
            values.Add(((BasicBoolean)value).getByte());
        }

        public override void append(IVector value)
        {
            values.AddRange(((BasicBooleanVector)value).getdataArray());
        }


        public List<byte> getdataArray()
        {
            return values;
        }

        public override IEntity getEntity(int index)
        {
            return new BasicBoolean(values[index]);
        }

        public override int getExtraParamForType()
        {
            throw new NotImplementedException();
        }
    }

}