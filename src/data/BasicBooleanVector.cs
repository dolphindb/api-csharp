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

        protected BasicBooleanVector(byte[] array, bool copy) : base(DATA_FORM.DF_VECTOR)
        {
           
            if (copy)
                values = (List<byte>)array.Clone();
            else
            {
                values = new List<byte>(array.Length);
                values.AddRange(array);
            }
                
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
            return values[index] == byte.MinValue;
        }

        public override void setNull(int index)
        {
            values[index] = byte.MinValue;
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
            bool v;
            if(bool.TryParse(value, out v))
            {
                values[index] = v ? (byte)1 : (byte)0; ;
            }
            else
            {
                setNull(index);
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
            return new BasicBooleanVector(sub, false);
        }

        public override int asof(IScalar value)
        {
            throw new Exception("BasicBooleanVector.asof not supported.");
        }

    }

}