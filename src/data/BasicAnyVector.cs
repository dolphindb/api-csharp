using dolphindb.io;
using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;

namespace dolphindb.data
{
    public class BasicAnyVector : AbstractVector
    {
        private IEntity[] values;


        public BasicAnyVector(int size) : base(DATA_FORM.DF_VECTOR)
        {
            values = new IEntity[size];
        }

        protected BasicAnyVector(IEntity[] array, bool copy): base(DATA_FORM.DF_VECTOR)
        {
            
            if (copy)
                values = (IEntity[])array.Clone();
            else
                values = array;
        }

        protected internal BasicAnyVector(ExtendedDataInput @in) : base(DATA_FORM.DF_VECTOR)
        {
            int rows = @in.readInt();
            int cols = @in.readInt();
            int size = rows * cols;
            values = new IEntity[size];

            BasicEntityFactory factory = new BasicEntityFactory();
            for (int i = 0; i < size; ++i)
            {
                short flag = @in.readShort();
                int form = flag >> 8;
                int type = flag & 0xff;
                bool extended = type >= 128;
                if (type >= 128)
                    type -= 128;
                //if (form != 1)
                //assert (form == 1);
                //if (type != 4)
                //assert(type == 4);
                IEntity obj = factory.createEntity((DATA_FORM)form, (DATA_TYPE)type, @in, extended);
                values[i] = obj;
            }

        }

        public virtual IEntity getEntity(int index)
        {
            return values[index];
        }

        public override IScalar get(int index)
        {
            if (values[index].isScalar())
            {
                return (IScalar)values[index];
            }
            else
            {
                throw new Exception("The element of the vector is not a scalar object.");
            }
        }

        public override void set(int index, IScalar value)
        {
            values[index] = value;
        }

        public virtual void setEntity(int index, IEntity value)
        {
            values[index] = value;
        }

        public override bool isNull(int index)
        {
            return values[index] == null || (values[index].isScalar() && ((IScalar)values[index]).isNull());
        }

        public override void setNull(int index)
        {
            values[index] = new Void();
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.MIXED;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_ANY;
        }

        public override int rows()
        {
            return values.Length;
        }

        public override string getString()
        {
            StringBuilder sb = new StringBuilder("(");
            int size = Math.Min(10, rows());
            if (size > 0)
            {
                sb.Append(getEntity(0).getString());
            }
            for (int i = 1; i < size; ++i)
            {
                sb.Append(',');
                sb.Append(getEntity(i).getString());
            }
            if (size < rows())
            {
                sb.Append(",...");
            }
            sb.Append(")");
            return sb.ToString();
        }

        public override Type getElementClass()
        {
            return typeof(IEntity);
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            foreach (IEntity value in values)
            {
                value.write(@out);
            }
        }

        public override void set(int index, string value)
        {
            values[index] = new BasicString(value);
        }

        public override object getList()
        {
            return values.ToList();
        }

        public override void add(object value)
        {
            throw new NotImplementedException();
        }

        public override void addRange(object list)
        {
            throw new NotImplementedException();
        }

        public override IVector getSubVector(int[] indices)
        {
            int length = indices.Length;
            IEntity[] sub = new IEntity[length];
            for (int i = 0; i < length; ++i)
                sub[i] = values[indices[i]];
            return new BasicAnyVector(sub, false);
        }

        public override int asof(IScalar value)
        {
            throw new Exception("BasicAnyVector.asof not supported.");
        }

        public override void deserialize(int start, int count, ExtendedDataInput @in)
        {
            throw new NotImplementedException();
        }

        public override void serialize(int start, int count, ExtendedDataOutput @out)
        {
            throw new NotImplementedException();
        }

        public override int getUnitLength()
        {
            throw new NotImplementedException();
        }

        public override int serialize(int start, int count, int offect, out int numElement, out int parition, ByteBuffer @out)
        {
            throw new NotImplementedException();
        }

        public override void append(IScalar value)
        {
            throw new NotImplementedException();
        }

        public override void append(IVector value)
        {
            throw new NotImplementedException();
        }
    }

}