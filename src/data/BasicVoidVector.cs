using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.data
{
    internal class BasicVoidVector : AbstractVector
    {
        public BasicVoidVector(int size) : base(DATA_FORM.DF_VECTOR)
        {
            size_= size;
        }

        public BasicVoidVector(DATA_FORM df, ExtendedDataInput @in) : base(DATA_FORM.DF_VECTOR)
        {
            int rows = @in.readInt();
            int cols = @in.readInt();
            int size = rows * cols;
            size_ = size;

        }

        int size_ = 0;
        public override void add(object value)
        {
            size_++;
        }

        public override void addRange(object list)
        {
            size_++;
        }

        public override void append(IScalar value)
        {
            size_++;
        }

        public override void append(IVector value)
        {
            size_ += value.rows();
        }

        public override int asof(IScalar value)
        {
            throw new NotImplementedException();
        }

        public override void deserialize(int start, int count, ExtendedDataInput @in)
        {
        }

        public override IScalar get(int index)
        {
            return new Void();
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.NOTHING;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_VOID;
        }

        public override Type getElementClass()
        {
            return typeof(Void);
        }

        public override IEntity getEntity(int index)
        {
            return new Void();
        }

        public override IVector getSubVector(int[] indices)
        {
            return new BasicVoidVector(indices.Length);
        }

        public override int getUnitLength()
        {
            throw new NotImplementedException();
        }

        public override int hashBucket(int index, int buckets)
        {
            throw new NotImplementedException();
        }

        public override bool isNull(int index)
        {
            return true;
        }

        public override int rows()
        {
            return size_;
        }

        public override void serialize(int start, int count, ExtendedDataOutput @out)
        {
        }

        public override int serialize(int indexStart, int offect, int targetNumElement, out int numElement, out int partial, ByteBuffer @out)
        {
            numElement = targetNumElement;
            partial = 0;
            return targetNumElement;
        }

        public override void set(int index, IScalar value)
        {
        }

        public override void set(int index, string value)
        {
        }

        public override void setNull(int index)
        {
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
        }
    }
}
