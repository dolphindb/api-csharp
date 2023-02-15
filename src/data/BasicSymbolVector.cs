
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
    using dolphindb.io;
    using ExtendedDataInput = io.ExtendedDataInput;
    using ExtendedDataOutput = io.ExtendedDataOutput;

    public class BasicSymbolVector : AbstractVector
    {

        private SymbolBase @base;
        private List<int> values;

        public BasicSymbolVector(int size) : base(DATA_FORM.DF_VECTOR)
        {
            @base = new SymbolBase(0);
            values = new List<int>(new int[size]);
        }

        public BasicSymbolVector(SymbolBase @base, int size) : base(DATA_FORM.DF_VECTOR)
        {
            this.@base = @base;
            values = new List<int>(new int[size]);
        }

        public BasicSymbolVector(IList<string> list) : base(DATA_FORM.DF_VECTOR)
        {
            @base = new SymbolBase(0);
            values = new List<int>(new int[list.Count]);
            for (int i = 0; i < list.Count; ++i)
            {
                values[i] = @base.find(list[i], true);
            }
        }

        public BasicSymbolVector(SymbolBase @base, int[] values, bool copy) : base(DATA_FORM.DF_VECTOR)
        {
            this.@base = @base;
            this.values = new List<int>(values);
        }


        protected internal BasicSymbolVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
        {
            int rows = @in.readInt();
            int columns = @in.readInt();
            int size = rows * columns;
            values = new List<int>(new int[rows]);
            @base = new SymbolBase(@in);
            for (int i = 0; i < size; ++i)
            {
                values[i] = @in.readInt();
            }
        }

        protected internal BasicSymbolVector(DATA_FORM df, ExtendedDataInput @in, SymbolBaseCollection collection) : base(df)
        {
            int rows = @in.readInt();
            int columns = @in.readInt();
            int size = rows * columns;
            values = new List<int>(new int[rows]);
            @base = collection.add(@in);
            for (int i = 0; i < size; ++i)
            {
                values[i] = @in.readInt();
            }
        }


        public override IScalar get(int index)
        {
            return new BasicString(@base.getSymbol(values[index]));
        }

        public virtual IVector getSubVector(int[] indices)
        {
            int length = indices.Length;
            int[] sub = new int[length];
            for (int i = 0; i < length; ++i)
            {
                sub[i] = values[indices[i]];
            }
            return new BasicSymbolVector(@base, sub, false);
        }

        public string getString(int index)
        {
            return @base.getSymbol(values[index]);
        }

        public override void set(int index, IScalar value)
        {
            if(value.getDataType() == DATA_TYPE.DT_STRING)
                values[index] = @base.find(value.getString(), true);
            else
                throw new Exception("The value must be a string scalar. ");
        }

        public override void set(int index, string value)
        {
            values[index] = @base.find(value, true);
        }

        public override int hashBucket(int index, int buckets)
        {
            return BasicString.hashBucket(@base.getSymbol(values[index]), buckets);
        }

        public IVector combine(IVector vector)
        {
            BasicSymbolVector v = (BasicSymbolVector)vector;
            int newSize = this.rows() + v.rows();
            int[] newValue = new int[newSize];
            if (v.@base == @base)
            {
                values.AddRange(((BasicSymbolVector)vector).getdataArray());
            }
            else
            {
                SymbolBase vBase = v.@base;
                int length = vBase.size();
                int[] mapper = new int[length];
                for (int i = 0; i < length; ++i)
                {
                    mapper[i] = @base.find(vBase.getSymbol(i), true);
                }
                length = v.rows();
                List<int> vValues = v.getdataArray();
                int baseRow = this.rows();
                for (int i = 0; i < length; ++i)
                {
                    newValue[baseRow + i] = mapper[vValues[i]];
                }
            }
            return new BasicSymbolVector(@base, newValue, false);
        }

        public override bool isNull(int index)
        {
            return values[index] == 0;
        }

        public override void setNull(int index)
        {
            values[index] = 0;
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.LITERAL;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_SYMBOL;
        }

        public override Type getElementClass()
        {
            return typeof(BasicString);
        }

        public override int rows()
        {
            return values.Count;
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            @base.write(@out);
            @out.writeIntArray(values.ToArray());
        }
        public virtual void write(ExtendedDataOutput @out, SymbolBaseCollection collection)
        {
            int dataType = (int)getDataType() + 128;
            int flag = ((int)DATA_FORM.DF_VECTOR << 8) + dataType;
            @out.writeShort(flag);
            @out.writeInt(rows());
            @out.writeInt(columns());
            collection.write(@out, @base);
            @out.writeIntArray(values.ToArray());
        }

        public override int asof(IScalar value)
        {
            string target = value.getString();
            int start = 0;
            int end = values.Count - 1;
            int mid;
            while (start <= end)
            {
                mid = (start + end) / 2;
                if (@base.getSymbol(values[mid]).CompareTo(target) <= 0)
                {
                    start = mid + 1;
                }
                else
                {
                    end = mid - 1;
                }
            }
            return end;
        }


        public override void add(object value)
        {
            throw new NotImplementedException();
        }

        public override void addRange(object list)
        {
            throw new NotImplementedException();
        }

        public override void deserialize(int start, int count, ExtendedDataInput @in)
        {
            throw new NotImplementedException();
        }

        public override void serialize(int start, int count, ExtendedDataOutput @out)
        {
            for (int i = 0; i < count; ++i)
            {
                @out.writeInt(values[start + i]);
            }
        }

        public override int getUnitLength()
        {
            return 4;
        }

        public override int serialize(int indexStart, int offect, int targetNumElement, out int numElement, out int partial, ByteBuffer @out)
        {
            throw new NotImplementedException();
        }

        public override void append(IScalar value)
        {
            values.Add(@base.find(((BasicString)value).getString(), true));
        }

        public List<int> getdataArray()
        {
            return values;
        }

        public override void append(IVector value)
        {
            throw new NotImplementedException();
        }

        public override IEntity getEntity(int index)
        {
            return get(index);
        }

        public override int getExtraParamForType()
        {
            throw new NotImplementedException();
        }
    }
}