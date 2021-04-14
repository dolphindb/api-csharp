using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

    public class BasicStringVector : AbstractVector
    {
        private List<string> values;
        private bool isSymbol;
        //2021.01.19 cwj
        private bool isBlob = false;
        //

        public BasicStringVector(int size) : this(DATA_FORM.DF_VECTOR, size, false)
        {
        }

        public BasicStringVector(IList<string> list) : base(DATA_FORM.DF_VECTOR)
        {
            if (list != null)
            {
                values = (List<String>)list;
                //values = new string[list.Count];
                //for (int i = 0; i < list.Count; ++i)
                //{
                //    values[i] = list[i];
                //}
            }
            isSymbol = false;
        }

        //2021.01.19 cwj
        public BasicStringVector(IList<string> list, bool blob) : base(DATA_FORM.DF_VECTOR)
        {
            if (list != null)
            {
                values = (List<String>)list;
                //values = new string[list.Count];
                //for (int i = 0; i < list.Count; ++i)
                //{
                //    values[i] = list[i];
                //}
            }
            isSymbol = false;
            isBlob = blob;
        }
        //

        public BasicStringVector(string[] array) : base(DATA_FORM.DF_VECTOR)
        {
            values = new List<string>(array.Length);
            values.AddRange(array);
            //values = array.Clone() as string[];
            isSymbol = false;
        }

        protected internal BasicStringVector(DATA_FORM df, int size, bool isSymbol) : base(df)
        {
            values = new List<string>(size);
            values.AddRange(new string[size]);
            this.isSymbol = isSymbol;
        }
        //2021.01.19 cwj
        protected internal BasicStringVector(DATA_FORM df, int size, bool isSymbol, bool blob) : base(df)
        {
            values = new List<string>(size);
            values.AddRange(new string[size]);
            this.isSymbol = isSymbol;
            this.isBlob = blob;
        }
        //
        protected internal BasicStringVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
        {
            int rows = @in.readInt();
            int columns = @in.readInt();
            int size = rows * columns;
            values = new List<string>(size);
            values.AddRange(new string[size]);
            for (int i = 0; i < size; ++i)
            {
                values[i] = @in.readString();
            }
        }

        //2021.01.19 cwj
        protected internal BasicStringVector(DATA_FORM df, ExtendedDataInput @in, bool symbol, bool blob) : base(df)
        {
            isBlob = blob;
            isSymbol = symbol;
            int rows = @in.readInt();
            int columns = @in.readInt();
            int size = rows * columns;
            values = new List<string>(size);
            values.AddRange(new string[size]);

            if(!blob)
            {
                for (int i = 0; i < size; ++i)
                {
                    values[i] = @in.readString();
                }
            }
            else
            {
                for (int i = 0; i < size; ++i)
                {
                    values[i] = @in.readBlob();
                }
            }


            
        }
        //

        public override IScalar get(int index)
        {
            return new BasicString(values[index]);
        }

        public virtual string getString(int index)
        {
            return values[index];
        }

        public override void set(int index, IScalar value)
        {
            if (value.getDataType() == DATA_TYPE.DT_STRING)
            {
                values[index] = ((BasicString)value).getString();
            }
            
        }

        public virtual void setString(int index, string value)
        {
            values[index] = value;
        }

        public override bool isNull(int index)
        {
            return string.ReferenceEquals(values[index], null) || values[index].Length == 0;
        }

        public override void setNull(int index)
        {
            values[index] = "";
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.LITERAL;
        }

        public override DATA_TYPE getDataType()
        {
            //2021.01.19 cwj
            if (isBlob) return DATA_TYPE.DT_BLOB;
            //
            return isSymbol ? DATA_TYPE.DT_SYMBOL : DATA_TYPE.DT_STRING;
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
            if (isBlob)
            {
                foreach (string str in values)
                {
                    @out.writeBlob(str);
                }
            }
            else
            {
                foreach (string str in values)
                {
                    @out.writeString(str);
                }
            }
            
        }

        public override object getList()
        {
            return values;
        }

        public override void set(int index, string value)
        {
            values[index] = value;
        }

        public override void add(object value)
        {
            values.Add(value.ToString());
        }

        public override void addRange(object list)
        {
            List<string> data = (List<string>)list;
            values.AddRange(data);
        }

        public override int hashBucket(int index, int buckets)
        {
            return BasicString.hashBucket(values[index], buckets);
        }

        public override IVector getSubVector(int[] indices)
        {
            int length = indices.Length;
            String[] sub = new String[length];
            for (int i = 0; i < length; ++i)
                sub[i] = values[indices[i]];
            return new BasicStringVector(sub);
        }

        public override int asof(IScalar value)
        {
            String target = value.getString();
            int start = 0;
            int end = values.Count - 1;
            int mid;
            while (start <= end)
            {
                mid = (start + end) / 2;
                
                if (string.Compare(values[mid], target) <= 0)
                    start = mid + 1;
                else
                    end = mid - 1;
            }
            return end;
        }


    }

}