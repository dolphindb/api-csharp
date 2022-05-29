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
            else
                throw new Exception("The value must be a string scalar. ");
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

        public  int serialize(byte[] buf, int bufSize, int start, int elementCount, out int offect)
        {
            int len = 0;
            int total = values.Count;
            int count = 0;
            if (isBlob)
            {
                while(len < bufSize && count < elementCount && start + count < total)
                {
                    string str = values[start + count];
                    int strLen = str.Length;
                    if (strLen + sizeof(int) + 1 >= bufSize - len)
                        break;
                    byte[] lenTmp = BitConverter.GetBytes(strLen);
                    byte[] strTmp = System.Text.Encoding.Default.GetBytes(str);
                    Array.Copy(strTmp, 0, buf, len, sizeof(int));
                    Array.Copy(strTmp, 0, buf, len + sizeof(int), strLen);
                    len += strLen + sizeof(int) + 1;
                    buf[len - 1] = 0;
                    count++;
                }
            }
            else
            {
                while (len < bufSize && count < elementCount && start + count < total)
                {
                    string str = values[start + count];
                    int strLen = str.Length;
                    if (strLen + 1 >= bufSize - len)
                        break;
                    byte[] strTmp = System.Text.Encoding.Default.GetBytes(str);
                    Array.Copy(strTmp, 0, buf, len, strLen);
                    len += strLen + 1;
                    buf[len - 1] = 0;
                    count++;
                }
            }
            offect = start + count;
            return len;

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
        
        protected override void writeVectorToBuffer(ByteBuffer buffer){
		    foreach(String val in values){
			    byte[] tmp = System.Text.Encoding.Default.GetBytes(val);
                buffer.WriteBytes(tmp, 0, tmp.Length);
			    buffer.WriteByte((byte)0);
		    }
        }

        public override void deserialize(int start, int count, ExtendedDataInput @in)
        {
            throw new NotImplementedException();
        }

        public override void serialize(int start, int count, ExtendedDataOutput @out)
        {
            for (int i = 0; i < count; ++i)
            {
                @out.writeString(values[start + i]);
            }
        }

        public override int getUnitLength()
        {
            return 1;
        }
        public override int serialize(int indexStart, int offect, int targetNumElement, out int numElement, out int partial, ByteBuffer @out)
        {
            int readByte = 0;
            targetNumElement = Math.Min(targetNumElement, values.Count - indexStart);
            numElement = 0;
            for (int i = 0; i < targetNumElement; ++i, ++numElement)
            {
                byte[] data = System.Text.Encoding.Default.GetBytes(values[indexStart + i]);
                if (isBlob)
                {
                    if (sizeof(int) + data.Length > @out.remain())
                        break;
                    @out.WriteInt(data.Length);
                    @out.WriteBytes(data);
                    readByte += sizeof(int) + data.Length;
                }
                else
                {
                    if (sizeof(byte) + data.Length > @out.remain())
                        break;
                    @out.WriteBytes(data);
                    @out.WriteByte(0);
                    readByte += sizeof(byte) + data.Length;
                }
            }
            partial = 0;
            if(numElement == 0)
                throw new Exception("too large data. ");
            return readByte;
        }

        public override void append(IScalar value)
        {
            values.Add(((BasicString)value).getValue());
        }

        public override void append(IVector value)
        {
            values.AddRange(((BasicStringVector)value).getdataArray());
        }

        public List<string> getdataArray()
        {
            return values;
        }
    }

}