using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Text;

namespace dolphindb.data
{

    public class BasicStringVector : AbstractVector
    {
        private List<string> values;
        private List<byte[]> blobValues;
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
            if (blob)
            {
                int rows = list.Count;
                blobValues = new List<byte[]>();
                for(int i = 0; i < list.Count; ++i)
                {
                    blobValues.Add(Encoding.UTF8.GetBytes(list[i]));
                }
            }
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

        public BasicStringVector(byte[][] array) : base(DATA_FORM.DF_VECTOR)
        {
            blobValues = new List<byte[]>(array.Length);
            blobValues.AddRange(array);
            isBlob = true;
        }

        protected internal BasicStringVector(DATA_FORM df, int size, bool isSymbol) : this(df, size, isSymbol, false)
        {
        }
        //2021.01.19 cwj
        protected internal BasicStringVector(DATA_FORM df, int size, bool isSymbol, bool blob = false) : base(df)
        {
            if (blob)
            {
                blobValues = new List<byte[]>();
                blobValues.AddRange(new byte[size][]);
            }
            else
            {
                values = new List<string>(size);
                values.AddRange(new string[size]);
            }
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
            if (blob)
            {
                blobValues = new List<byte[]>();
                blobValues.AddRange(new byte[rows][]);
                for(int i = 0; i < rows; ++i)
                {
                    blobValues[i] = @in.readBlob();
                }
            }
            else
            {
                values = new List<string>(size);
                values.AddRange(new string[size]);
                for (int i = 0; i < size; ++i)
                {
                    values[i] = @in.readString();
                }
            }
        }
        //

        public override IScalar get(int index)
        {
            if(isBlob)
                return new BasicString(blobValues[index], true);
            else
                return new BasicString(values[index], false);
        }

        public virtual string getString(int index)
        {
            if (isBlob)
            {
                return Encoding.UTF8.GetString(blobValues[index]);
            }
            else
                return values[index];
        }

        public override void set(int index, IScalar value)
        {
            if (isBlob)
            {
                if (value.getDataType() == DATA_TYPE.DT_BLOB)
                {
                    blobValues[index] = ((BasicString)value).getBytes();
                }
                else
                    throw new Exception("The value must be a blob scalar. ");
            }
            else
            {
                if (value.getDataType() == DATA_TYPE.DT_STRING)
                {
                    values[index] = ((BasicString)value).getString();
                }
                else
                    throw new Exception("The value must be a string scalar. ");
            }
        }

        public virtual void setString(int index, string value)
        {
            if (isBlob)
            {
                blobValues[index] = Encoding.UTF8.GetBytes(value);
            }
            else
            {
                values[index] = value;
            }
        }

        public override bool isNull(int index)
        {
            if (isBlob)
                return blobValues[index].Length == 0;
            else
                return string.ReferenceEquals(values[index], null) || values[index].Length == 0;
        }

        public override void setNull(int index)
        {
            if (isBlob)
                blobValues[index] = new byte[0];
            else
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
            if (isBlob)
                return blobValues.Count;
            else
                return values.Count;
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            if (isBlob)
            {
                foreach (byte[] str in blobValues)
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
            int total = blobValues.Count;
            int count = 0;
            if (isBlob)
            {
                while(len < bufSize && count < elementCount && start + count < total)
                {
                    byte[] str = blobValues[start + count];
                    int strLen = str.Length;
                    if (strLen + sizeof(int) + 1 >= bufSize - len)
                        break;
                    byte[] lenTmp = BitConverter.GetBytes(strLen);
                    Array.Copy(lenTmp, 0, buf, len, sizeof(int));
                    Array.Copy(str, 0, buf, len + sizeof(int), strLen);
                    len += strLen + sizeof(int);
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
            if (isBlob)
                return blobValues;
            else
                return values;
        }

        public override void set(int index, string value)
        {
            if (isBlob)
                blobValues[index] = Encoding.UTF8.GetBytes(value);
            else
                values[index] = value;
        }

        public override void add(object value)
        {
            if (isBlob)
                blobValues.Add(Encoding.UTF8.GetBytes(value.ToString()));
            else
                values.Add(value.ToString());
        }

        public override void addRange(object list)
        {
            List<string> data = (List<string>)list;
            if (isBlob)
            {
                for(int i = 0; i < data.Count; ++i)
                {
                    blobValues.Add(Encoding.UTF8.GetBytes(data[i]));
                }
            }
            else
                values.AddRange(data);
        }

        public override int hashBucket(int index, int buckets)
        {
            return get(index).hashBucket(buckets);
        }

        public override IVector getSubVector(int[] indices)
        {
            int length = indices.Length;
            if (isBlob)
            {
                byte[][] sub = new byte[length][];
                for(int i = 0; i < length; ++i)
                {
                    sub[i] = blobValues[indices[i]];
                }
                return new BasicStringVector(sub);                                                      
            }
            else {
                String[] sub = new String[length];
                for (int i = 0; i < length; ++i)
                    sub[i] = values[indices[i]];
                return new BasicStringVector(sub);
            }
        }

        private static int compare(byte[] b1, byte[] b2)
        {
            int minLength = Math.Min(b1.Length, b2.Length);
            for(int i = 0; i < minLength; ++i)
            {
                if (b1[i] < b2[i])
                    return -1;
                if (b1[i] > b2[i])
                    return 1;
            }
            if (b1.Length > b2.Length)
                return 1;
            else if (b1.Length == b2.Length)
                return 0;
            else
                return -1;
        }

        public override int asof(IScalar value)
        {
            if (isBlob)
            {
                if (value.getDataType() != DATA_TYPE.DT_BLOB)
                    throw new Exception("value must be a blob scalar. ");
                byte[] target = ((BasicString)value).getBytes();
                int start = 0;
                int end = blobValues.Count - 1;
                int mid;
                while (start <= end)
                {
                    mid = (start + end) / 2;

                    if (compare(blobValues[mid], target) <= 0)
                        start = mid + 1;
                    else
                        end = mid - 1;
                }
                return end;
            }
            else
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

        public override void deserialize(int start, int count, ExtendedDataInput @in)
        {
            throw new NotImplementedException();
        }

        public override void serialize(int start, int count, ExtendedDataOutput @out)
        {
            if (isBlob)
            {
                for (int i = 0; i < count; ++i)
                {
                    @out.writeBlob(blobValues[start + i]);
                }
            }
            else
            {
                for (int i = 0; i < count; ++i)
                {
                    @out.writeString(values[start + i]);
                }
            }

        }

        public override int getUnitLength()
        {
            return 1;
        }
        public override int serialize(int indexStart, int offect, int targetNumElement, out int numElement, out int partial, ByteBuffer @out)
        {
            int readByte = 0;
            targetNumElement = Math.Min(targetNumElement, isBlob ? blobValues.Count - indexStart: values.Count - indexStart);
            numElement = 0;
            for (int i = 0; i < targetNumElement; ++i, ++numElement)
            {
                if (isBlob)
                {
                    byte[] data = blobValues[i];
                    if (sizeof(int) + data.Length > @out.remain())
                        break;
                    @out.WriteInt(data.Length);
                    @out.WriteBytes(data);
                    readByte += sizeof(int) + data.Length;
                }
                else
                {
                    byte[] data = System.Text.Encoding.Default.GetBytes(values[indexStart + i]);
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
            if (isBlob)
                blobValues.Add(((BasicString)value).getBytes());
            else
                values.Add(((BasicString)value).getValue());
        }

        public override void append(IVector value)
        {
            if(isBlob)
                blobValues.AddRange(((BasicStringVector)value).getDataByteArray());
            else
                values.AddRange(((BasicStringVector)value).getdataArray());
        }

        public List<string> getdataArray()
        {
            return values;
        }

        public List<byte[]> getDataByteArray()
        {
            return blobValues;
        }

        public byte[] getBytes(int index)
        {
            if (isBlob)
            {
                return blobValues[index];
            }
            else
            {
                throw new Exception("The value must be a string scalar. ");
            }
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