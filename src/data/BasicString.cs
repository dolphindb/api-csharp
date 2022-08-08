using System;
using dolphindb.io;
using System.Text;

namespace dolphindb.data
{

    public class BasicString : AbstractScalar, IComparable<BasicString>
    {
        private string value;
        private byte[] blobValue;
        private bool isBlob;

        public BasicString(string value, bool isBlob = false)
        {
            if (isBlob)
                blobValue = Encoding.UTF8.GetBytes(value);
            else
                this.value = value;
            this.isBlob = isBlob;
        }

        public BasicString(byte[] value, bool isBlob = false)
        {
            if (isBlob)
                blobValue = value;
            else
                this.value = Encoding.UTF8.GetString(value);
            this.isBlob = isBlob;
        }

        public BasicString(ExtendedDataInput @in)
        {
            value = @in.readString();
        }
        //2021.01.19 cwj
        public BasicString(ExtendedDataInput @in, bool blob)
        {
            if (blob)
                blobValue = @in.readBlob();
            else
                value = @in.readString();
            isBlob = blob;
        }
        //


        public string getValue()
        {
            if (isBlob)
                return Encoding.UTF8.GetString(blobValue);
            else
                return value;
        }

        protected void setValue(string value)
        {
            if(isBlob)
                this.blobValue = Encoding.UTF8.GetBytes(value);
            else
                this.value = value;
        }


        public override bool isNull()
        {
            if (isBlob)
                return blobValue.Length == 0;
            else
                return value.Length == 0;
        }

        public override void setNull()
        {
            if (isBlob)
                blobValue = new byte[0];
            else
                value = "";
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.LITERAL;
        }

        public override DATA_TYPE getDataType()
        {
            if(isBlob)
                return DATA_TYPE.DT_BLOB;
            else 
                return DATA_TYPE.DT_STRING;
        }

        public override Number getNumber()
        {
            throw new Exception("Imcompatible data type");
        }

        public override object getTemporal()
        {
            throw new Exception("Imcompatible data type");
        }

        public override bool Equals(object o)
        {
            if (!(o is BasicString) || o == null)
            {
                return false;
            }
            else
            {
                if (isBlob != ((BasicString)o).isBlob)
                    return false;
                if (isBlob)
                {
                    byte[] oValue = ((BasicString)o).blobValue;
                    if (blobValue.Length != oValue.Length)
                        return false;
                    else
                    {
                        for(int i = 0; i < blobValue.Length; ++i)
                        {
                            if (blobValue[i] != oValue[i])
                                return false;
                        }
                    }
                }
                else
                    return value.Equals(((BasicString)o).value);
            }
            return true;
        }

        public override int GetHashCode()
        {
            if (isBlob)
            {
                StringBuilder stringBuilder = new StringBuilder();
                for(int i = 0; i < blobValue.Length; ++i)
                {
                    stringBuilder.Append((char)blobValue[i]);
                }
                return stringBuilder.ToString().GetHashCode();
            }
            return value.GetHashCode();
        }


        protected override void writeScalarToOutputStream(ExtendedDataOutput @out)
        {
            if (isBlob)
                @out.writeBlob(blobValue);
            else
                @out.writeString(value);
        }
        //2021.01.19 cwj override?
        protected void writeScalarToOutputStream(ExtendedDataOutput @out,bool blob)
        {
            if (blob)
                @out.writeBlob(blobValue);
            else
                @out.writeString(value);
            
        }

        public virtual int CompareTo(BasicString o)
        {
            if (isBlob)
                return Encoding.UTF8.GetString(blobValue).CompareTo(o.value);
            else
                return value.CompareTo(o.value);
        }

        public override string getString()
        {
            if (isBlob)
                return Encoding.UTF8.GetString(blobValue);
            return value;
        }

        public override object getObject()
        {
            return getValue();
        }

        public override void setObject(object value)
        {
            if (isBlob)
            {
                string temp = Convert.ToString(value);
                blobValue = Encoding.UTF8.GetBytes(temp);
            }
            else
                this.value = Convert.ToString(value);
        }

        public static int hashBucket(String str, int buckets)
        {
            int length = str.Length;

            int bytes = 0;
            char[] cc = str.ToCharArray();
            for(int i = 0; i < length; i++)
            {
                char c = cc[i];
                if (c >= '\u0001' && c <= '\u007f')
                    ++bytes;
                else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff'))
                    bytes += 2;
                else
                    bytes += 3;
            }

            int h = bytes;
            if(bytes == length)
            {
                int length4 = bytes / 4;
                for (int i = 0; i < length4; i++)
                {
                    int i4 = i * 4;
                    int k = (cc[i4] & 0xff) + ((cc[i4+1] & 0xff) << 8)
                            + ((cc[i4+2] & 0xff) << 16) + ((cc[i4+3] & 0xff) << 24);
                    k *= 0x5bd1e995;
                    k ^= Utils.UIntMoveRight(k, 24);
                    k *= 0x5bd1e995;
                    h *= 0x5bd1e995;
                    h ^= k;
                }

                switch (bytes % 4)
                {
                    case 3:
                        h ^= (cc[(bytes & ~3) + 2] & 0xff) << 16;
                        break;
                    case 2:
                        h ^= (cc[(bytes & ~3) + 1] & 0xff) << 8;
                        break;
                    case 1:
                        h ^= cc[bytes & ~3] & 0xff;
                        h *= 0x5bd1e995;
                        break;
                }

                h ^= Utils.UIntMoveRight(h, 13);
                h *= 0x5bd1e995;
                h ^= Utils.UIntMoveRight(h, 15);
            }
            else
            {
                int k = 0;
                int cursor = 0;
                for (int i = 0; i < length; ++i)
                {
                    char c = cc[i];
                    if (c >= '\u0001' && c <= '\u007f')
                    {
                        k += c << (8 * cursor++);
                    }
                    else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff'))
                    {
                        k += (0xc0 | (0x1f & (c >> 6))) << (8 * cursor++);
                        if (cursor == 4)
                        {
                            k *= 0x5bd1e995;
                            k ^= Utils.UIntMoveRight(k, 24);
                            k *= 0x5bd1e995;
                            h *= 0x5bd1e995;
                            h ^= k;
                            k = 0;
                            cursor = 0;
                        }
                        k += (0x80 | (0x3f & c)) << (8 * cursor++);
                    }
                    else
                    {
                        k += (0xe0 | (0x0f & (c >> 12))) << (8 * cursor++);
                        if (cursor == 4)
                        {
                            k *= 0x5bd1e995;
                            k ^= Utils.UIntMoveRight(k, 24);
                            k *= 0x5bd1e995;
                            h *= 0x5bd1e995;
                            h ^= k;
                            k = 0;
                            cursor = 0;
                        }
                        k += (0x80 | (0x3f & (c >> 6))) << (8 * cursor++);
                        if (cursor == 4)
                        {
                            k *= 0x5bd1e995;
                            k ^= Utils.UIntMoveRight(k, 24);
                            k *= 0x5bd1e995;
                            h *= 0x5bd1e995;
                            h ^= k;
                            k = 0;
                            cursor = 0;
                        }
                        k += (0x80 | (0x3f & c)) << (8 * cursor++);
                    }
                    if (cursor == 4)
                    {
                        k *= 0x5bd1e995;
                        k ^= Utils.UIntMoveRight(k, 24);
                        k *= 0x5bd1e995;
                        h *= 0x5bd1e995;
                        h ^= k;
                        k = 0;
                        cursor = 0;
                    }
                }
                if (cursor > 0)
                {
                    h ^= k;
                    h *= 0x5bd1e995;
                }

                h ^= Utils.UIntMoveRight(h, 13);
                h *= 0x5bd1e995;
                h ^= Utils.UIntMoveRight(h, 15);
            }
            if (h >= 0)
                return h % buckets;
            else
            {
                return (int)((4294967296l + h) % buckets);
            }
        }

            //return str.GetHashCode() % buckets;

        public override int hashBucket(int buckets)
        {
            if (isBlob)
            {
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 0; i < blobValue.Length; ++i)
                {
                    stringBuilder.Append((char)blobValue[i]);
                }
                return hashBucket(stringBuilder.ToString(), buckets);
            }
            else
                return hashBucket(this.value, buckets);
        }

        public byte[] getBytes()
        {
            if (isBlob)
                return blobValue;
            else
                throw new Exception("The value must be a string scalar. ");
        }
    }

}