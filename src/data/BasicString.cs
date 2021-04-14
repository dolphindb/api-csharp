using System;
using dolphindb.io;

namespace dolphindb.data
{

    public class BasicString : AbstractScalar, IComparable<BasicString>
    {
        private string value;

        public BasicString(string value)
        {
            this.value = value;
        }

        public BasicString(ExtendedDataInput @in)
        {
            value = @in.readString();
        }
        //2021.01.19 cwj
        public BasicString(ExtendedDataInput @in, bool blob)
        {
            if (blob)
                value = @in.readBlob();
            else
                value = @in.readString();
        }
        //


        public string getValue()
        {
            return value;
        }

        protected void setValue(string value)
        {
            this.value = value;
        }


        public override bool isNull()
        {
            return value.Length == 0;
        }

        public override void setNull()
        {
            value = "";
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.LITERAL;
        }

        public override DATA_TYPE getDataType()
        {
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
                return value.Equals(((BasicString)o).value);
            }
        }

        public override int GetHashCode()
        {
            return value.GetHashCode();
        }


        protected override void writeScalarToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeString(value);
        }
        //2021.01.19 cwj override?
        protected void writeScalarToOutputStream(ExtendedDataOutput @out,bool blob)
        {
            if (blob)
                @out.writeBlob(value);
            else
                @out.writeString(value);
            
        }

        public virtual int CompareTo(BasicString o)
        {
            return value.CompareTo(o.value);
        }

        public override string getString()
        {
            return value;
        }

        public override object getObject()
        {
            return getValue();
        }

        public override void setObject(object value)
        {
            this.value = Convert.ToString(value);
        }

        public static int hashBucket(String str, int buckets)
        {
            int length = str.Length;

            //check utf8 bytes
            int bytes = 0;
            for (int i = 0; i < length; ++i)
            {
                char c = str[i];
                if (c >= '\u0001' && c <= '\u007f')
                    ++bytes;
                else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff'))
                    bytes += 2;
                else
                    bytes += 3;
            }

            //calculate murmur32 hash
            int h = bytes;
            if (bytes == length)
            {
                int length4 = bytes / 4;
                for (int i = 0; i < length4; i++)
                {
                    int i4 = i * 4;
                    int k = (str[i4] & 0xff) + ((str[i4+1] & 0xff) << 8)
                            + ((str[i4+2] & 0xff) << 16) + ((str[i4+3] & 0xff) << 24);
                    k *= 0x5bd1e995;
                    k ^= k >> 24;
                    k *= 0x5bd1e995;
                    h *= 0x5bd1e995;
                    h ^= k;
                }
                // Handle the last few bytes of the input array
                switch (bytes % 4)
                {
                    case 3:
                        h ^= (str[(bytes & ~3) + 2] & 0xff) << 16;
                        h ^= (str[(bytes & ~3) + 1] & 0xff) << 8;
                        h ^= str[bytes & ~3] & 0xff;
                        h *= 0x5bd1e995;
                        break;
                    case 2:
                        h ^= (str[(bytes & ~3) + 1] & 0xff) << 8;
                        h ^= str[bytes & ~3] & 0xff;
                        h *= 0x5bd1e995;
                        break;
                    case 1:
                        h ^= str[bytes & ~3] & 0xff;
                        h *= 0x5bd1e995;
                        break;
                }

                h ^= h >> 13;
                h *= 0x5bd1e995;
                h ^= h >> 15;
            }
            else
            {
                int k = 0;
                int cursor = 0;
                for (int i = 0; i < length; ++i)
                {
                    char c = str[i];
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
                            k ^= k >> 24;
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
                            k ^= k >> 24;
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
                            k ^= k >> 24;
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
                        k ^= k >> 24;
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

                h ^= h >> 13;
                h *= 0x5bd1e995;
                h ^= h >> 15;
            }
            if (h >= 0)
                return h % buckets;
            else
            {
                return (int)((4294967296l + h) % buckets);
            }
        }




    }

}