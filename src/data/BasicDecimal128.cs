using dolphindb.data;
using dolphindb.io;
using Microsoft.VisualBasic;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Text;
using System.Text.RegularExpressions;

namespace dolphindb.data
{
    public class BasicDecimal128 : AbstractScalar
    {
        private int scale_ = 0;//Decimal precision
        private BigInteger value_; //covert decimal to int for saving

        public static readonly BigInteger NULL = BigInteger.Parse("-170141183460469231731687303715884105728");
        public static readonly BigInteger MAX = BigInteger.Pow(2, 128) - 1;

        public BasicDecimal128(string data, int scale = -1)
        {
            if (scale == -1)
            {
                value_ = parseString(data, ref scale_, false);
            }
            else
            {
                scale_ = scale;
                value_ = parseString(data, ref scale);
            }
        }

        public BasicDecimal128(decimal data, int scale){
            checkScale(scale);
            value_ = parseString(data.ToString(), ref scale, true); // TODO: Improve performance
            this.scale_ = scale;
        }

        internal BasicDecimal128(ExtendedDataInput @input, int scale = -1)
        {
            scale_ = scale == -1 ? @input.readInt() : scale;
            byte[] data = new byte[16];
            @input.readFully(data);
            reserveBytes(data, !@input.isLittleEndian());
            value_ = createBigIntegerByBytes(data);
        }


        internal BasicDecimal128(BigInteger rawData, int scale)
        {
            checkScale(scale);
            checkValueRange(rawData);
            value_ = rawData;
            scale_ = scale;
            checkValueRange(value_);
        }


        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.DENARY;
        }

        public override string ToString()
        {
            return getString();
        }
        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_DECIMAL128;
        }

        public override object getObject()
        {
            return getString();
        }

        public decimal getDecimalValue()
        {
            decimal tmp = (decimal)value_;
            return tmp / (decimal)pow10(scale_);
        }

        public override string getString()
        {
            if (scale_ == 0 && (!isNull()))
                return value_.ToString();
            else if (isNull())
                return "";
            else
            {
                StringBuilder sb = new StringBuilder();
                if (value_ < 0)
                    sb.Append("-");
                BigInteger val = value_;
                if (val < 0) val *= -1;

                BigInteger pow = (BigInteger)pow10(scale_);
                BigInteger begin = val / pow;
                sb.Append(begin.ToString());
                if (scale_ > 0)
                {
                    sb.Append(".");
                    BigInteger after = val % pow;
                    int zeroBeginSize = pow.ToString().Length - after.ToString().Length - 1;
                    for (int i = 0; i < zeroBeginSize; ++i) sb.Append('0');
                    sb.Append(after.ToString());
                    int zeroSize = scale_ - after.ToString().Length - zeroBeginSize;
                    for (int i = 0; i < zeroSize; ++i) sb.Append('0');
                }
                return sb.ToString();
            }
        }

        public override bool isNull()
        {
            return value_ == NULL;
        }

        public override void setNull()
        {
            value_ = new BigInteger(NULL.ToByteArray());
        }

        public override Number getNumber()
        {
            return new Number(getDecimalValue());
        }

        public override int getScale()
        {
            return scale_;
        }

        public BigInteger getRawData()
        {
            return value_;
        }

        public void setRawData(BigInteger rawData)
        {
            checkValueRange(rawData);
            value_ = rawData;
        }

        public override int hashBucket(int buckets)
        {
            throw new NotImplementedException();
        }

        public override void setObject(object value)
        {
            if (typeof(string) == value.GetType())
            {
                value_ = parseString((string)value, ref scale_, true);
            }
            else
            {
                throw new ArgumentException("the type of value must be string.");
            }
        }

        public override void writeScalarToOutputStream(ExtendedDataOutput @out)
        {
            @out.write(reserveBytes(getBigIntegerBytes(value_).ToArray(), !@out.isLittleEndian()));
        }

        public override bool Equals(object o)
        {
            if (!(o is BasicDecimal128) || o == null)
            {
                return false;
            }
            else
            {
                BasicDecimal128 obj = (BasicDecimal128)o;
                return value_ == obj.value_ && scale_ == obj.scale_;
            }
        }

        internal static List<byte> getBigIntegerBytes(BigInteger data)
        {
            checkValueRange(data);
#if NETFRAMEWORK
            byte[] bytes = data.ToByteArray();
#else
            byte[] bytes = data.ToByteArray(false, false);
#endif
            List<byte> buffer = new List<byte>();
            buffer.AddRange(new byte[16]);
            int dataLen = bytes.Length;
            for (int i = 0; i < dataLen; ++i)
            {
                buffer[i] = (byte)bytes[i];
            }
            if (data < 0 && dataLen < 16)
            {
                buffer[dataLen] = (byte)(256 - buffer[dataLen]);
                for (int i = dataLen; i < 16; ++i)
                {
                    buffer[i] = 255;
                }
            }
            return buffer;
        }

        internal static void checkScale(int scale)
        {
            if (scale < 0 || scale > 38)
            {
                throw new Exception("Scale " + scale + " is out of bounds, it must be in [0,38].");
            }
        }

        internal static byte[] reserveBytes(byte[] data, bool needReserve)
        {
            if(needReserve){
                int size = data.Length;
                int group = size / 16;
                for(int i = 0; i < group; ++i)
                {
                    for(int j = 0; j < 8; ++j)
                    {
                        byte tmp = data[i * group + j];
                        data[i * group + j] = data[i * group + 15 - j];
                        data[i * group + 15 - j] = tmp;
                    }
                }
            }
            return data;
        }

        internal static void checkValueRange(BigInteger data)
        {
            if (data > MAX)
            {
                throw new Exception("Decimal math overflow!");
            }else if(data < NULL)
            {
                throw new Exception("Decimal math overflow!");
            }
        }

        internal static BigInteger pow10(int scale)
        {
            BigInteger t = (BigInteger.Pow(new BigInteger(10), scale));
            return t;
        }
        internal static BigInteger createBigIntegerByBytes(byte[] data)
        {
#if NETFRAMEWORK
            return new BigInteger(data);
#else
            return new BigInteger(data, false, false);
#endif
        }

        internal static BigInteger parseString(string data, ref int scale, bool scale_work = true)
        {
            if (scale_work) checkScale(scale);
            if (data == "")
            {
                if (!scale_work)
                {
                    throw new Exception("When the data is an empty string, the scale must be set. ");
                }
                else
                {
                    return NULL;
                }
            }
            BigInteger value = 0;

            data = data.Trim();

            //Convert data to int value_ and scale_
            string[] vals = data.Split('.');
            BigInteger integer_ = 0;//integer part
            BigInteger decimal_ = 0;//decimal part
            int decimal_len = 0;

            if (vals.Length == 2)
            {
                //123.45
                if (BigInteger.TryParse(vals[0], out integer_) == false || BigInteger.TryParse(vals[1], out decimal_) == false)
                {
                    throw new FormatException("decimal data form is not correct: " + data);
                }
                decimal_len = vals[1].Length;
            }
            else if (vals.Length == 1)
            {
                //123
                if (BigInteger.TryParse(vals[0], out integer_) == false)
                {
                    throw new FormatException("decimal data form is not correct: " + data);
                }
            }
            else
            {
                //123.5.5 
                throw new FormatException("decimal data form is not correct: " + data);
            }

            if (!scale_work)
            {
                scale = decimal_len;
            }

            if (vals[0].StartsWith("-"))
            {
                decimal_ *= -1;//covert decimal part to negative
            }

            try
            {
                if (decimal_len < scale)
                {
                    //Add zero to decimal part
                    int s = scale - decimal_len;
                    decimal_ = checked(decimal_ * (BigInteger)pow10(s));
                }
                else if (decimal_len > scale)
                {
                    //Truncate decimal part
                    int s = decimal_len - scale;
                    BigInteger powCount = (BigInteger)pow10(s);
                    if (decimal_ >= 0)
                    {
                        decimal_ += powCount / 2;
                    }
                    else
                    {
                        decimal_ -= powCount / 2;
                    }
                    decimal_ = decimal_ / powCount;
                }

                integer_ = checked(integer_ * (BigInteger)pow10(scale));
                value = checked(integer_ + (BigInteger)decimal_);
            }
            catch (OverflowException)
            {
                throw new OverflowException("Decimal math overflow!");
            }
            checkValueRange(value);
            return value;
        }

        public override int GetHashCode()
        {
            int hashCode = -230681975;
            hashCode = hashCode * -1521134295 + scale_.GetHashCode();
            hashCode = hashCode * -1521134295 + value_.GetHashCode();
            return hashCode;
        }
    }
}
