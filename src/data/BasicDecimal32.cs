using dolphindb.data;
using dolphindb.io;
using Microsoft.VisualBasic;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Text;

namespace dolphindb.data
{
    public class BasicDecimal32 : AbstractScalar
    {
        private int scale_ = 0;//Decimal precision
        private int value_; //covert decimal to int for saving

        public BasicDecimal32(string data, int scale = -1)
        {
            if(scale == -1)
            {
                value_ = parseString(data, ref scale_, false);
            }
            else
            {
                scale_ = scale;
                value_ = parseString(data, ref scale);
            }
        }

        public BasicDecimal32(decimal data, int scale)
        {
            checkScale(scale);
            try
            {
                data = checked(pow10(scale) * data);
                scale_ = scale;
                value_ = checked((int)Math.Round(data));
            }catch(OverflowException)
            {
                throw new OverflowException("Decimal math overflow!");
            }
        }

        public BasicDecimal32(ExtendedDataInput @input)
        {
            scale_ = @input.readInt();
            value_ = @input.readInt();
        }

        internal BasicDecimal32(int rawData, int scale)
        {
            checkScale(scale);
            value_ = rawData;
            scale_ = scale;
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
            return DATA_TYPE.DT_DECIMAL32;
        }

        public override object getObject()
        {
            return getString();
        }
        
        public decimal getDecimalValue()
        {
            decimal tmp = value_;
            return tmp / pow10(scale_);
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
                int val = value_;
                if (val < 0) val *= -1;

                int pow = pow10(scale_);
                int begin = val / (int)pow;
                sb.Append(begin.ToString());
                if (scale_ > 0)
                {
                    sb.Append(".");
                    int after = val % pow;
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
            return value_ == int.MinValue;
        }

        public override void setNull()
        {
            value_ = int.MinValue;
        }

        public override Number getNumber()
        {
            return new Number(getDecimalValue());
        }

        public override int getScale()
        {
            return scale_;
        }

        public int getRawData()
        {
            return value_;
        }

        public void setRawData(int rawData)
        {
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
            @out.writeInt(value_);
        }

        public override bool Equals(object o)
        {
            if (!(o is BasicDecimal32) || o == null)
            {
                return false;
            }
            else
            {
                BasicDecimal32 obj = (BasicDecimal32)o;
                return value_ == obj.value_ && scale_ == obj.scale_;
            }
        }

        internal static void checkScale(int scale)
        {
            if (scale < 0 || scale > 9)
            {
                throw new Exception("Scale " + scale + " is out of bounds, it must be in [0,9].");
            }
        }

        internal static int pow10(int scale) // TODO: Improve performance
        {
            if (scale == 0) return 1;
            int data = 1;
            for(int i = 0; i < scale; ++i)
            {
                try
                {
                    data = checked(data * 10);
                } catch (OverflowException)
                {
                    throw new Exception("Decimal math overflow!");
                }
            }
            return data;
        }

        internal static int parseString(string data, ref int scale, bool scale_work=true)
        {
            if(scale_work) checkScale(scale);
            if (data == "")
            {
                if (!scale_work)
                {
                    throw new Exception("When the data is an empty string, the scale must be set. ");
                }
                else
                {
                    return int.MinValue;
                }
            }
            BigInteger tmp = BasicDecimal128.parseString(data, ref scale, scale_work);
            if (tmp > int.MaxValue || tmp < int.MinValue)
            {
                throw new Exception("Decimal math overflow!");
            }
            return ((int)tmp);
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
