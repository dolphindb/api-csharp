using System;
using dolphindb.io;

namespace dolphindb.data
{

	public class BasicByte : AbstractScalar, IComparable<BasicByte>
	{
		private byte value;

		public BasicByte(byte value)
		{
			this.value = value;
		}

		public BasicByte(ExtendedDataInput @in)
		{
			value = @in.readByte();
		}

		public virtual byte getValue()
		{
			return value;
		}

		public override bool isNull()
		{
			return value == byte.MinValue;
		}

		public override void setNull()
		{
			value = byte.MinValue;
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.INTEGRAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_BYTE;
		}

		public override Number getNumber()
		{
            if (isNull())
                return null;
            else
                return new Number(value);
        }


		public override object getTemporal()
		{
			throw new Exception("Imcompatible data type");
		}

		public override string getString()
		{
			if (isNull())
				{
					return "";
				}
				else if (value > 31 && value < 127)
				{
					return "'" + ((char)value).ToString() + "'";
				}
				else
				{
					return value.ToString();
				}
		}

		public override bool Equals(object o)
		{
			if (!(o is BasicByte) || o == null)
			{
				return false;
			}
			else
			{
				return value == ((BasicByte)o).value;
			}
		}

		public override int GetHashCode()
		{
			return (new byte?(value)).GetHashCode();
		}

		protected override void writeScalarToOutputStream(ExtendedDataOutput @out)
		{
			@out.writeByte(value);
		}

		public virtual int CompareTo(BasicByte o)
		{
            return value.CompareTo(o.value);
        }
	}

}