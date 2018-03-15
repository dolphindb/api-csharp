using dolphindb.data;
using dolphindb.io;
using System;

namespace dolphindb.data
{

	public class BasicBoolean : AbstractScalar, IComparable<BasicBoolean>
	{
		private byte value;

		public BasicBoolean(bool value)
		{
			this.value = value ? (byte)1 : (byte)0;
		}

		public BasicBoolean(ExtendedDataInput @in)
		{
			value = @in.readByte();
		}

		protected internal BasicBoolean(byte value)
		{
			this.value = value;
		}

		public virtual bool getValue()
		{
			return value != 0;
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
			return DATA_CATEGORY.LOGICAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_BOOL;
		}

        public override Number getNumber()
		{
			
				if (isNull())
				{
					return null;
				}
				else
				{
					return new Number(value);
				}
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
				else
				{
					return getValue().ToString();
				}
		}

		public override bool Equals(object o)
		{
			if (!(o is BasicBoolean) || o == null)
			{
				return false;
			}
			else
			{
				return value == ((BasicBoolean)o).value;
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

		public virtual int CompareTo(BasicBoolean o)
		{
            return value.CompareTo(o.value);
        }
	}

}