using System;
using dolphindb.io;

namespace dolphindb.data
{

	public class BasicShort : AbstractScalar, IComparable<BasicShort>
	{
		private short value;

		public BasicShort(short value)
		{
			this.value = value;
		}

		public BasicShort(ExtendedDataInput @in)
		{
			value = @in.readShort();
		}

		public virtual short getValue()
		{
			return value;
		}

		public override bool isNull()
		{
			return value == short.MinValue;
		}

		public override void setNull()
		{
			value = short.MinValue;
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.INTEGRAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_SHORT;
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
				return value.ToString();
			}
		}

		public override bool Equals(object o)
		{
			if (!(o is BasicShort) || o == null)
			{
				return false;
			}
			else
			{
				return value == ((BasicShort)o).value;
			}
		}

		public override int GetHashCode()
		{
			return (new short?(value)).GetHashCode();
		}

		protected override void writeScalarToOutputStream(ExtendedDataOutput @out)
		{
			@out.writeShort(value);
		}

		public virtual int CompareTo(BasicShort o)
		{
            return value.CompareTo(o.value);
        }
	}

}