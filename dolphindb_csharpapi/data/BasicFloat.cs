using dolphindb.io;
using System;

namespace dolphindb.data
{
	/// 
	/// <summary>
	/// Corresponds to DolphinDB float scalar
	/// 
	/// </summary>

	public class BasicFloat : AbstractScalar, IComparable<BasicFloat>
	{
        private readonly string df1 = "0.######";
        private readonly string df2 = "0.######E0";
        private float value;

		public BasicFloat(float value)
		{
			this.value = value;
		}

		public BasicFloat(ExtendedDataInput @in)
		{
			value = @in.readFloat();
		}

		public virtual float getValue()
		{
			return value;
		}

		public override bool isNull()
		{
			return value == -float.MaxValue;
		}

		public override void setNull()
		{
			value = -float.MaxValue;
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.FLOATING;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_FLOAT;
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
				else if (float.IsNaN(value) || float.IsInfinity(value))
				{
					return value.ToString();
				}
				else
				{
					float absVal = Math.Abs(value);
					if ((absVal > 0 && absVal <= 0.000001) || absVal >= 1000000.0)
					{
						return value.ToString(df2);
					}
					else
					{
						return value.ToString(df1);
					}
				}
		}

		public override bool Equals(object o)
		{
			if (!(o is BasicFloat) || o == null)
			{
				return false;
			}
			else
			{
				return value == ((BasicFloat)o).value;
			}
		}

		public override int GetHashCode()
		{
			return (new float?(value)).GetHashCode();
		}

        protected override void writeScalarToOutputStream(ExtendedDataOutput @out)
        {
            @out.writeFloat(value);
        }

        public virtual int CompareTo(BasicFloat o)
		{
            return value.CompareTo(o.value);
        }
	}

}