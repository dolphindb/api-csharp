using dolphindb.data;
using dolphindb.io;
using System;

namespace dolphindb.data
{
	/// 
	/// <summary>
	/// Corresponds to DolphinDB double scalar
	/// 
	/// </summary>

	public class BasicDouble : AbstractScalar, IComparable<BasicDouble>
	{
		private readonly string df1 = "0.######";
		private readonly string df2 = "0.######E0";
		private double value;

		public BasicDouble(double value)
		{
			this.value = value;
		}

		public BasicDouble(ExtendedDataInput @in)
		{
			value = @in.readDouble();
		}

		public virtual double getValue()
		{
			return value;
		}

		public override bool isNull()
		{
			return value == -double.MaxValue;
		}

		public override void setNull()
		{
			value = -double.MaxValue;
		}

		public override DATA_CATEGORY getDataCategory()
		{
				return DATA_CATEGORY.FLOATING;
		}

		public override DATA_TYPE getDataType()
		{
				return DATA_TYPE.DT_DOUBLE;
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
				else if (double.IsNaN(value) || double.IsInfinity(value))
				{
					return value.ToString();
				}
				else
				{
					double absVal = Math.Abs(value);
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
			if (!(o is BasicDouble) || o == null)
			{
				return false;
			}
			else
			{
				return value == ((BasicDouble)o).value;
			}
		}

		public override int GetHashCode()
		{
			return (new double?(value)).GetHashCode();
		}

		protected override void writeScalarToOutputStream(ExtendedDataOutput @out)
		{
			@out.writeDouble(value);
		}

		public virtual int CompareTo(BasicDouble o)
		{
			return value.CompareTo(o.value);
		}
	}

}