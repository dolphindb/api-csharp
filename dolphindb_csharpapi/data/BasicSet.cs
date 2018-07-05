using System;
using System.Collections.Generic;

namespace com.xxdb.data
{


	using ExtendedDataInput = com.xxdb.io.ExtendedDataInput;
	using ExtendedDataOutput = com.xxdb.io.ExtendedDataOutput;

	/// 
	/// <summary>
	/// Corresponds to DolphinDB set object
	/// 
	/// </summary>

	public class BasicSet : AbstractEntity, Set
	{
		private ISet<Scalar> set;
		private DATA_TYPE keyType;

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public BasicSet(Entity_DATA_TYPE keyType, com.xxdb.io.ExtendedDataInput in) throws java.io.IOException
		public BasicSet(DATA_TYPE keyType, ExtendedDataInput @in)
		{
			this.keyType = keyType;

			BasicEntityFactory factory = new BasicEntityFactory();
			DATA_TYPE[] types = Enum.GetValues(typeof(DATA_TYPE));

			//read key vector
			short flag = @in.readShort();
			int form = flag >> 8;
			int type = flag & 0xff;
			if (form != (int)DATA_FORM.DF_VECTOR)
			{
				throw new IOException("The form of set keys must be vector");
			}
			if (type < 0 || type >= types.Length)
			{
				throw new IOException("Invalid key type: " + type);
			}

			Vector keys = (Vector)factory.createEntity(DATA_FORM.DF_VECTOR, types[type], @in);

			int size = keys.rows();
			int capacity = (int)(size / 0.75);
			set = new HashSet<Scalar>(capacity);
			for (int i = 0; i < size; ++i)
			{
				set.Add(keys.get(i));
			}
		}

		public BasicSet(DATA_TYPE keyType, int capacity)
		{
			if (keyType == DATA_TYPE.DT_VOID || keyType == DATA_TYPE.DT_SYMBOL || keyType.ordinal() >= (int)DATA_TYPE.DT_FUNCTIONDEF)
			{
				throw new System.ArgumentException("Invalid keyType: " + keyType.name());
			}
			this.keyType = keyType;
			set = new HashSet<Scalar>();
		}

		public BasicSet(DATA_TYPE keyType) : this(keyType, 0)
		{
		}

		public override DATA_FORM DataForm
		{
			get
			{
				return DATA_FORM.DF_SET;
			}
		}

		public virtual DATA_CATEGORY DataCategory
		{
			get
			{
				return getDataCategory(keyType);
			}
		}

		public virtual DATA_TYPE DataType
		{
			get
			{
				return keyType;
			}
		}

		public virtual int rows()
		{
			return set.Count;
		}

		public virtual int columns()
		{
			return 1;
		}

		public virtual Vector keys()
		{
			return keys(set.Count);
		}

		private Vector keys(int top)
		{
			BasicEntityFactory factory = new BasicEntityFactory();
			int size = Math.Min(top, set.Count);
			Vector keys = (Vector)factory.createVectorWithDefaultValue(keyType, size);
			IEnumerator<Scalar> it = set.GetEnumerator();
			int count = 0;
			try
			{
				while (count < size)
				{
//JAVA TO C# CONVERTER TODO TASK: Java iterators are only converted within the context of 'while' and 'for' loops:
					keys.set(count++, it.next());
				}
			}
			catch (Exception)
			{
				return null;
			}
			return keys;
		}

		public virtual bool contains(Scalar key)
		{
			return set.Contains(key);
		}

		public virtual bool add(Scalar key)
		{
			if (key.DataType != key.DataType)
			{
				return false;
			}
			return set.Add(key);
		}

		public virtual string String
		{
			get
			{
				return keys(Vector_Fields.DISPLAY_ROWS).String;
			}
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public void write(com.xxdb.io.ExtendedDataOutput out) throws java.io.IOException
		public virtual void write(ExtendedDataOutput @out)
		{
			Vector keys = keys();
			int flag = ((int)DATA_FORM.DF_SET << 8) + DataType.ordinal();
			@out.writeShort(flag);
			keys.write(@out);
		}
	}

}