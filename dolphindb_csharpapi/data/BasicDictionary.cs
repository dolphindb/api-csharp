using System;
using System.Collections.Generic;
using System.Text;

namespace dolphindb.data
{
    
	public class BasicDictionary : AbstractEntity, Dictionary
	{
		private IDictionary<Scalar, Entity> dict;
		private DATA_TYPE keyType;
		private DATA_TYPE valueType;


		public BasicDictionary(DATA_TYPE valueType, ExtendedDataInput @in)
		{
			this.valueType = valueType;

			BasicEntityFactory factory = new BasicEntityFactory();
			DATA_TYPE[] types = Enum.GetValues(typeof(DATA_TYPE));

			//read key vector
			short flag = @in.readShort();
			int form = flag >> 8;
			int type = flag & 0xff;
			if (form != (int)DATA_FORM.DF_VECTOR)
			{
				throw new IOException("The form of dictionary keys must be vector");
			}
			if (type < 0 || type >= types.Length)
			{
				throw new IOException("Invalid key type: " + type);
			}
			keyType = types[type];
			Vector keys = (Vector)factory.createEntity(DATA_FORM.DF_VECTOR, types[type], @in);

			//read value vector
			flag = @in.readShort();
			form = flag >> 8;
			type = flag & 0xff;
			if (form != (int)DATA_FORM.DF_VECTOR)
			{
				throw new IOException("The form of dictionary values must be vector");
			}
			if (type < 0 || type >= types.Length)
			{
				throw new IOException("Invalid value type: " + type);
			}

			Vector values = (Vector)factory.createEntity(DATA_FORM.DF_VECTOR, types[type], @in);

			if (keys.rows() != values.rows())
			{
				throw new IOException("The key size doesn't equate to value size.");
			}

			int size = keys.rows();
			int capacity = (int)(size / 0.75);
			dict = new Dictionary<Scalar, Entity>(capacity);
			if (values.DataType == DATA_TYPE.DT_ANY)
			{
				BasicAnyVector entityValues = (BasicAnyVector)values;
				for (int i = 0; i < size; ++i)
				{
					dict[keys.get(i)] = entityValues.getEntity(i);
				}
			}
			else
			{
				for (int i = 0; i < size; ++i)
				{
					dict[keys.get(i)] = values.get(i);
				}
			}
		}

		public BasicDictionary(DATA_TYPE keyType, DATA_TYPE valueType, int capacity)
		{
			if (keyType == DATA_TYPE.DT_VOID || keyType == DATA_TYPE.DT_ANY || keyType == DATA_TYPE.DT_DICTIONARY)
			{
				throw new System.ArgumentException("Invalid keyType: " + keyType.name());
			}
			this.keyType = keyType;
			this.valueType = valueType;
			dict = new Dictionary<Scalar, Entity>();
		}

		public BasicDictionary(DATA_TYPE keyType, DATA_TYPE valueType) : this(keyType, valueType, 0)
		{
		}

		public override DATA_FORM DataForm
		{
			get
			{
				return DATA_FORM.DF_DICTIONARY;
			}
		}

		public virtual DATA_CATEGORY DataCategory
		{
			get
			{
				return getDataCategory(valueType);
			}
		}

		public virtual DATA_TYPE DataType
		{
			get
			{
				return valueType;
			}
		}

		public virtual int rows()
		{
			return dict.Count;
		}

		public virtual int columns()
		{
			return 1;
		}

		public virtual DATA_TYPE KeyDataType
		{
			get
			{
				return keyType;
			}
		}

		public virtual Entity get(Scalar key)
		{
			return dict[key];
		}

		public virtual bool put(Scalar key, Entity value)
		{
			if (key.DataType != KeyDataType || (value.DataType != DataType))
			{
				return false;
			}
			else
			{
				dict[key] = value;
				return true;
			}
		}

		public virtual ISet<Scalar> keys()
		{
			return dict.Keys;
		}

		public virtual ICollection<Entity> values()
		{
			return dict.Values;
		}

		public virtual ISet<KeyValuePair<Scalar, Entity>> entrySet()
		{
			return dict.SetOfKeyValuePairs();
		}

		public virtual string String
		{
			get
			{
				if (valueType == DATA_TYPE.DT_ANY)
				{
					StringBuilder content = new StringBuilder();
					int count = 0;
					ISet<KeyValuePair<Scalar, Entity>> entries = dict.SetOfKeyValuePairs();
					ISet<KeyValuePair<Scalar, Entity>>.Enumerator it = entries.GetEnumerator();
					while (it.MoveNext() && count < 20)
					{
						KeyValuePair<Scalar, Entity> entry = it.Current;
						content.Append(entry.Key.String);
						content.Append("->");
						DATA_FORM form = entry.Value.DataForm;
						if (form == DATA_FORM.DF_MATRIX || form == DATA_FORM.DF_TABLE)
						{
							content.Append("\n");
						}
						else if (form == DATA_FORM.DF_DICTIONARY)
						{
							content.Append("{\n");
						}
						content.Append(entry.Value.String);
						if (form == DATA_FORM.DF_DICTIONARY)
						{
							content.Append("}");
						}
						content.Append("\n");
						++count;
					}
	                //todo: Java iterators are only converted within the context of 'while' and 'for' loops:
					if (it.hasNext())
					{
						content.Append("...\n");
					}
					return content.ToString();
				}
				else
				{
					StringBuilder sbKeys = new StringBuilder("{");
					StringBuilder sbValues = new StringBuilder("{");
					ISet<KeyValuePair<Scalar, Entity>> entries = dict.SetOfKeyValuePairs();
					ISet<KeyValuePair<Scalar, Entity>>.Enumerator it = entries.GetEnumerator();
                    //todo: Java iterators are only converted within the context of 'while' and 'for' loops:
                    if (it.hasNext())
					{
                        //todo: Java iterators are only converted within the context of 'while' and 'for' loops:
                        KeyValuePair<Scalar, Entity> entry = it.next();
						sbKeys.Append(entry.Key.String);
						sbValues.Append(entry.Value.String);
					}
					int count = 1;
					while (it.MoveNext() && count < 20)
					{
						KeyValuePair<Scalar, Entity> entry = it.Current;
						sbKeys.Append(',');
						sbKeys.Append(entry.Key.String);
						sbValues.Append(',');
						sbValues.Append(entry.Value.String);
						++count;
					}
                    //todo: Java iterators are only converted within the context of 'while' and 'for' loops:
                    if (it.hasNext())
					{
						sbKeys.Append("...");
						sbValues.Append("...");
					}
					sbKeys.Append("}");
					sbValues.Append("}");
					return sbKeys.ToString() + "->" + sbValues.ToString();
				}
			}
		}

		public virtual void write(ExtendedDataOutput @out)
		{
			if (valueType == DATA_TYPE.DT_DICTIONARY)
			{
				throw new IOException("Can't streamlize the dictionary with value type " + valueType.name());
			}

			BasicEntityFactory factory = new BasicEntityFactory();
			Vector keys = (Vector)factory.createVectorWithDefaultValue(keyType, dict.Count);
			Vector values = (Vector)factory.createVectorWithDefaultValue(valueType, dict.Count);
			int index = 0;
			try
			{
				foreach (KeyValuePair<Scalar, Entity> entry in dict.SetOfKeyValuePairs())
				{
					keys.set(index, entry.Key);
					values.set(index, (Scalar)entry.Value);
					++index;
				}
			}
			catch (Exception ex)
			{
				throw new IOException(ex.Message);
			}

			int flag = ((int)DATA_FORM.DF_DICTIONARY << 8) + DataType.ordinal();
			@out.writeShort(flag);

			keys.write(@out);
			values.write(@out);
		}
	}

}