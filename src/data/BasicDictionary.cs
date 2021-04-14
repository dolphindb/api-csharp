using dolphindb.io;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Data;

namespace dolphindb.data
{

    public class BasicDictionary : AbstractEntity, IDictionary
    {
        private IDictionary<IScalar, IEntity> dict;
        private DATA_TYPE keyType;
        private DATA_TYPE valueType;


        public BasicDictionary(DATA_TYPE valueType, ExtendedDataInput @in)
        {
            this.valueType = valueType;

            BasicEntityFactory factory = new BasicEntityFactory();
            DATA_TYPE[] types = Enum.GetValues(typeof(DATA_TYPE)) as DATA_TYPE[];

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
            IVector keys = (IVector)factory.createEntity(DATA_FORM.DF_VECTOR, types[type], @in);

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

            IVector values = (IVector)factory.createEntity(DATA_FORM.DF_VECTOR, types[type], @in);

            if (keys.rows() != values.rows())
            {
                throw new IOException("The key size doesn't equate to value size.");
            }

            int size = keys.rows();
            int capacity = (int)(size / 0.75);
            dict = new Dictionary<IScalar, IEntity>(capacity);
            if (values.getDataType() == DATA_TYPE.DT_ANY)
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
                throw new System.ArgumentException("Invalid keyType: " + keyType.ToString());
            }
            this.keyType = keyType;
            this.valueType = valueType;
            dict = new Dictionary<IScalar, IEntity>();
        }

        public BasicDictionary(DATA_TYPE keyType, DATA_TYPE valueType) : this(keyType, valueType, 0)
        {
        }

        public override DATA_FORM getDataForm()
        {
            return DATA_FORM.DF_DICTIONARY;
        }

        public virtual DATA_CATEGORY getDataCategory()
        {
            return getDataCategory(valueType);
        }

        public virtual DATA_TYPE getDataType()
        {
            return valueType;
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

        public virtual IEntity get(IScalar key)
        {
            return dict[key];
        }

        public virtual bool put(IScalar key, IEntity value)
        {
            if (key.getDataType() != KeyDataType || (value.getDataType() != getDataType()))
            {
                return false;
            }
            else
            {
                dict[key] = value;
                return true;
            }
        }

        public virtual ICollection<IScalar> keys()
        {
            return dict.Keys;
        }

        public virtual ICollection<IEntity> values()
        {
            return dict.Values;
        }

        public virtual ISet<KeyValuePair<IScalar, IEntity>> entrySet()
        {
            throw new NotImplementedException();
            //return dict.SetOfKeyValuePairs();
        }

        public virtual string String
        {
            get
            {
                if (valueType == DATA_TYPE.DT_ANY)
                {
                    StringBuilder content = new StringBuilder();
                    int count = 0;
                    IEnumerator<KeyValuePair<IScalar, IEntity>> it = dict.GetEnumerator();
                    while (it.MoveNext() && count < 20)
                    {
                        KeyValuePair<IScalar, IEntity> entry = it.Current;
                        content.Append(entry.Key.getString());
                        content.Append("->");
                        DATA_FORM form = entry.Value.getDataForm();
                        if (form == DATA_FORM.DF_MATRIX || form == DATA_FORM.DF_TABLE)
                        {
                            content.Append("\n");
                        }
                        else if (form == DATA_FORM.DF_DICTIONARY)
                        {
                            content.Append("{\n");
                        }
                        content.Append(entry.Value.getString());
                        if (form == DATA_FORM.DF_DICTIONARY)
                        {
                            content.Append("}");
                        }
                        content.Append("\n");
                        ++count;
                    }
                    //todo: Java iterators are only converted within the context of 'while' and 'for' loops:
                    if (it.MoveNext())
                    {
                        content.Append("...\n");
                    }
                    return content.ToString();
                }
                else
                {
                    StringBuilder sbKeys = new StringBuilder("{");
                    StringBuilder sbValues = new StringBuilder("{");
                    IEnumerator<KeyValuePair<IScalar, IEntity>> it = dict.GetEnumerator();
                    //todo: Java iterators are only converted within the context of 'while' and 'for' loops:
                    if (it.MoveNext())
                    {
                        //todo: Java iterators are only converted within the context of 'while' and 'for' loops:
                        KeyValuePair<IScalar, IEntity> entry = it.Current;
                        sbKeys.Append(entry.Key.getString());
                        sbValues.Append(entry.Value.getString());
                    }
                    int count = 1;
                    while (it.MoveNext() && count < 20)
                    {
                        KeyValuePair<IScalar, IEntity> entry = it.Current;
                        sbKeys.Append(',');
                        sbKeys.Append(entry.Key.getString());
                        sbValues.Append(',');
                        sbValues.Append(entry.Value.getString());
                        ++count;
                    }
                    //todo: Java iterators are only converted within the context of 'while' and 'for' loops:
                    if (it.MoveNext())
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
                throw new IOException("Can't streamlize the dictionary with value type " + valueType.ToString());
            }

            BasicEntityFactory factory = new BasicEntityFactory();
            IVector keys = (IVector)factory.createScalarWithDefaultValue(keyType);
            IVector values = (IVector)factory.createVectorWithDefaultValue(valueType, dict.Count);
            int index = 0;
            try
            {
                IEnumerator<KeyValuePair<IScalar, IEntity>> itr = dict.GetEnumerator();

                while (itr.MoveNext())
                {
                    keys.set(index, itr.Current.Key);
                    values.set(index, (IScalar)itr.Current.Value);
                    ++index;
                }
            }
            catch (Exception ex)
            {
                throw new IOException(ex.Message);
            }

            int flag = ((int)DATA_FORM.DF_DICTIONARY << 8) + (int)getDataType();
            @out.writeShort(flag);

            keys.write(@out);
            values.write(@out);
        }

        public string getString()
        {
            throw new NotImplementedException();
        }

        public DataTable toDataTable()
        {
            DataTable dt = buildTable();
            int itemCount = this.keys().Count;
            IScalar[] keys = new IScalar[itemCount];
            IEntity[] values = new IEntity[itemCount];
            this.keys().CopyTo(keys, 0);
            this.values().CopyTo(values, 0);
            if (itemCount > 0)
            {
                if (values[0].isScalar() == false)
                {
                    throw new InvalidCastException("Only scalar value supported!");
                }
            }
            for (int i = 0; i < itemCount; i++)
            {
                DataRow dr = dt.NewRow();
                dr["dict_key"] = keys[i].getObject();
                dr["dict_value"] = values[i].getObject();
                dt.Rows.Add(dr);
            }
            return dt;
        }

        private DataTable buildTable()
        {
            DataTable dt = new DataTable();

            DataColumn dc = new DataColumn("dict_key", Utils.getSystemType(keyType));
            dt.Columns.Add(dc);
            dc = new DataColumn("dict_value", Utils.getSystemType(valueType));
            dt.Columns.Add(dc);

            return dt;
        }
        public object getObject()
        {
            throw new NotImplementedException();
        }
    }

}