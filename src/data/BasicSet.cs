using dolphindb.io;
using System;
using System.Collections.Generic;
using System.IO;
using System.Data;

namespace dolphindb.data
{

    public class BasicSet : AbstractEntity, ISet
    {
        private ISet<IScalar> set;
        private DATA_TYPE keyType;


        public BasicSet(DATA_TYPE keyType, ExtendedDataInput @in)
        {
            this.keyType = keyType;

            BasicEntityFactory factory = new BasicEntityFactory();
            DATA_TYPE[] types = Enum.GetValues(typeof(DATA_TYPE)) as DATA_TYPE[];

            //read key vector
            short flag = @in.readShort();
            int form = flag >> 8;
            int type = flag & 0xff;
            bool extended = type >= 128;
            if (type >= 128)
                type -= 128;
            if (form != (int)DATA_FORM.DF_VECTOR)
            {
                throw new IOException("The form of set keys must be vector");
            }
            if (type < 0 || type >= types.Length)
            {
                throw new IOException("Invalid key type: " + type);
            }

            IVector keys = (IVector)factory.createEntity(DATA_FORM.DF_VECTOR, types[type], @in, extended);

            int size = keys.rows();

            set = new HashSet<IScalar>();
            for (int i = 0; i < size; ++i)
            {
                set.Add(keys.get(i));
            }
        }

        public BasicSet(DATA_TYPE keyType, int capacity)
        {
            if (keyType == DATA_TYPE.DT_VOID || keyType == DATA_TYPE.DT_SYMBOL || (int)keyType >= (int)DATA_TYPE.DT_FUNCTIONDEF)
            {
                throw new System.ArgumentException("Invalid keyType: " + keyType.ToString());
            }
            this.keyType = keyType;
            set = new HashSet<IScalar>();
        }

        public BasicSet(DATA_TYPE keyType) : this(keyType, 0)
        {
        }

        public override DATA_FORM getDataForm()
        {
            return DATA_FORM.DF_SET;
        }

        public virtual DATA_CATEGORY getDataCategory()
        {
            return Utils.typeToCategory(keyType);
        }

        public virtual DATA_TYPE getDataType()
        {
            return keyType;
        }

        public virtual int rows()
        {
            return set.Count;
        }

        public virtual int columns()
        {
            return 1;
        }

        public virtual IVector keys()
        {
            return keys(set.Count);
        }

        private IVector keys(int top)
        {
            BasicEntityFactory factory = new BasicEntityFactory();
            int size = Math.Min(top, set.Count);
            IVector keys = (IVector)factory.createVectorWithDefaultValue(keyType, size);
            IEnumerator<IScalar> it = set.GetEnumerator();
            int count = 0;
            try
            {
                while (count < size)
                {
                    keys.set(count++, it.Current);
                    it.MoveNext();
                }
            }
            catch (Exception)
            {
                return null;
            }
            return keys;
        }

        public virtual bool contains(IScalar key)
        {
            return set.Contains(key);
        }

        public virtual bool add(IScalar key)
        {
            return set.Add(key);
        }

        public virtual string getString()
        {
            return keys(Vector_Fields.DISPLAY_ROWS).getString();
        }

        public virtual void write(ExtendedDataOutput @out)
        {
            IVector _keys = keys();
            int flag = ((int)DATA_FORM.DF_SET << 8) + (int)getDataType();
            @out.writeShort(flag);
            _keys.write(@out);
        }

        public DataTable toDataTable()
        {
            DataTable dt = buildTable();

            foreach (IScalar item in set)
            {
                DataRow dr = dt.NewRow();
                dr["set_key"] = item.getObject();
                dt.Rows.Add(dr);
            }
            return dt;
        }
        private DataTable buildTable()
        {
            DataTable dt = new DataTable();

            DataColumn dc = new DataColumn("set_key", Utils.getSystemType(keyType));
            dt.Columns.Add(dc);

            return dt;
        }
        public object getObject()
        {
            throw new NotImplementedException();
        }

        public void writeCompressed(ExtendedDataOutput output)
        {
            throw new NotImplementedException();
        }
    }

}