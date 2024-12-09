using System;
using dolphindb.io;
using System.Data;
namespace dolphindb.data
{
    public abstract class AbstractScalar : AbstractEntity, IScalar
    {
        public abstract void writeScalarToOutputStream(ExtendedDataOutput @out);

        public override DATA_FORM getDataForm()
        {
            return DATA_FORM.DF_SCALAR;
        }

        public int rows()
        {
            return 1;
        }

        public int columns()
        {
            return 1;
        }

        public void write(ExtendedDataOutput @out)
        {
            int flag = ((int)DATA_FORM.DF_SCALAR << 8) + (int)getDataType();
            @out.writeShort(flag);
            if( getDataCategory() == DATA_CATEGORY.DENARY)
            {
                @out.writeInt(getScale());
            }
            writeScalarToOutputStream(@out);
        }

        public string toString()
        {
            return getString();
        }

        public virtual bool isNull()
        {
            throw new NotImplementedException();
        }

        public virtual void setNull()
        {
            throw new NotImplementedException();
        }

        public virtual Number getNumber()
        {
            throw new NotImplementedException();
        }

        public virtual object getTemporal()
        {
            throw new NotImplementedException();
        }

        public virtual DATA_CATEGORY getDataCategory()
        {
            throw new NotImplementedException();
        }

        public virtual DATA_TYPE getDataType()
        {
            throw new NotImplementedException();
        }

        public abstract string getString();

        public virtual DataTable toDataTable()
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("dolphinScalar", Utils.getSystemType(getDataType()));
            DataRow dr = dt.NewRow();
            dr[0] = Utils.getRowDataTableObject(this);
            dt.Rows.Add(dr);
            return dt;
        }

        public abstract object getObject();

        public abstract void setObject(object value);

        public void writeCompressed(ExtendedDataOutput output)
        {
            throw new NotImplementedException();
        }

        public abstract int hashBucket(int buckets);

        public virtual int getScale()
        {
            throw new NotImplementedException();
        }

        public virtual int getExtraParamForType(){
            return 0;
        }
    }
}