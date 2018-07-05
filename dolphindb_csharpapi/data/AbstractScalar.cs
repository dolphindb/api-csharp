namespace dolphindb.data
{
    using System;
    using dolphindb.io;

    public abstract class AbstractScalar : AbstractEntity, IScalar
	{
        protected abstract void writeScalarToOutputStream(ExtendedDataOutput @out);

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
	    	@out.writeInt(flag);
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
    }
}