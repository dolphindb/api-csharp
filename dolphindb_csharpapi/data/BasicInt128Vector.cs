
using dolphindb.io;
using dolphindb.data;
using System.Collections.Generic;
using System;

public class BasicInt128Vector : AbstractVector
{

    protected Long2[] values;

    public BasicInt128Vector(int size) : this(DATA_FORM.DF_VECTOR, size)
    {   
    }

public BasicInt128Vector(List<Long2> list): base(DATA_FORM.DF_VECTOR)
{
    if (list != null)
    {
        values = new Long2[list.Count];
        for (int i = 0; i < list.Count; ++i)
            values[i] = list[i];
    }
}

public BasicInt128Vector(Long2[] array) :    base(DATA_FORM.DF_VECTOR)
{
    values = (Long2[])array.Clone();
}

internal BasicInt128Vector(DATA_FORM df, int size) : base(df)
{
   
    values = new Long2[size];
    for (int i = 0; i < size; ++i)
        values[i] = new Long2(0, 0);
}

    internal BasicInt128Vector(DATA_FORM df, ExtendedDataInput @in):base(df)
{
		int rows = @in.readInt();
		int cols = @in.readInt(); 
		int size = rows * cols;
        values = new Long2[size];
		int totalBytes = size * 16, off = 0;
        bool littleEndian = @in.isLittleEndian();
		while (off<totalBytes) {
			int len = System.Math.Min(BUF_SIZE, totalBytes - off);
			@in.readFully(buf, 0, len);
            int start = off / 16, end = len / 16;
            Byte[] dst = new Byte[len];
            Buffer.BlockCopy(buf,0, dst,0,len);
            if (!littleEndian)
                Array.Reverse(dst);
			if(littleEndian){
				for (int i = 0; i<end; i++){
					long low = BitConverter.ToInt64(dst,i * 16);
                    long high = BitConverter.ToInt64(dst,i * 16 + 8);
                    values[i + start] = new Long2(high, low);
				}
			}
			else{
				for (int i = 0; i<end; i++){
					long high = BitConverter.ToInt64(dst,i * 16);
                    long low = BitConverter.ToInt64(dst,i * 16 + 8);
                    values[i + start] = new Long2(high, low);
				}
			}
			off += len;
		}
	}
	
	public override IScalar get(int index)
{
    return new BasicInt128(values[index].high, values[index].low);
}

public override void set(int index, IScalar value)
{
    values [index].high = ((BasicInt128)value).getMostSignicantBits();
    values [index].low = ((BasicInt128)value).getLeastSignicantBits();
}

public void setInt128(int index, long highValue, long lowValue)
{
    values[index].high = highValue;
    values[index].low = lowValue;
}


    public override bool isNull(int index)
{
    return values[index].isNull();
}


    public override void setNull(int index)
{
    values[index].setNull();
}


    public override DATA_CATEGORY getDataCategory()
{
    return DATA_CATEGORY.BINARY;
}


    public override DATA_TYPE getDataType()
{
    return DATA_TYPE.DT_INT128;
}


    public override Type getElementClass()
{
    return typeof(BasicInt128);
	}

	
    public override int rows()
{
    return values.Length;
}

    protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
    {
		    @out.writeLong2Array(values);
    }

    public override void set(int index, string value)
    {
        throw new NotImplementedException();
    }

    public override void add(object value)
    {
        throw new NotImplementedException();
    }

    public override void addRange(object list)
    {
        throw new NotImplementedException();
    }
}

