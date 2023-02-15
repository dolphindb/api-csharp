
using dolphindb.io;
using dolphindb.data;
using System.Collections.Generic;
using System;

public class BasicInt128Vector : AbstractVector
{

    protected List<Long2> values;

    public BasicInt128Vector(int size) : this(DATA_FORM.DF_VECTOR, size)
    {   
    }

public BasicInt128Vector(List<Long2> list): base(DATA_FORM.DF_VECTOR)
{
    if (list != null)
    {
        values = new List<Long2>();
        values.AddRange(new Long2[list.Count]);
        for (int i = 0; i < list.Count; ++i)
            values[i] = list[i];
    }
}

public BasicInt128Vector(Long2[] array) :    base(DATA_FORM.DF_VECTOR)
{
        values = new List<Long2>();
        values.AddRange(array);
}

internal BasicInt128Vector(DATA_FORM df, int size) : base(df)
{

        values = new List<Long2>();
        values.AddRange(new Long2[size]);
    for (int i = 0; i < size; ++i)
        values[i] = new Long2(0, 0);
}

    internal BasicInt128Vector(DATA_FORM df, ExtendedDataInput @in):base(df)
{
		int rows = @in.readInt();
		int cols = @in.readInt(); 
		int size = rows * cols;
        values = new List<Long2>();
        values.AddRange(new Long2[size]);
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
    if (value.getDataType() == DATA_TYPE.DT_INT128)
    {
        values[index].high = ((BasicInt128)value).getMostSignicantBits();
        values[index].low = ((BasicInt128)value).getLeastSignicantBits();
    }
    else
        throw new Exception("The value must be a int128 scalar. ");
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
    return values.Count;
}

    protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
    {
		    @out.writeLong2Array(values.ToArray());
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

    public override int hashBucket(int index, int buckets)
    {
        return values[index].hashBucket(buckets);
    }


    public override IVector getSubVector(int[] indices)
    {
        int length = indices.Length;
        Long2[] sub = new Long2[length];
        for (int i = 0; i < length; ++i)
            sub[i] = values[indices[i]];
        return new BasicInt128Vector(sub);
    }

    protected Long2[] getSubArray(int[] indices)
    {
        int length = indices.Length;
        Long2[] sub = new Long2[length];
        for (int i = 0; i < length; ++i)
            sub[i] = values[indices[i]];
        return sub;
    }

    public override int asof(IScalar value)
    {
        throw new Exception("BasicInt128Vector.asof not supported.");
    }

    public override void deserialize(int start, int count, ExtendedDataInput @in)
    {
        if (start + count > values.Count)
        {
            values.AddRange(new Long2[start + count - values.Count]);
        }
        int totalBytes = count * 16, off = 0;
        bool littleEndian = @in.isLittleEndian();
        while (off < totalBytes)
        {
            int len = System.Math.Min(BUF_SIZE, totalBytes - off);
            @in.readFully(buf, 0, len);
            int subStart = off / 16, end = len / 16;
            Byte[] dst = new Byte[len];
            Buffer.BlockCopy(buf, 0, dst, 0, len);
            if (!littleEndian)
                Array.Reverse(dst);
            if (littleEndian)
            {
                for (int i = 0; i < end; i++)
                {
                    long low = BitConverter.ToInt64(dst, i * 16);
                    long high = BitConverter.ToInt64(dst, i * 16 + 8);
                    values[i + subStart + start] = new Long2(high, low);
                }
            }
            else
            {
                for (int i = 0; i < end; i++)
                {
                    long high = BitConverter.ToInt64(dst, i * 16);
                    long low = BitConverter.ToInt64(dst, i * 16 + 8);
                    values[i + subStart + start] = new Long2(high, low);
                }
            }
            off += len;
        }
    }

    public override void serialize(int start, int count, ExtendedDataOutput @out)
    {
        Long2[] buffer = new Long2[count];
        for(int i = 0; i < count; ++i)
        {
            buffer[i] = values[i + start];
        }
        @out.writeLong2Array(buffer);
    }


    public override int getUnitLength()
    {
        return 16;
    }

    public override int serialize(int indexStart, int offect, int targetNumElement, out int numElement, out int partial, ByteBuffer @out)
    {
        targetNumElement = Math.Min((@out.remain() / getUnitLength()), targetNumElement);
        if (@out.isLittleEndian)
        {
            for (int i = 0; i < targetNumElement; ++i)
            {
                @out.WriteLong(values[indexStart + i].low);
                @out.WriteLong(values[indexStart + i].high);
            }
        }
        else
        {
            for (int i = 0; i < targetNumElement; ++i)
            {
                @out.WriteLong(values[indexStart + i].high);
                @out.WriteLong(values[indexStart + i].low);
            }
        }
        numElement = targetNumElement;
        partial = 0;
        return targetNumElement * 16;
    }

    public override void append(IScalar value)
    {
        values.Add(((BasicInt128)value).getValue());
    }

    public override void append(IVector value)
    {
        values.AddRange(((BasicInt128Vector)value).getdataArray());
    }

    public List<Long2> getdataArray()
    {
        return values;
    }

    public override IEntity getEntity(int index)
    {
        return get(index);
    }

    public override int getExtraParamForType()
    {
        throw new NotImplementedException();
    }
}

