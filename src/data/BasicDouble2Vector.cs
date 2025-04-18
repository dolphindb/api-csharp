
using dolphindb.io;
using dolphindb.data;
using System.Collections.Generic;
using System;

public abstract class BasicDouble2Vector : AbstractVector
{

    protected List<Double2> values;
    internal BasicDouble2Vector(int size) : this(DATA_FORM.DF_VECTOR, size)
    {
    }

    internal BasicDouble2Vector(List<Double2> list) : base(DATA_FORM.DF_VECTOR)
    {
        if (list != null)
        {
            values = new List<Double2>();
            values.AddRange(new Double2[list.Count]);
            for (int i = 0; i < list.Count; ++i)
                values[i] = list[i];
        }
    }

    internal BasicDouble2Vector(Double2[] array) : base(DATA_FORM.DF_VECTOR)
    {
        values = new List<Double2>();
        values.AddRange(array);
    }

    internal BasicDouble2Vector(DATA_FORM df, int size) : base(df)
    {

        values = new List<Double2>();
        values.AddRange(new Double2[size]);
        for (int i = 0; i < size; ++i)
            values[i] = new Double2(-double.MaxValue, -double.MaxValue);
    }

    internal BasicDouble2Vector(DATA_FORM df, ExtendedDataInput @in) : base(df)
    {
        int rows = @in.readInt();
        int cols = @in.readInt();
        int size = rows * cols;
        values = new List<Double2>();
        values.AddRange(new Double2[size]);
        int totalBytes = size * 16, off = 0;
        bool littleEndian = @in.isLittleEndian();
        byte[] buffer = buf.Value;
        while (off < totalBytes)
        {
            int len = System.Math.Min(BUF_SIZE, totalBytes - off);
            @in.readFully(buffer, 0, len);
            int start = off / 16, end = len / 16;
            Byte[] dst = new Byte[len];
            Buffer.BlockCopy(buffer, 0, dst, 0, len);
            if (!littleEndian)
                Array.Reverse(dst);
            if (littleEndian)
            {
                for (int i = 0; i < end; i++)
                {
                    double real = BitConverter.ToDouble(dst, i * 16);
                    double image = BitConverter.ToDouble(dst, i * 16 + 8);
                    values[i + start] = new Double2(real, image);
                }
            }
            else
            {
                for (int i = 0; i < end; i++)
                {
                    double image = BitConverter.ToDouble(dst, i * 16);
                    double real = BitConverter.ToDouble(dst, i * 16 + 8);
                    values[i + start] = new Double2(real, image);
                }
            }
            off += len;
        }
    }

    public Double2 getDouble2(int index)
    {
        return values[index];
    }

    public override bool isNull(int index)
    {
        return values[index].isNull();
    }


    public override void setNull(int index)
    {
        values[index] = new Double2(-double.MaxValue, -double.MaxValue);
    }


    public override DATA_CATEGORY getDataCategory()
    {
        return DATA_CATEGORY.BINARY;
    }


    public override int rows()
    {
        return values.Count;
    }

    protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
    {
        @out.writeDouble2Array(values.ToArray());
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

    protected Double2[] getSubArray(int[] indices)
    {
        int length = indices.Length;
        Double2[] sub = new Double2[length];
        for (int i = 0; i < length; ++i)
            sub[i] = values[indices[i]];
        return sub;
    }

    public override int asof(IScalar value)
    {
        throw new Exception("BasicDouble2Vector.asof not supported.");
    }

    public override void deserialize(int start, int count, ExtendedDataInput @in)
    {
        if (start + count > values.Count)
        {
            values.AddRange(new Double2[start + count - values.Count]);
        }
        int totalBytes = count * 16, off = 0;
        bool littleEndian = @in.isLittleEndian();
        byte[] buffer = buf.Value;
        while (off < totalBytes)
        {
            int len = System.Math.Min(BUF_SIZE, totalBytes - off);
            @in.readFully(buffer, 0, len);
            int subStart = off / 16, end = len / 16;
            Byte[] dst = new Byte[len];
            Buffer.BlockCopy(buffer, 0, dst, 0, len);
            if (!littleEndian)
                Array.Reverse(dst);
            if (littleEndian)
            {
                for (int i = 0; i < end; i++)
                {
                    double real = BitConverter.ToDouble(dst, i * 16);
                    double image = BitConverter.ToDouble(dst, i * 16 + 8);
                    values[i + subStart + start] = new Double2(real, image);
                }
            }
            else
            {
                for (int i = 0; i < end; i++)
                {
                    double image = BitConverter.ToDouble(dst, i * 16);
                    double real = BitConverter.ToDouble(dst, i * 16 + 8);
                    values[i + subStart + start] = new Double2(real, image);
                }
            }
            off += len;
        }
    }

    public override void serialize(int start, int count, ExtendedDataOutput @out)
    {
        Double2[] buffer = new Double2[count];
        for (int i = 0; i < count; ++i)
        {
            buffer[i] = values[i + start];
        }
        @out.writeDouble2Array(buffer);
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
                @out.WriteDouble(values[indexStart + i].x);
                @out.WriteDouble(values[indexStart + i].y);
            }
        }
        else
        {
            for (int i = 0; i < targetNumElement; ++i)
            {
                @out.WriteDouble(values[indexStart + i].y);
                @out.WriteDouble(values[indexStart + i].x);
            }
        }
        numElement = targetNumElement;
        partial = 0;
        return targetNumElement * 16;
    }

    public List<Double2> getDataArray()
    {
        return values;
    }

    public override IEntity getEntity(int index)
    {
        return get(index);
    }
}
