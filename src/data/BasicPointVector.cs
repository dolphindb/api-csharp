
using dolphindb.io;
using dolphindb.data;
using System.Collections.Generic;
using System;

public class BasicPointVector : BasicDouble2Vector
{
    public BasicPointVector(int size) : this(DATA_FORM.DF_VECTOR, size) { }
    public BasicPointVector(List<Double2> list) : base(list) { }

    public BasicPointVector(Double2[] array) : base(array) { }
    internal BasicPointVector(DATA_FORM df, int size) : base(df, size) { }

    internal BasicPointVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in) { }

    public override IScalar get(int index)
    {
        return new BasicPoint(values[index].x, values[index].y);
    }

    public override void set(int index, IScalar value)
    {
        if (value.getDataType() == DATA_TYPE.DT_POINT)
        {
            values[index] = new Double2(((BasicPoint)value).getX(), ((BasicPoint)value).getY());
        }
        else
            throw new Exception("The value must be a point scalar. ");
    }

    public void setPoint(int index, double x, double y)
    {
        values[index] = new Double2(x, y);
    }

    public override DATA_TYPE getDataType()
    {
        return DATA_TYPE.DT_POINT;
    }

    public override Type getElementClass()
    {
        return typeof(BasicPoint);
    }

    public override IVector getSubVector(int[] indices)
    {
        int length = indices.Length;
        Double2[] sub = new Double2[length];
        for (int i = 0; i < length; ++i)
            sub[i] = values[indices[i]];
        return new BasicPointVector(sub);
    }

    public override void append(IScalar value)
    {
        values.Add(((BasicPoint)value).getValue());
    }

    public override void append(IVector value)
    {
        values.AddRange(((BasicPointVector)value).getDataArray());
    }
}
