
using dolphindb.io;
using dolphindb.data;
using System.Collections.Generic;
using System;

public class BasicComplexVector : BasicDouble2Vector
{
    public BasicComplexVector(int size) : this(DATA_FORM.DF_VECTOR, size) { }
    public BasicComplexVector(List<Double2> list) : base(list) { }

    public BasicComplexVector(Double2[] array) : base(array) { }
    internal BasicComplexVector(DATA_FORM df, int size) : base(df, size) { }

    internal BasicComplexVector(DATA_FORM df, ExtendedDataInput @in) : base(df, @in) { }

    public override IScalar get(int index)
    {
        return new BasicComplex(values[index].x, values[index].y);
    }

    public override void set(int index, IScalar value)
    {
        if (value.getDataType() == DATA_TYPE.DT_COMPLEX)
        {
            values[index] = new Double2(((BasicComplex)value).getReal(), ((BasicComplex)value).getImage());
        }
        else
            throw new Exception("The value must be a complex scalar. ");
    }

    public void setComplex(int index, double real, double image)
    {
        values[index] = new Double2(real, image);
    }

    public override DATA_TYPE getDataType()
    {
        return DATA_TYPE.DT_COMPLEX;
    }


    public override Type getElementClass()
    {
        return typeof(BasicComplex);
    }

    public override IVector getSubVector(int[] indices)
    {
        int length = indices.Length;
        Double2[] sub = new Double2[length];
        for (int i = 0; i < length; ++i)
            sub[i] = values[indices[i]];
        return new BasicComplexVector(sub);
    }

    public override void append(IScalar value)
    {
        values.Add(((BasicComplex)value).getValue());
    }

    public override void append(IVector value)
    {
        values.AddRange(((BasicComplexVector)value).getDataArray());
    }
}
