

using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{
    public class BasicUuidVector : BasicInt128Vector
    {


    public BasicUuidVector(int size):base(size)
    {
    }

    public BasicUuidVector(List<Long2> list):base(list)
    {
    }

    public BasicUuidVector(Long2[] array):base(array)
    {
    }

    internal BasicUuidVector(DATA_FORM df, int size):base(df,size)
    {
    }

    internal BasicUuidVector(DATA_FORM df, ExtendedDataInput @in):base(df,@in)
    {
    }

    public override void set(int index, IScalar value)
    {
        if (value.getDataType() == DATA_TYPE.DT_UUID)
        {
            Long2 t = ((BasicInt128)value).getLong2();
            setInt128(index, t.high, t.low);
        }
        else
            throw new Exception("The value must be a uuid scalar. ");
    }

        public override IScalar get(int index)
    {
        return new BasicUuid(values[index].high, values[index].low);
    }

    public override DATA_TYPE getDataType()
    {
        return DATA_TYPE.DT_UUID;
    }
    public override Type getElementClass()
    {
        return typeof(BasicUuid);
}

    public override IVector getSubVector(int[] indices)
    {
        return new BasicUuidVector(getSubArray(indices));
    }


    }

}
