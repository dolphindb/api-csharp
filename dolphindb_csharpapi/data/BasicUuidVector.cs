

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
}

}
