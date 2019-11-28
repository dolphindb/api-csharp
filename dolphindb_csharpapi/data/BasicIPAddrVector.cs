using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.data
{
    public class BasicIPAddrVector : BasicInt128Vector
    {


    public BasicIPAddrVector(int size):base(size)
    {
        
    }

    public BasicIPAddrVector(List<Long2> list):base(list)
    {
    }

    public BasicIPAddrVector(Long2[] array):base(array)
    {
    }

    internal BasicIPAddrVector(DATA_FORM df, int size):base(df,size)
    {
    }

    internal BasicIPAddrVector(DATA_FORM df, ExtendedDataInput @in):base(df,@in)
    {
    }

    public override IScalar get(int index)
    {
        return new BasicIPAddr(values[index].high, values[index].low);
    }

     public override DATA_TYPE getDataType()
    {
        return DATA_TYPE.DT_IPADDR;
    }

    public override Type getElementClass()
    {
            return typeof(BasicIPAddr);
    }
}

}
