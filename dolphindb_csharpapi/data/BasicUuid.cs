using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.data
{
     public class BasicUuid : BasicInt128
    {


    public BasicUuid(long high, long low):base(high, low)
    {
    }


    internal BasicUuid(ExtendedDataInput @in):base(@in)
    {
    }

    public override DATA_TYPE getDataType()
    {
        return DATA_TYPE.DT_UUID;
    }

    public override String getString()
    {
            long mostSigBits = value.high;
            long leastSigBits = value.low;
            byte[] hb = BitConverter.GetBytes(value.high);
            int a = BitConverter.ToInt32(hb, 0);
            short b = BitConverter.ToInt16(hb, 4);
            short c = BitConverter.ToInt16(hb, 6);
            byte[] lb = BitConverter.GetBytes(value.low);

            Guid uuid = new Guid(a,b,c,lb);

            return uuid.ToString();
    }

    public static BasicUuid fromString(String name)
    {
            String[] components = name.Split('-');
            if (components.Length != 5)
                throw new ArgumentException("Invalid UUID string: " + name);

            long mostSigBits = Convert.ToInt64(components[0],16);
            mostSigBits <<= 16;
            mostSigBits |= Convert.ToInt64(components[1],16);
            mostSigBits <<= 16;
            mostSigBits |= Convert.ToInt64(components[2],16);

            long leastSigBits = Convert.ToInt64(components[3],16);
            leastSigBits <<= 48;
            leastSigBits |= Convert.ToInt64(components[4],16);

            return new BasicUuid(mostSigBits, leastSigBits);
    }
}

}
