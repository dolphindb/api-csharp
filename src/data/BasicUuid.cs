using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.data
{
    public class BasicUuid : BasicInt128
    {


        public BasicUuid(long high, long low) : base(high, low)
        {
        }


        internal BasicUuid(ExtendedDataInput @in) : base(@in)
        {
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_UUID;
        }

        public override String getString()
        {
            if(isNull())
            {
                return "";
            }else{
                ulong c = (ulong)0xffffffff << 32;
                //ulong a = 0xffffffff;
                ulong t = (ulong)value.high & c;
                ulong uHigh = (ulong)value.high;
                ulong uLow = (ulong)value.low;
                byte[] buffer = new byte[8];
                for (int i = 0; i < 8; ++i)
                {
                    buffer[i] = (byte)(uLow >> ((7 - i) * 8));
                }
                return new Guid((int)(uHigh >> 32 & 0xffffffff), (short)(uHigh >> 16 & 0xffff),
                    (short)(uHigh & 0xffff), buffer).ToString();
            }
        }

        public static new BasicUuid fromString(String name)
        {
            String[] components = name.Split('-');
            if (components.Length != 5)
                throw new ArgumentException("Invalid UUID string: " + name);

            long mostSigBits = Convert.ToInt64(components[0], 16);
            mostSigBits <<= 16;
            mostSigBits |= Convert.ToInt64(components[1], 16);
            mostSigBits <<= 16;
            mostSigBits |= Convert.ToInt64(components[2], 16);

            long leastSigBits = Convert.ToInt64(components[3], 16);
            leastSigBits <<= 48;
            leastSigBits |= Convert.ToInt64(components[4], 16);

            return new BasicUuid(mostSigBits, leastSigBits);
        }
    }

}
