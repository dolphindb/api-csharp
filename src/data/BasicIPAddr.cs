using dolphindb.io;
using System;
using System.Text;

namespace dolphindb.data
{
    public class BasicIPAddr : BasicInt128
    {


        public BasicIPAddr(long high, long low) : base(high, low)
        {

        }


        public BasicIPAddr(ExtendedDataInput @in) : base(@in)
        {
        }


        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_IPADDR;
        }


        public override String getString()
        {
            return getString(value);
        }

        public static String getString(Long2 value)
        {
            if (value.high == 0 && (value.low >> 32) == 0)
            {
                //ip4
                long ip4 = value.low;
                return String.Format("{0}.{1}.{2}.{3}", ip4 >> 24, (ip4 >> 16) & 0xff, (ip4 >> 8) & 0xff, ip4 & 0xff);
            }
            else
            {
                //ip6
                StringBuilder sb = new StringBuilder();
                bool consecutiveZeros = false;
                int[] data = new int[8];
                data[0] = (int)(value.high >> 48);
                data[1] = (int)((value.high >> 32) & 0xffff);
                data[2] = (int)((value.high >> 16) & 0xffff);
                data[3] = (int)(value.high & 0xffff);
                data[4] = (int)(value.low >> 48);
                data[5] = (int)((value.low >> 32) & 0xffff);
                data[6] = (int)((value.low >> 16) & 0xffff);
                data[7] = (int)(value.low & 0xffff);
                for (int i = 0; i < 8; ++i)
                {
                    if (i > 0 && i < 6 && !consecutiveZeros && data[i] == 0 && data[i + 1] == 0)
                    {
                        //consecutive zeros
                        consecutiveZeros = true;
                        ++i;
                        while (i < 6 && data[i + 1] == 0) ++i;
                    }
                    else
                    {
                        //swap two bytes
                        sb.Append(String.Format("{0:x}", (short)data[i]));
                    }
                    sb.Append(':');
                }
                return sb.ToString().Substring(0, sb.Length - 1);
            }
        }

        public static new BasicIPAddr fromString(String name)
        {
            if (name.Length < 7)
                return null;
            for (int i = 0; i < 4; ++i)
            {
                if (name.Substring(i, 1) == ".")
                    return parseIP4(name);
            }
            return parseIP6(name);
        }

        public static BasicIPAddr parseIP4(String str)
        {
            int byteIndex = 0;
            long curByte = 0;
            int len = str.Length;
            long low = 0;
            for (int i = 0; i <= len; ++i)
            {
                if (i == len || str.Substring(i, 1) == ".")
                {
                    if (curByte < 0 || curByte > 255 || byteIndex > 3)
                        return null;
                    low += curByte << ((3 - byteIndex) * 8);
                    byteIndex++;
                    curByte = 0;
                    continue;
                }
                char ch = Convert.ToChar(str.Substring(i, 1));
                if (ch < '0' || ch > '9')
                    return null;
                curByte = curByte * 10 + ch - 48;
            }
            if (byteIndex != 4)
                return null;
            return new BasicIPAddr(0, low);
        }

        public static BasicIPAddr parseIP6(String str)
        {
            int byteIndex = 0;
            int curByte = 0;
            int len = str.Length;
            int lastColonPos = -1;
            byte[] buf = new byte[16];

            for (int i = 0; i <= len; ++i)
            {
                if (i == len || str.Substring(i, 1) == ":")
                {
                    //check consecutive colon
                    if (lastColonPos == (int)i - 1)
                    {
                        //check how many colons in the remaining string
                        int colons = byteIndex / 2;
                        for (int k = i + 1; k < len; ++k)
                            if (str.Substring(k, 1) == ":")
                                ++colons;
                        int consecutiveZeros = (7 - colons) * 2;
                        for (int k = 0; k < consecutiveZeros; ++k)
                            buf[byteIndex++] = 0;
                    }
                    else
                    {
                        if (curByte < 0 || curByte > 65535 || byteIndex > 15)
                            return null;
                        buf[byteIndex++] = (byte)(curByte >> 8);
                        buf[byteIndex++] = (byte)(curByte & 255);
                        curByte = 0;
                    }
                    lastColonPos = i;
                    continue;
                }
                char ch = Convert.ToChar(str.Substring(i, 1));
                char value = (char)(ch >= 97 ? ch - 87 : (ch >= 65 ? ch - 55 : ch - 48));
                if (value < 0 || value > 15)
                    return null;
                curByte = (curByte << 4) + value;
            }
            if (byteIndex == 16)
            {
                long low = (buf[8] & 0xFFL) << 56 | (buf[9] & 0xFFL) << 48 | (buf[10] & 0xFFL) << 40 | (buf[11] & 0xFFL) << 32
                 | (buf[12] & 0xFFL) << 24 | (buf[13] & 0xFFL) << 16 | (buf[14] & 0xFFL) << 8 | (buf[15] & 0xFFL);

                long high = (buf[0] & 0xFFL) << 56 | (buf[1] & 0xFFL) << 48 | (buf[2] & 0xFFL) << 40 | (buf[3] & 0xFFL) << 32
                         | (buf[4] & 0xFFL) << 24 | (buf[5] & 0xFFL) << 16 | (buf[6] & 0xFFL) << 8 | (buf[7] & 0xFFL);
                return new BasicIPAddr(high, low);
            }
            else
                return null;
        }
    }

}
