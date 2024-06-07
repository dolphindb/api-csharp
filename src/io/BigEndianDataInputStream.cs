using System;
using System.IO;

namespace dolphindb.io
{


    public class BigEndianDataInputStream : AbstractExtendedDataInputStream
    {

        public BigEndianDataInputStream(Stream @in) : base(@in)
        {

        }

        public override bool isLittleEndian()
        {
            return false;
        }

        public override int readInt()
        {
            byte b1 = readAndCheckByte();
            byte b2 = readAndCheckByte();
            byte b3 = readAndCheckByte();
            byte b4 = readAndCheckByte();
            return fromBytes(b1, b2, b3, b4);

        }

        public override long readLong()
        {
            byte b1 = readAndCheckByte();
            byte b2 = readAndCheckByte();
            byte b3 = readAndCheckByte();
            byte b4 = readAndCheckByte();
            byte b5 = readAndCheckByte();
            byte b6 = readAndCheckByte();
            byte b7 = readAndCheckByte();
            byte b8 = readAndCheckByte();
            return fromBytes(b1, b2, b3, b4, b5, b6, b7, b8);
        }

        public override Long2 readLong2()
        {

            long high = readLong();
            long low = readLong();
		    return new Long2(high, low);
        }

        public override int readUnsignedShort()
        {
            byte b1 = readAndCheckByte();
            byte b2 = readAndCheckByte();
            return fromBytes(b1, b2, (byte)0, (byte)0);
        }
    }

}