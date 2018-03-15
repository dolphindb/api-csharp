using System.IO;

namespace dolphindb.io
{


	public class BigEndianDataInputStream : AbstractExtendedDataInputStream
	{

		public BigEndianDataInputStream(Stream @in) : base(@in)
		{

		}

		public override int readInt()
		{
            try
            {
                byte b1 = readAndCheckByte();
                byte b2 = readAndCheckByte();
                byte b3 = readAndCheckByte();
                byte b4 = readAndCheckByte();
                return fromBytes(b1, b2, b3, b4);
            }
            catch(IOException ex)
            {
                throw ex;
            }

		}

		public override long readLong()
		{
            try
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
            catch (IOException ex)
            {
                throw ex;
            }
        }

		public override int readUnsignedShort()
		{
            try
            {
                byte b1 = readAndCheckByte();
                byte b2 = readAndCheckByte();
                return fromBytes(b1, b2, (byte)0, (byte)0);
            }
            catch (IOException ex)
            {
                throw ex;
            }
		}
	}

}