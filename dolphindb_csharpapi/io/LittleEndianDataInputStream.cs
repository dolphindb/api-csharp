using System.IO;
namespace dolphindb.io
{


	public class LittleEndianDataInputStream : AbstractExtendedDataInputStream
	{

		public LittleEndianDataInputStream(Stream @in) : base(@in)
		{
		}

		public override int readInt()
		{
			byte b1 = readAndCheckByte();
			byte b2 = readAndCheckByte();
			byte b3 = readAndCheckByte();
			byte b4 = readAndCheckByte();
			return fromBytes(b4, b3, b2, b1);
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
			return fromBytes(b8, b7, b6, b5, b4, b3, b2, b1);
		}

		public override int readUnsignedShort()
		{
			byte b1 = readAndCheckByte();
			byte b2 = readAndCheckByte();
			return fromBytes((byte)0, (byte)0, b2, b1);
		}
	}

}