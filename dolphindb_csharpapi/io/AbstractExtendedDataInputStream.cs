using System;
using System.IO;
using System.Text;

namespace dolphindb.io
{


	public abstract class AbstractExtendedDataInputStream : BinaryReader,ExtendedDataInput
	{

		private const int UTF8_STRING_LIMIT = 65536;
		private byte[] buf_;
        protected Stream _inStream;
		protected internal AbstractExtendedDataInputStream(Stream inStream):base(inStream)
		{
            _inStream = inStream;
        }

		public bool readBoolean()
		{
			return readUnsignedByte() != 0;
		}

		public byte readByte()
		{
			return (byte)readUnsignedByte();
		}

		public void readFully(byte[] arg0)
		{
            base.Read(arg0,0,arg0.Length);
		}

		public void readFully(byte[] arg0, int arg1, int arg2)
		{
            base.Read(arg0, arg1, arg2);
		}

        public int readUnsignedByte()
		{
            byte b1 = this.ReadByte();
            if (0 > b1)
			{
				throw new EndOfStreamException();
			}
			return b1;
		}

		public  int skipBytes(int n)
		{
			return (int)_inStream.Seek(n,SeekOrigin.Current);
		}

		protected internal virtual byte readAndCheckByte()
		{
            byte b = this.ReadByte();

            byte b1 = b;
           
            return (byte)b1;
		}

		protected internal virtual int fromBytes(byte b1, byte b2, byte b3, byte b4)
		{
			return b1 << 24 | (b2 & 0xFF) << 16 | (b3 & 0xFF) << 8 | (b4 & 0xFF);
		}

		protected internal virtual long fromBytes(byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7, byte b8)
		{
			return (b1 & 0xFFL) << 56 | (b2 & 0xFFL) << 48 | (b3 & 0xFFL) << 40 | (b4 & 0xFFL) << 32 | (b5 & 0xFFL) << 24 | (b6 & 0xFFL) << 16 | (b7 & 0xFFL) << 8 | (b8 & 0xFFL);
		}

        string ExtendedDataInput.readLine()
        {
            return readUTF8((byte)'\n');
        }

        short ExtendedDataInput.readShort()
        {
            return (short)readUnsignedShort();
        }

        int ExtendedDataInput.readUnsignedByte()
        {
            return base.ReadByte();
        }

        int ExtendedDataInput.skipBytes(int n)
        {
            throw new NotImplementedException();
        }

        public abstract int readInt();

        public abstract int readUnsignedShort();

        private String readUTF8(byte terminator)
        {
            if (buf_ == null)
                buf_ = new byte[2048];
            byte ch = readAndCheckByte();
            int count = 0;
            while (ch != terminator)
            {
                if (count == buf_.Length)
                {
                    if (count >= UTF8_STRING_LIMIT)
                        throw new IOException("UTF-8 string length exceeds the limit of 65536 bytes");
                    byte[] tmp = new byte[buf_.Length * 2];
                    Array.Copy(buf_, 0, tmp, 0, buf_.Length);
                    buf_ = tmp;
                }
                buf_[count++] = ch;
                ch = readAndCheckByte();
            }
            return Encoding.Default.GetString(buf_, 0, count);
        }

        public abstract long readLong();

        public override int Read()
        {
            int re = base.ReadByte();
            return re;
        }

        byte ExtendedDataInput.readByte()
        {
            return base.ReadByte();
        }

        public char readChar()
        {
            return base.ReadChar();
        }

        public double readDouble()
        {
            return base.ReadDouble();
        }

        public float readFloat()
        {
            byte[] b = base.ReadBytes(4);
            return BitConverter.ToSingle(b,0);
        }

        public string readString()
        {
            return readUTF8((byte)0);
        }
    }
    
}