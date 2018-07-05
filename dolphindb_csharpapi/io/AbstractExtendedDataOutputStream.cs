using System;
using System.IO;
using System.Text;

namespace dolphindb.io
{


	public abstract class AbstractExtendedDataOutputStream : StreamWriter,ExtendedDataOutput
	{
		public abstract void writeLongArray(long[] A, int startIdx, int len);
		public abstract void writeIntArray(int[] A, int startIdx, int len);
		public abstract void writeShortArray(short[] A, int startIdx, int len);
		private const int UTF8_STRING_LIMIT = 65535;
		protected internal const int BUF_SIZE = 4096;
		protected internal sbyte[] buf;
		private static readonly int longBufSize = BUF_SIZE / 8;
		private static readonly int intBufSize = BUF_SIZE / 4;
		private int[] intBuf;
		private long[] longBuf;
        protected Stream _outStream;
        public AbstractExtendedDataOutputStream(Stream outStream):base(outStream)
		{
            _outStream = outStream;
        }

		public void flush()
		{
            try
            {
                base.Flush();
            }
            catch (IOException ex)
            {
                throw ex;
            }
           
        }
        public void write(int v)
        {
            try
            {
                byte[] b = BitConverter.GetBytes(v);
                _outStream.Write(b, 0, b.Length);
            }
            catch (IOException ex)
            {
                throw ex;
            }

        }

        public void write(byte[] b,int offset,int len)
        {
            try
            {
                _outStream.Write(b, offset, len);
            }
            catch (IOException ex)
            {
                throw ex;
            }
        }
        

        public void writeBoolean(bool v)
		{
            try
            {
                byte[] b = BitConverter.GetBytes(v);
                _outStream.Write(b,0,b.Length);
            }
            catch (IOException ex)
            {
                throw ex;
            }
            
		}

		public void writeByte(int v)
		{
            try
            {
                base.Write(v);
            }
            catch (IOException ex)
            {
                throw ex;
            }
            
		}

		public void writeChar(char v)
		{
            try
            {
                base.Write(v);
            }
            catch (IOException ex)
            {
                throw ex;
            }
		}

		public void writeFloat(float v)
		{
            try
            {
                byte[] b = BitConverter.GetBytes(v);
                _outStream.Write(b, 0, b.Length);
            }
            catch (IOException ex)
            {
                throw ex;
            }
		}

		public void writeDouble(double v)
		{
            try
            {
                byte[] b = BitConverter.GetBytes(v);
                _outStream.Write(b, 0, b.Length);

            }
            catch (IOException ex)
            {
                throw ex;
            }
		}

		public void writeBytes(string s)
		{
            try
            {
                base.Write(s);
            }
            catch (IOException ex)
            {
                throw ex;
            }
		}

		public  void writeChars(string s)
		{
            try
            {
                byte[] b = Encoding.Default.GetBytes(s);
                _outStream.Write(b, 0, b.Length);
            }
            catch (IOException ex)
            {
                throw ex;
            }
		}

		public void writeUTF(string value)
		{
            try
            {
                byte[] b = Encoding.UTF8.GetBytes(value);
                _outStream.Write(b, 0, b.Length);
            }
            catch (IOException ex)
            {
                throw ex;
            }
		}

		public void writeString(string value)
		{
            try
            {
                byte[] b = Encoding.Default.GetBytes(value);
                _outStream.Write(b, 0, b.Length);
            }
            catch (IOException ex)
            {
                throw ex;
            }
		}

		public static int getUTFlength(string value, int start, int sum)
		{
            try
            {
                int len = value.Length;
                for (int i = start; i < len && sum <= 65535; ++i)
                {
                    char c = value[i];
                    if (c >= '\u0001' && c <= '\u007f')
                    {
                        sum += 1;
                    }
                    else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff'))
                    {
                        sum += 2;
                    }
                    else
                    {
                        sum += 3;
                    }
                }

                if (sum > UTF8_STRING_LIMIT)
                {
                    throw new FormatException();
                }
                return sum;
            }
            catch (IOException ex)
            {
                throw ex;
            }
            
		}
		public void writeShortArray(short[] A)
		{
            try
            {
                writeShortArray(A, 0, A.Length);
            }
            catch (IOException ex)
            {
                throw ex;
            }

		}

        public void writeIntArray(int[] A)
		{
            try
            {
                writeIntArray(A, 0, A.Length);
            }
            catch (IOException ex)
            {
                throw ex;
            }
        
		}

        public  void writeLongArray(long[] A)
		{
            try
            {
                writeLongArray(A, 0, A.Length);
            }
            catch (IOException ex)
            {
                throw ex;
            }
         
		}

		public virtual void writeDoubleArray(double[] A)
		{
            try
            {
                writeDoubleArray(A, 0, A.Length);
            }
            catch (IOException ex)
            {
                throw ex;
            }
           
		}

		public virtual void writeDoubleArray(double[] A, int startIdx, int len)
		{
            try
            {
                if (longBuf == null)
                {
                    longBuf = new long[longBufSize];
                }
                int end = startIdx + len;
                int pos = 0;
                for (int i = startIdx; i < end; ++i)
                {
                    if (pos >= longBufSize)
                    {
                        writeLongArray(longBuf, 0, pos);
                        pos = 0;
                    }
                    longBuf[pos++] = (long)A[i];
                }
                if (pos > 0)
                {
                    writeLongArray(longBuf, 0, pos);
                }
            }
            catch (IOException ex)
            {
                throw ex;
            }
            
		}

		public virtual void writeFloatArray(float[] A)
		{
            try
            {
                writeFloatArray(A, 0, A.Length);
            }
            catch (IOException ex)
            {
                throw ex;
            }

		}

        public virtual void writeFloatArray(float[] A, int startIdx, int len)
		{
            try
            {
                if (intBuf == null)
                {
                    intBuf = new int[intBufSize];
                }
                int end = startIdx + len;
                int pos = 0;
                for (int i = startIdx; i < end; ++i)
                {
                    if (pos >= intBufSize)
                    {
                        writeIntArray(intBuf, 0, pos);
                        pos = 0;
                    }
                    intBuf[pos++] = Convert.ToInt32(A[i]);
                }
                if (pos > 0)
                {
                    writeIntArray(intBuf, 0, pos);
                }
            }
            catch (IOException ex)
            {
                throw ex;
            }
            
		}

		public virtual void writeStringArray(string[] A)
		{
            try
            {
                writeStringArray(A, 0, A.Length);
            }
            catch (IOException ex)
            {
                throw ex;
            }
		}

		public virtual void writeStringArray(string[] A, int startIdx, int len)
		{
            try
            {
                if (buf == null)
                {
                    buf = new sbyte[BUF_SIZE];
                }
                int end = startIdx + len;
                int pos = 0;
                for (int j = startIdx; j < end; ++j)
                {
                    string value = A[j];
                    int valueLen = value.Length;
                    int i = 0;
                    do
                    {
                        while (i < valueLen && pos < buf.Length - 4)
                        {
                            char c = value[i++];
                            if (c >= '\u0001' && c <= '\u007f')
                            {
                                buf[pos++] = (sbyte)c;
                            }
                            else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff'))
                            {
                                buf[pos++] = unchecked((sbyte)(0xc0 | (0x1f & (c >> 6))));
                                buf[pos++] = unchecked((sbyte)(0x80 | (0x3f & c)));
                            }
                            else
                            {
                                buf[pos++] = unchecked((sbyte)(0xe0 | (0x0f & (c >> 12))));
                                buf[pos++] = unchecked((sbyte)(0x80 | (0x3f & (c >> 6))));
                                buf[pos++] = unchecked((sbyte)(0x80 | (0x3f & c)));
                            }
                        }
                        if (i >= valueLen)
                        {
                            buf[pos++] = 0;
                        }
                        if (pos + 4 >= buf.Length)
                        {
                            _outStream.Write((byte[])(Array)buf, 0, pos);
                            pos = 0;
                        }
                    } while (i < valueLen);
                }
                if (pos > 0)
                {
                    _outStream.Write((byte[])(Array)buf, 0, pos);
                }
            }
            catch (IOException ex)
            {
                throw ex;
            }
           
		}

        public abstract void writeInt(int value);

        public abstract void writeLong(long value);

        public abstract void writeShort(short s);

        public void write(byte[] b)
        {
            write(b,0,b.Length);
        }
    }
}