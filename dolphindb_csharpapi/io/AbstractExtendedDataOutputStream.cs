using System;
using System.IO;
using System.Text;

namespace dolphindb.io
{


    public abstract class AbstractExtendedDataOutputStream : BinaryWriter, ExtendedDataOutput
    {
        public abstract void writeLongArray(long[] A, int startIdx, int len);
        public abstract void writeIntArray(int[] A, int startIdx, int len);
        public abstract void writeShortArray(short[] A, int startIdx, int len);
        private const int UTF8_STRING_LIMIT = 65535;
        protected internal const int BUF_SIZE = 4096;
        protected internal byte[] buf;
        protected static readonly int longBufSize = BUF_SIZE / 8;
        protected static readonly int intBufSize = BUF_SIZE / 4;
        protected int[] intBuf;
        protected long[] longBuf;
        //Stream _outStream;
        public AbstractExtendedDataOutputStream(Stream outStream) : base(outStream)
        {
            //  _outStream = outStream;
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

        public void writeBoolean(bool v)
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

        public void writeByte(int v)
        {
            byte b = (byte)v;
            base.Write(b);
        }

        public void writeChar(char v)
        {
            writeShort(v);
        }

        public void writeChar(int v)
        {
            writeShort(v);
        }

        public void writeFloat(float v)
        {
            int s = BitConverter.ToInt32(BitConverter.GetBytes(v), 0);
            writeInt(s);
        }

        public void writeDouble(double v)
        {
            long s = BitConverter.DoubleToInt64Bits(v);
            writeLong(s);
        }

        public void writeBytes(string s)
        {
            byte[] b = Encoding.UTF8.GetBytes(s);
            base.Write(b);
            /*
             * int len = s.Length;
            int i = 0;
            int pos = 0;

            if (buf == null)
                buf = new byte[BUF_SIZE];
            do
            {
                while (i < len && pos < buf.Length - 4)
                {
                    char c = s[i++];
                    if (c >= '\u0001' && c <= '\u007f')
                        buf[pos++] = (byte)c;
                    else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff'))
                    {
                        buf[pos++] = (byte)(0xc0 | (0x1f & (c >> 6)));
                        buf[pos++] = (byte)(0x80 | (0x3f & c));
                    }
                    else
                    {
                        buf[pos++] = (byte)(0xe0 | (0x0f & (c >> 12)));
                        buf[pos++] = (byte)(0x80 | (0x3f & (c >> 6)));
                        buf[pos++] = (byte)(0x80 | (0x3f & c));
                    }
                }
                base.Write(buf, 0, pos);
                pos = 0;
            } while (i < len);
            */
        }

        public void writeChars(string s)
        {
            int len = s.Length;
            for (int i = 0; i < len; ++i)
                writeChar(s[i]);
        }

        public void writeUTF(string value)
        {
            base.Write(value);
            //try
            //{
            //    int len = value.Length;
            //    int i = 0;
            //    int pos = 0;
            //    bool lengthWritten = false;
            //    if (buf == null)
            //        buf = new byte[BUF_SIZE];
            //    do
            //    {
            //        while (i < len && pos < buf.Length - 3)
            //        {
            //            char c = value[i++];
            //            if (c >= '\u0001' && c <= '\u007f')
            //                buf[pos++] = (byte)c;
            //            else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff'))
            //            {
            //                buf[pos++] = (byte)(0xc0 | (0x1f & (c >> 6)));
            //                buf[pos++] = (byte)(0x80 | (0x3f & c));
            //            }
            //            else
            //            {
            //                buf[pos++] = (byte)(0xe0 | (0x0f & (c >> 12)));
            //                buf[pos++] = (byte)(0x80 | (0x3f & (c >> 6)));
            //                buf[pos++] = (byte)(0x80 | (0x3f & c));
            //            }
            //        }
            //        if (!lengthWritten)
            //        {
            //            if (i == len)
            //                writeShort((short)pos);
            //            else
            //                writeShort((short)getUTFlength(value, i, pos));
            //            lengthWritten = true;
            //        }
            //        base.Write(buf, 0, pos);
            //        pos = 0;
            //    } while (i < len);
            //}
            //catch (IOException ex)
            //{
            //    throw ex;
            //}
        }

        public void writeString(string value)
        {
            if (value == null) return;
            int len = value.Length;
            int i = 0;
            int pos = 0;
            if (buf == null)
                buf = new byte[BUF_SIZE];
            do
            {
                while (i < len && pos < buf.Length - 4)
                {
                    char c = value[i++];
                    if (c >= '\u0001' && c <= '\u007f')
                        buf[pos++] = (byte)c;
                    else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff'))
                    {
                        buf[pos++] = (byte)(0xc0 | (0x1f & (c >> 6)));
                        buf[pos++] = (byte)(0x80 | (0x3f & c));
                    }
                    else
                    {
                        buf[pos++] = (byte)(0xe0 | (0x0f & (c >> 12)));
                        buf[pos++] = (byte)(0x80 | (0x3f & (c >> 6)));
                        buf[pos++] = (byte)(0x80 | (0x3f & c));
                    }
                }
                if (i >= len)
                    buf[pos++] = 0;
                base.Write(buf, 0, pos);
                pos = 0;
            } while (i < len);
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

        public void writeLongArray(long[] A)
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

                    longBuf[pos++] = BitConverter.DoubleToInt64Bits(A[i]);
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
                    intBuf[pos++] = BitConverter.ToInt32(BitConverter.GetBytes(A[i]), 0);
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
                    buf = new byte[BUF_SIZE];
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
                                buf[pos++] = (byte)c;
                            }
                            else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff'))
                            {
                                buf[pos++] = unchecked((byte)(0xc0 | (0x1f & (c >> 6))));
                                buf[pos++] = unchecked((byte)(0x80 | (0x3f & c)));
                            }
                            else
                            {
                                buf[pos++] = unchecked((byte)(0xe0 | (0x0f & (c >> 12))));
                                buf[pos++] = unchecked((byte)(0x80 | (0x3f & (c >> 6))));
                                buf[pos++] = unchecked((byte)(0x80 | (0x3f & c)));
                            }
                        }
                        if (i >= valueLen)
                        {
                            buf[pos++] = 0;
                        }
                        if (pos + 4 >= buf.Length)
                        {
                            base.Write((byte[])(Array)buf, 0, pos);
                            pos = 0;
                        }
                    } while (i < valueLen);
                }
                if (pos > 0)
                {
                    base.Write((byte[])(Array)buf, 0, pos);
                }
            }
            catch (IOException ex)
            {
                throw ex;
            }

        }

        
    public void writeLong2Array(Long2[] A)
    {
        writeLong2Array(A, 0, A.Length);
	}

    public abstract void writeInt(int value);

    public abstract void writeLong(long value);

    public abstract void writeShort(int s);

        public abstract void writeLong2Array(Long2[] A, int startIdx, int len);

        public void write(byte[] b)
        {
            base.Write(b, 0, b.Length);
        }
        public void write(byte[] b, int offset, int length)
        {
            base.Write(b, offset, length);
        }

        public void write(int b)
        {
            //byte ub = byte.Parse(b.ToString());
            base.Write((byte)b);
        }

        public abstract void writeLong2(Long2 v);
    }
}