using System;
using System.IO;

namespace dolphindb.io
{


	public class BigEndianDataOutputStream : AbstractExtendedDataOutputStream
	{

        public BigEndianDataOutputStream(Stream outStream) : base(outStream)
		{
  		}

		public override void writeShort(short v)
		{
            write(0xff & (v >> 8));
			write(0xff & v);
		}

		public override void writeInt(int v)
		{
            try
            {
                write(0xFF & (v >> 24));
                write(0xFF & (v >> 16));
                write(0xFF & (v >> 8));
                write(0xFF & v);
            }
            catch (IOException ex)
            {
                throw ex;
            }

            
		}

		public override void writeLong(long v)
		{
            try
            {
                write((int)(0xFF & (v >> 56)));
                write((int)(0xFF & (v >> 48)));
                write((int)(0xFF & (v >> 40)));
                write((int)(0xFF & (v >> 32)));
                write((int)(0xFF & (v >> 24)));
                write((int)(0xFF & (v >> 16)));
                write((int)(0xFF & (v >> 8)));
                write((int)(0xFF & v));
            }
            catch (IOException ex)
            {
                throw ex;
            }

            
		}
		public override void writeShortArray(short[] A, int startIdx, int len)
		{
            try
            {

            }
            catch (IOException ex)
            {
                throw ex;
            }

            if (buf == null)
			{
				buf = new sbyte[BUF_SIZE];
			}
			int end = startIdx + len;
			int pos = 0;
			for (int i = startIdx; i < end; ++i)
			{
				short v = A[i];
				if (pos + 2 >= BUF_SIZE)
				{
					write((byte[])(Array)buf, 0, pos);
					pos = 0;
				}
				buf[pos++] = unchecked((sbyte)(0xFF & (v >> 8)));
				buf[pos++] = unchecked((sbyte)(0xFF & (v)));
			}
			if (pos > 0)
			{
				write((byte[])(Array)buf, 0, pos);
			}
		}

		public override void writeIntArray(int[] A, int startIdx, int len)
		{
            try
            {

            }
            catch (IOException ex)
            {
                throw ex;
            }

            if (buf == null)
			{
				buf = new sbyte[BUF_SIZE];
			}
			int end = startIdx + len;
			int pos = 0;
			for (int i = startIdx; i < end; ++i)
			{
				int v = A[i];
				if (pos + 4 >= BUF_SIZE)
				{
					write((byte[])(Array)buf, 0, pos);
					pos = 0;
				}
				buf[pos++] = unchecked((sbyte)(0xFF & (v >> 24)));
				buf[pos++] = unchecked((sbyte)(0xFF & (v >> 16)));
				buf[pos++] = unchecked((sbyte)(0xFF & (v >> 8)));
				buf[pos++] = unchecked((sbyte)(0xFF & (v)));
			}
			if (pos > 0)
			{
				write((byte[])(Array)buf, 0, pos);
			}
		}

		public override void writeLongArray(long[] A, int startIdx, int len)
		{
            try
            {
                if (buf == null)
                {
                    buf = new sbyte[BUF_SIZE];
                }
                int end = startIdx + len;
                int pos = 0;
                for (int i = startIdx; i < end; ++i)
                {
                    long v = A[i];
                    if (pos + 8 >= BUF_SIZE)
                    {
                        write((byte[])(Array)buf, 0, pos);
                        pos = 0;
                    }
                    buf[pos++] = unchecked((sbyte)(0xFF & (v >> 56)));
                    buf[pos++] = unchecked((sbyte)(0xFF & (v >> 48)));
                    buf[pos++] = unchecked((sbyte)(0xFF & (v >> 40)));
                    buf[pos++] = unchecked((sbyte)(0xFF & (v >> 32)));
                    buf[pos++] = unchecked((sbyte)(0xFF & (v >> 24)));
                    buf[pos++] = unchecked((sbyte)(0xFF & (v >> 16)));
                    buf[pos++] = unchecked((sbyte)(0xFF & (v >> 8)));
                    buf[pos++] = unchecked((sbyte)(0xFF & (v)));
                }
                if (pos > 0)
                {
                    write((byte[])(Array)buf, 0, pos);
                }
            }
            catch (IOException ex)
            {
                throw ex;
            }
        }

    }

}