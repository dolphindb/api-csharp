using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace com.xxdb.csjdk.io
{
    public abstract class InputStream
    {

        private readonly int MAX_SKIP_BUFFER_SIZE = 2048;

        public abstract int read();

        public int read(byte[] b)
        {
            return read(b, 0, b.Length);
        }

        public int read(byte[] b, int off, int len)
        {
            if (b == null)
            {
                throw new NullReferenceException();
            }
            else if (off < 0 || len < 0 || len > b.Length - off)
            {
                throw new IndexOutOfRangeException();
            }
            else if (len == 0)
            {
                return 0;
            }

            int c = read();
            if (c == -1)
            {
                return -1;
            }
            b[off] = (byte)c;

            int i = 1;
            try
            {
                for (; i < len; i++)
                {
                    c = read();
                    if (c == -1)
                    {
                        break;
                    }
                    b[off + i] = (byte)c;
                }
            }
            catch (IOException ee)
            {
            }
            return i;
        }

        public long skip(long n)
        {

            long remaining = n;
            int nr;

            if (n <= 0)
            {
                return 0;
            }

            int size = (int)Math.Min(MAX_SKIP_BUFFER_SIZE, remaining);
            byte[]
            skipBuffer = new byte[size];
            while (remaining > 0)
            {
                nr = read(skipBuffer, 0, (int)Math.Min(size, remaining));
                if (nr < 0)
                {
                    break;
                }
                remaining -= nr;
            }

            return n - remaining;
        }

        public int available()
        {
            return 0;
        }

        public void close() { }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void mark(int readlimit) { }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void reset()
        {
            throw new IOException("mark/reset not supported");
        }


        public bool markSupported()
        {
            return false;
        }
    }
}
