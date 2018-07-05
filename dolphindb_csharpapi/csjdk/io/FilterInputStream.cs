using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace com.xxdb.csjdk.io
{

    public class FilterInputStream : InputStream
    {
        protected volatile InputStream _inStream;


        protected FilterInputStream(InputStream @in)
        {
                this._inStream = @in;
        }

        
        public override int read()
        {
            try
            {
                return _inStream.read();
            }
            catch (IOException ex)
            {
                throw ex;
            }
            
        }

    
        public int read(byte[] b) 
        {
            try
            {
                return read(b, 0, b.Length);
            }
            catch (IOException ex)
            {
                throw ex;
            }
            
        }

        public int read(byte[] b, int off, int len)
        {
            try
            {
                return _inStream.read(b, off, len);
            }
            catch (IOException ex)
            {
                throw ex;
            }
        
        }

        public long skip(long n)
        {
            return _inStream.skip(n);
        }

        public int available()
        {
            return _inStream.available();
        }


        public void close()
        {
            _inStream.close();
        }

    }
}
