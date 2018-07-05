using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace com.xxdb.jobjects
{
    class FilterOutputStream
    {
        private Stream _outStream;
        public FilterOutputStream(Stream @out)
        {
            _outStream = @out;
        }

        public void write(int b)
        {

        }
    }
}
