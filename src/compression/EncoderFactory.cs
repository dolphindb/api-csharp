using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.compression;

namespace dolphindb.compression
{
    class EncoderFactory
    {
        private EncoderFactory() { }

        public static Encoder Get(int method)
        {
            if (method == 1)
                return (Encoder)new LZ4Encoder();
            else if (method == 2)
                return (Encoder)new DeltaOfDeltaEncoder();
            throw new InvalidOperationException();
        }
    }
}
