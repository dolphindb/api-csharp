using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.compression
{
    class DecoderFactory
    {
        private DecoderFactory() { }

        public static Decoder get(int method)
        {
            if (method == 1)
                return (Decoder)new LZ4Decoder();
            else if (method == 2)
                return new DeltaOfDeltaDecoder();
            else
                throw new InvalidOperationException();
        }
    }
}
