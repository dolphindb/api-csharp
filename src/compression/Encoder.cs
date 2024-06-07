using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.data;

namespace dolphindb.compression
{
    interface Encoder
    {

        int compress(AbstractVector input, int elementCount, int unitLength, int maxCompressedLength, ByteBuffer output);
    }
}
