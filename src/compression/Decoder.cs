using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.data;
using dolphindb.io;
using System.IO;
namespace dolphindb.compression
{
    public interface Decoder
    {
        ExtendedDataInput Decompress(ExtendedDataInput input, int length, int unitLength, int elementCount, int extra, bool isLittleEndian, int type, short scale);
    }
}
