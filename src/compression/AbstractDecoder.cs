using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using dolphindb.io;
using dolphindb.compression;
using dolphindb.data;

namespace dolphindb.compression
{
    public abstract class AbstractDecoder : Decoder {
        public abstract ExtendedDataInput Decompress(ExtendedDataInput input, int length, int unitLength, int elementCount, int extra, bool isLittleEndian, int type, short scale);

        public ByteBuffer CreateColumnVector(int rows, int extra, int unitLength, bool isLittleEndian, int minSize, int dataType, short scale)
        {
            ByteBuffer ret = ByteBuffer.Allocate(Math.Max(rows * unitLength + 8, minSize));
            ret.order(isLittleEndian);
            ret.WriteInt(rows);
            ret.WriteInt(extra);
            if (Utils.getCategory((DATA_TYPE)dataType) == DATA_CATEGORY.DENARY)
            {
                ret.WriteInt(scale);
            }
            return ret;
        }
}
}
