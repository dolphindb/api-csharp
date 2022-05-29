using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.data;
using dolphindb.io;
namespace dolphindb.compression
{
    class VectorDecompressor
    {
        public IVector Decompress(IEntityFactory factory, ExtendedDataInput input, bool extended, bool isLittleEndian)
        {

            int compressedBytes = input.readInt();
            //((LittleEndianDataInputStream)input).skipBytes(7);
            for(int i =0; i < 7; ++i)
            {
                input.readByte();
            }
            int compression = input.readByte();
            int dataType = input.readByte();
            int unitLength = input.readByte();
            //((LittleEndianDataInputStream)input).skipBytes(6);
            for (int i = 0; i < 2; ++i)
            {
                input.readByte();
            }
            int etra = input.readInt();
            int elementCount = input.readInt();
            if (dataType < (int)DATA_TYPE.DT_BOOL_ARRAY)
                etra = 1;
            input.readInt();

            ExtendedDataInput decompressedIn = DecoderFactory.get(compression).Decompress(input, compressedBytes - 20, unitLength, elementCount, etra, isLittleEndian);
            bool extend = dataType >= 128;
            if (dataType >= 128)
                dataType -= 128;
            DATA_TYPE dt = (DATA_TYPE)dataType;
            return (IVector)factory.createEntity(DATA_FORM.DF_VECTOR, dt, decompressedIn, extend);
        }
    }
}
