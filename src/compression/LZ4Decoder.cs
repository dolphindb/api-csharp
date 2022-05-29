using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.compression;
using dolphindb.compression;
using dolphindb.io;
using System.IO;
using LZ4Sharp;
    class LZ4Decoder: AbstractDecoder
    {

    public override ExtendedDataInput Decompress(ExtendedDataInput input, int length, int unitLength, int elementCount, int extra, bool isLittleEndian) {
        int offset = 8;
        byte[] output = CreateColumnVector(elementCount, extra, unitLength, isLittleEndian, 0).array();
        ILZ4Decompressor t = LZ4DecompressorFactory.CreateNew();
        while (length > 0) {
            int blockSize = input.readInt();
            if(blockSize< 0){
            	blockSize = blockSize & 2147483647;
            }
            length -= sizeof(int);
            blockSize = Math.Min(blockSize, length);
            if (blockSize == 0) break;
            
            byte[] src = new byte[blockSize];
            int index = 0;
            while (index < blockSize)
            {
                index += ((BinaryReader)input).Read(src, index, blockSize - index);
            }
            byte[] ret = t.Decompress(src);
            if(offset + ret.Length > output.Length)
            {
                byte[] tmp = new byte[Math.Max(offset + ret.Length, output.Length) * 2];
                Array.Copy(output, tmp, offset);
                output = tmp;
            }
            Buffer.BlockCopy(ret, 0, output, offset, ret.Length);
            offset += ret.Length;
            length -= blockSize;
        }
        
        if (isLittleEndian)
                return new LittleEndianDataInputStream(new MemoryStream(output));

        return new BigEndianDataInputStream(new MemoryStream(output));
    }
}
