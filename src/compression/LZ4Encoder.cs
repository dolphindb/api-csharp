using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using LZ4Sharp;
using dolphindb.data;

namespace dolphindb.compression
{
    class LZ4Encoder :Encoder
    {
        private static int DEFAULT_BLOCK_SIZE = 65536;
        private LZ4Sharp.ILZ4Compressor compresser;

        public int compress(AbstractVector input, int elementCount, int unitLength, int maxCompressedLength, ByteBuffer output)
        {
            if (compresser == null)
            {
                compresser = LZ4Sharp.LZ4CompressorFactory.CreateNew();
            }
            int byteCount = 0;
            int dataCount = input.rows();
            int dataIndex = 0;
            ByteBuffer dataBufer = ByteBuffer.Allocate(DEFAULT_BLOCK_SIZE);
            dataBufer.order(output.isLittleEndian);
            int partial = 0, elementNum;
            while (dataCount > dataIndex)
            {
                int readBytes = input.serialize(dataIndex, partial, dataCount - dataIndex, out elementNum, out partial, dataBufer);
                while(readBytes > 0)
                {
                    int blockSize = Math.Min(DEFAULT_BLOCK_SIZE, dataBufer.ReadableBytes);
                    byte[] srcBuf = new byte[blockSize];
                    dataBufer.ReadBytes(srcBuf, 0, blockSize);
                    byte[] ret = compresser.Compress(srcBuf);
                    if(ret.Length + sizeof(int) > output.remain())
                    {
                        output.reserve((output.WriterIndex + ret.Length + sizeof(int)) * 2);
                    }
                    output.WriteInt(ret.Length);
                    output.WriteBytes(ret);
                    byteCount += ret.Length + sizeof(int) * 8;
                    readBytes -= blockSize;
                }
                dataIndex += elementNum;
                dataBufer.Clear();
            }
            return byteCount;
        }
    }
}
