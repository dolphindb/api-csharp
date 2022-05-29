using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.compression;
using dolphindb.data;

namespace dolphindb.compression
{
    class DeltaOfDeltaEncoder:Encoder
    {
        private static int DEFAULT_BLOCK_SIZE = 65536;
        
        public int compress(AbstractVector input, int elementCount, int unitLength, int maxCompressedLength, ByteBuffer output){
            DeltaOfDeltaBlockEncoder blockEncoder = new DeltaOfDeltaBlockEncoder(unitLength);
            int count = 0;
            int dataCount = input.rows();
            int dataIndex = 0;
            ByteBuffer dataBufer = ByteBuffer.Allocate(DEFAULT_BLOCK_SIZE);
            dataBufer.order(output.isLittleEndian);
            int partial = 0, elementNum = 0;
            while (dataCount > dataIndex)
            {
                int readBytes = input.serialize(dataIndex, partial, dataCount - dataIndex, out elementNum, out partial, dataBufer);
                dataIndex += elementNum;
                while (readBytes > 0)
                {
                    int blockSize = Math.Min(readBytes, DEFAULT_BLOCK_SIZE);
                    long[] compressed = blockEncoder.compress(dataBufer, blockSize);
                    //write blockSize+data
                    output.WriteInt(compressed.Length * sizeof(long));
                    foreach (long l in compressed)
                    {
                        if (output.remain() < sizeof(long))
                            output.reserve(output.Capacity * 2);
                        output.WriteLong(l);
                    }
                    count += sizeof(int) + compressed.Length * sizeof(long);
                    readBytes -= blockSize;
                    dataBufer.Clear();
                }
            }
            return count;
        }

        public ByteBuffer compress(ByteBuffer input, int elementCount, int unitLength, int maxCompressedLength)  {
            DeltaOfDeltaBlockEncoder blockEncoder = new DeltaOfDeltaBlockEncoder(unitLength);
            int count = 0;
            //TODO: create header in advanced
            ByteBuffer output = ByteBuffer.Allocate(maxCompressedLength);
            while (elementCount > 0 && count<maxCompressedLength) {
                int blockSize = Math.Min(elementCount * unitLength, DEFAULT_BLOCK_SIZE);
                long[] compressed = blockEncoder.compress(input, blockSize);
                //write blockSize+data
                output.WriteInt(compressed.Length * sizeof(long) * 8);
                foreach (long l in compressed) {
                    output.WriteLong(l);
                }
                count += sizeof(int) * 8 + compressed.Length * sizeof(long) * 8;
                elementCount -= blockSize / unitLength;
            }
            return output;
        }
    }
}



class DeltaOfDeltaBlockEncoder
{
    private int unitLength;
    private int FIRST_DELTA_BITS;

    private ByteBuffer input;
    private DeltaBitOutput output;
    private long storedValue;
    private long storedDelta;
    private int count;

    public DeltaOfDeltaBlockEncoder(int unitLength)
    {
        this.unitLength = unitLength;
        this.FIRST_DELTA_BITS = unitLength * sizeof(byte) * 8;
    }

    public static long encodeZigZag64(long n)
    {
        return (n << 1) ^ (n >> 63);
    }

    public long[] compress(ByteBuffer src, int blockSize)
    {
        this.output = new DeltaBitOutput();
        this.input = src;
        this.count = 0; 
        long value = 0;
        while (count * unitLength < blockSize)
        {
            value = ReadBuffer(input);
            if (!(unitLength == 2 && value == short.MinValue || unitLength == 4 && value == int.MinValue || unitLength == 8 && value == long.MinValue))
                break;
            output.writeBits(0, 1);
        }

        if (count * unitLength >= blockSize)
        {
            writeEnd();
            return this.output.getLongArray();
        }
        count--;
        input.ReaderIndex = input.ReaderIndex - unitLength;
        
        writeHeader();
        while (count * unitLength < blockSize)
        {
            value = ReadBuffer(input);
            if (!(unitLength == 2 && value == short.MinValue || unitLength == 4 && value == int.MinValue || unitLength == 8 && value == long.MinValue))
                break;
            output.writeBits(0, 1);
        }

        if (count * unitLength >= blockSize)
        {
            writeEnd();
            return this.output.getLongArray();
        }
        count--;
        input.ReaderIndex = input.ReaderIndex - unitLength;
        
        writeFirstDelta();
        while (count * unitLength < blockSize)
        {
            writeNext();
        }
        writeEnd();
        return this.output.getLongArray();
    }

    private void writeHeader()
    {
        storedValue = ReadBuffer(input);
        output.writeBit();
        output.writeBits(encodeZigZag64(storedValue), unitLength * sizeof(byte) * 8);
    }

    private void writeFirstDelta()
    {
        output.writeBit();
        long value = ReadBuffer(input);
        storedDelta = value - storedValue;
        output.writeBits(encodeZigZag64(storedDelta), FIRST_DELTA_BITS);
        storedValue = value;
    }

    private void writeNext()
    {
        //TODO: NULL VALUE
        // if(null) -> writeNull;
        long value = ReadBuffer(input);
        if (unitLength == 2 && value == short.MinValue || unitLength == 4 && value == int.MinValue || unitLength == 8 && value == long.MinValue)
        {
            compressDataNull();
            return;
        }
        long newDelta =value-storedValue;
        long deltaD =newDelta-storedDelta;
        //FIXME: implement based on the c++ code
        if (deltaD == 0)
        {
            output.skipBit();
        }
        else
        {
            deltaD = encodeZigZag64(deltaD);
            deltaD--;
            if (deltaD < 1L << 7)
            {
                output.writeBits(2L, 2); // store '10'
          
                output.writeBits(deltaD, 7); // Using 7 bits, store the value..
            }
            else if (deltaD < 1L << 9)
            {
                output.writeBits(6L, 3); // store '110'
                output.writeBits(deltaD, 9); // Use 9 bits
            }
            else if (deltaD < 1L << 16)
            {
                output.writeBits(14L, 4); // store '1110'
                output.writeBits(deltaD, 16); // Use 12 bits
            }
            else if (deltaD < 1L << 32)
            {
                output.writeBits(30L, 5); // Store '11110'
                output.writeBits(deltaD, 32); // Store delta using 32 bits
            }
            else
            {
                output.writeBits(62L, 6); // Store '111110'
                output.writeBits(deltaD, 64); // Store delta using 64 bits
            }
        }
        storedDelta = newDelta;
        storedValue = value;
    }

    public void compressDataNull()
    {
        output.writeBits(63, 6);
    }

    public void writeEnd()
    {
        output.writeBits(62, 6);
        unchecked
        {
            output.writeBits((long)0xFFFFFFFFFFFFFFFFL, 64);
        }
        output.skipBit();
    }

    private long ReadBuffer(ByteBuffer input)
    {
        count++;
        if (unitLength == 2)
            return input.ReadShort();
        else if (unitLength == 4)
            return input.ReadInt();
        else if (unitLength == 8)
            return input.ReadLong();
        count--;
        throw new InvalidOperationException();
    }


}
