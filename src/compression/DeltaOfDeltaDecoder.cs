using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using dolphindb.io;
using dolphindb.compression;

namespace dolphindb.compression
{
    class DeltaOfDeltaDecoder : AbstractDecoder

    {
        public override ExtendedDataInput Decompress(ExtendedDataInput input, int length, int unitLength, int elementCount, int extra, bool isLittleEndian, int type, short scale)
        {
            ByteBuffer dest = CreateColumnVector(elementCount, extra, unitLength, isLittleEndian, 0, type, scale);
            int offset = dest.ReadableBytes;
            byte[] output = dest.array();
            int outLength = output.Length - offset;
            int count = 0;
            DeltaOfDeltaBlockDecoder blockDecoder = new DeltaOfDeltaBlockDecoder(unitLength);

            while (length > 0 && count < outLength)
            {
                int blockSize = input.readInt();
                if (blockSize < 0)
                {
                    blockSize = blockSize & 2147483647;
                }
                length -= sizeof(int);
                blockSize = Math.Min(blockSize, length);
                if (blockSize == 0) break;
                long[] src = new long[blockSize / sizeof(long)];
                for (int i = 0; i < src.Length; i++)
                {
                    src[i] = input.readLong();
                }

                count += blockDecoder.decompress(src, dest) * unitLength;
                length -= blockSize;
            }
            if (isLittleEndian)
                return new LittleEndianDataInputStream(new MemoryStream(output, 0, offset + count));
            return
                new BigEndianDataInputStream(new MemoryStream(output, 0, offset + count));
        }

    }
}


class DeltaOfDeltaBlockDecoder
{
    private int unitLength;
    private int FIRST_DELTA_BITS;

    private DeltaBitInput input;
    private ByteBuffer dest;
    private long storedValue;
    private long storedDelta;
    private int count;

    public DeltaOfDeltaBlockDecoder(int unitLength)
    {
        this.unitLength = unitLength;
        this.FIRST_DELTA_BITS = unitLength * sizeof(byte) * 8;
    }

    public static long decodeZigZag64(long n)
    {
        ulong uln = (ulong)n;
        return ((long)(uln >> 1)) ^ (-(long)(uln & 1));
    }

    public int decompress(long[] src, ByteBuffer dest)
    {
        this.dest = dest;
        this.storedValue = 0;
        this.storedDelta = 0;
        count = 0;
        input = new DeltaBitInput(src);
        bool flag = input.readBit();
        while (!flag)
        {
            writeNull(dest);
            flag = input.readBit();
            //            if (in.getPosition() == src.length - 2) return count;
        }
        if (!readHeader())
            return count;
        if (!first())
            return count;
        while (input.getPosition() < src.Length)
        {
            if (!nextValue())
                return count;
        }
        return count;
    }

    private bool readHeader()
    {
        try
        {
            long flag = 0;
            flag = input.readBits(5);
            if (flag == 30)
            {
                if ((ulong)input.readBits(64) == 0xFFFFFFFFFFFFFFFFL)
                {
                    return false;
                }
                else
                {
                    input.rollBack(5);
                    input.rollBack(64);
                }
            }
            else
            {
                input.rollBack(5);
            }
            storedValue = decodeZigZag64(input.getLong(unitLength * 8));
            writeBuffer(dest, storedValue);
        }
        catch (Exception)
        {
            return false;
        }
        return true;
    }

    private bool first()
    {
        try
        {
            bool flag = input.readBit();
            while (!flag)
            {
                writeNull(dest);
                flag = input.readBit();
            }
            int sign = (int)input.getLong(5);
            if (sign == 30)
            {
                if ((ulong)input.getLong(64) == 0xFFFFFFFFFFFFFFFFL)
                    return false;
                else
                {
                    input.rollBack(64);
                    input.rollBack(5);
                }
            }
            else
            {
                input.rollBack(5);
            }
        }
        catch (Exception)
        {
            return false;
        }
        try
        {
            storedDelta = decodeZigZag64(input.getLong(FIRST_DELTA_BITS));
        }
        catch (Exception)
        {
            return false;
        }
        storedValue = storedValue + storedDelta;
        writeBuffer(dest, storedValue);
        return true;
    }

    private bool writeBuffer(ByteBuffer dest, long storedValue)
    {
        if (dest.ReadableBytes < unitLength)
            return false;
        //
        if (unitLength == 2)
            dest.WriteShort((short)storedValue);
        else if (unitLength == 4)
            dest.WriteInt((int)storedValue);
        else if (unitLength == 8)
            dest.WriteLong((long)storedValue);
        count++;
        return true;
    }

    private void writeNull(ByteBuffer dest)
    {
        if (dest.ReadableBytes < unitLength)
            return;
        if (unitLength == 2)
            dest.WriteShort(short.MinValue);
        else if (unitLength == 4)
            dest.WriteInt(int.MinValue);
        else if (unitLength == 8)
            dest.WriteLong(long.MinValue);
        count++;
    }

    private bool nextValue()
    {
        try
        {
            int readInstruction = input.nextClearBit(6);
            long deltaDelta;

            switch (readInstruction)
            {
                case 0x00:
                    storedValue += storedDelta;
                    return writeBuffer(dest, storedValue);
                case 0x02:
                    deltaDelta = input.getLong(7);
                    break;
                case 0x06:
                    deltaDelta = input.getLong(9);
                    break;
                case 0x0e:
                    deltaDelta = input.getLong(16);
                    break;
                case 0x1e:
                    deltaDelta = input.getLong(32);
                    break;
                case 0x3e:
                    deltaDelta = input.getLong(64);
                    // For storage save.. if this is the last available word, check if remaining bits are all 1
                    if ((ulong)deltaDelta == 0xFFFFFFFFFFFFFFFFL)
                    {
                        return false;
                    }
                    break;
                case 0x3f:
                    writeNull(dest);
                    return true;
                default:
                    throw new InvalidCastException();
            }
            deltaDelta++;
            deltaDelta = decodeZigZag64(deltaDelta);
            storedDelta += deltaDelta;
            storedValue += storedDelta;
            return writeBuffer(dest, storedValue);
        }
        catch (Exception)
        {
            return false;
        }
    }
}
