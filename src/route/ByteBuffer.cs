/// From https://gitee.com/zkpursuit/Untiy-CSharp-ByteBuffer/blob/master/ByteBuffer.cs
/// Add reserve, remain 
/// Modify WriteBytes, WriteByte
/// Deleted Chinese comments

using System;
using System.Collections.Generic;

[Serializable]
public class ByteBuffer 
{
    public byte[] buf;
    private int readIndex = 0;
    private int writeIndex = 0;
    private int markReadIndex = 0;
    private int markWirteIndex = 0;
    private int capacity;
    public bool isLittleEndian = true;

    private static List<ByteBuffer> pool = new List<ByteBuffer>();
    private static int poolMaxCount = 200;
    private bool isPool = false;

    private ByteBuffer(int capacity)
    {
        this.buf = new byte[capacity];
        this.capacity = capacity;
        this.readIndex = 0;
        this.writeIndex = 0;
    }

    private ByteBuffer(byte[] bytes)
    {
        this.buf = new byte[bytes.Length];
        Array.Copy(bytes, 0, buf, 0, buf.Length);
        this.capacity = buf.Length;
        this.readIndex = 0;
        this.writeIndex = bytes.Length + 1;
    }

    public static ByteBuffer Allocate(int capacity, bool fromPool = false)
    {
        if (!fromPool)
        {
            return new ByteBuffer(capacity);
        }
        lock (pool)
        {
            ByteBuffer bbuf;
            if (pool.Count == 0)
            {
                bbuf = new ByteBuffer(capacity)
                {
                    isPool = true
                };
                return bbuf;
            }
            int lastIndex = pool.Count - 1;
            bbuf = pool[lastIndex];
            pool.RemoveAt(lastIndex);
            if (!bbuf.isPool)
            {
                bbuf.isPool = true;
            }
            return bbuf;
        }
    }

    public static ByteBuffer Allocate(byte[] bytes, bool fromPool = false)
    {
        if (!fromPool)
        {
            return new ByteBuffer(bytes);
        }
        lock (pool)
        {
            ByteBuffer bbuf;
            if (pool.Count == 0)
            {
                bbuf = new ByteBuffer(bytes)
                {
                    isPool = true
                };
                return bbuf;
            }
            int lastIndex = pool.Count - 1;
            bbuf = pool[lastIndex];
            bbuf.WriteBytes(bytes);
            pool.RemoveAt(lastIndex);
            if (!bbuf.isPool)
            {
                bbuf.isPool = true;
            }
            return bbuf;
        }
    }

    private int FixLength(int value)
    {
        if (value == 0)
        {
            return 1;
        }
        value--;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        return value + 1;
    }

    private byte[] Flip(byte[] bytes)
    {
        if (!this.isLittleEndian)
        {
            Array.Reverse(bytes);
        }
        return bytes;
    }

    private int FixSizeAndReset(int currLen, int futureLen)
    {
        if (futureLen > currLen)
        {
            int size = FixLength(currLen) * 2;
            if (futureLen > size)
            {
                size = FixLength(futureLen) * 2;
            }
            byte[] newbuf = new byte[size];
            Array.Copy(buf, 0, newbuf, 0, currLen);
            buf = newbuf;
            capacity = size;
        }
        return futureLen;
    }

    public void WriteBytes(byte[] bytes, int startIndex, int length)
    {
        if (capacity - writeIndex < length)
            throw new Exception("ByteBuffer write a cross-border. ");
        int offset = length - startIndex;
            if (offset <= 0) return;
        int total = offset + writeIndex;
        int len = buf.Length;
        //FixSizeAndReset(len, total);
        for (int i = writeIndex, j = startIndex; i < total; i++, j++)
        {
            buf[i] = bytes[j];
        }
        writeIndex = total;
    }

    public void PutBytes(byte[] bytes, int index)
    {
        for (int i = index, j = 0; j<bytes.Length; i++, j++)
        {
            buf[i] = bytes[j];
        }
    }

    public void WriteBytes(byte[] bytes, int length)
    {
        WriteBytes(bytes, 0, length);
    }

    public void WriteBytes(byte[] bytes)
    {
        WriteBytes(bytes, bytes.Length);
    }

    public void Write(ByteBuffer buffer)
    {
        if (buffer == null) return;
        if (buffer.ReadableBytes <= 0) return;
        WriteBytes(buffer.ToArray());
    }

    public void WriteShort(short value)
    {
        WriteBytes(Flip(BitConverter.GetBytes(value)));
    }

    public void WriteUshort(ushort value)
    {
        WriteBytes(Flip(BitConverter.GetBytes(value)));
    }

    public void WriteInt(int value)
    {
        WriteBytes(Flip(BitConverter.GetBytes(value)));
    }

    public void WriteInt(int value, int index)
    {
        WriteBytes(Flip(BitConverter.GetBytes(value)), index);
    }

    public void PutInt(int value, int index)
    {
        PutBytes(Flip(BitConverter.GetBytes(value)), index);
    }

    public void WriteUint(uint value)
    {
        WriteBytes(Flip(BitConverter.GetBytes(value)));
    }

    public void WriteLong(long value)
    {
        WriteBytes(Flip(BitConverter.GetBytes(value)));
    }

    public void WriteUlong(ulong value)
    {
        WriteBytes(Flip(BitConverter.GetBytes(value)));
    }

    public void WriteFloat(float value)
    {
        WriteBytes(Flip(BitConverter.GetBytes(value)));
    }

    public void WriteByte(byte value)
    {
        if (capacity - writeIndex < 1)
            throw new Exception("ByteBuffer write a cross-border. ");
        int afterLen = writeIndex + 1;
        int len = buf.Length;
        //FixSizeAndReset(len, afterLen);
        buf[writeIndex] = value;
        writeIndex = afterLen;
    }

    public void WriteByte(int value)
    {
        byte b = (byte)value;
        WriteByte(b);
    }

    public void WriteDouble(double value)
    {
        WriteBytes(Flip(BitConverter.GetBytes(value)));
    }

    public void WriteChar(char value)
    {
        WriteBytes(Flip(BitConverter.GetBytes(value)));
    }

    public void WriteBoolean(bool value)
    {
        WriteBytes(Flip(BitConverter.GetBytes(value)));
    }

    public byte ReadByte()
    {
        byte b = buf[readIndex];
        readIndex++;
        return b;
    }

    private byte[] Get(int index, int len)
    {
        byte[] bytes = new byte[len];
        Array.Copy(buf, index, bytes, 0, len);
        return Flip(bytes);
    }

    private byte[] Read(int len)
    {
        byte[] bytes = Get(readIndex, len);
        readIndex += len;
        return bytes;
    }

    public ushort ReadUshort()
    {
        return BitConverter.ToUInt16(Read(2), 0);
    }

    public short ReadShort()
    {
        return BitConverter.ToInt16(Read(2), 0);
    }

    public uint ReadUint()
    {
        return BitConverter.ToUInt32(Read(4), 0);
    }

    public int ReadInt()
    {
        return BitConverter.ToInt32(Read(4), 0);
    }

    public ulong ReadUlong()
    {
        return BitConverter.ToUInt64(Read(8), 0);
    }

    public long ReadLong()
    {
        return BitConverter.ToInt64(Read(8), 0);
    }

    public float ReadFloat()
    {
        return BitConverter.ToSingle(Read(4), 0);
    }

    public double ReadDouble()
    {
        return BitConverter.ToDouble(Read(8), 0);
    }

    public char ReadChar()
    {
        return BitConverter.ToChar(Read(2), 0);
    }

    public bool ReadBoolean()
    {
        return BitConverter.ToBoolean(Read(1), 0);
    }

    public void ReadBytes(byte[] disbytes, int disstart, int len)
    {
        int size = disstart + len;
        for (int i = disstart; i < size; i++)
        {
            disbytes[i] = this.ReadByte();
        }
    }

    public byte GetByte(int index)
    {
        return buf[index];
    }

    public byte GetByte()
    {
        return GetByte(readIndex);
    }

    public double GetDouble(int index)
    {
        return BitConverter.ToDouble(Get(index, 8), 0);
    }

    public double GetDouble()
    {
        return GetDouble(readIndex);
    }

    public float GetFloat(int index)
    {
        return BitConverter.ToSingle(Get(index, 4), 0);
    }

    public float GetFloat()
    {
        return GetFloat(readIndex);
    }

    public long GetLong(int index)
    {
        return BitConverter.ToInt64(Get(index, 8), 0);
    }

    public long GetLong()
    {
        return GetLong(readIndex);
    }

    public ulong GetUlong(int index)
    {
        return BitConverter.ToUInt64(Get(index, 8), 0);
    }

    public ulong GetUlong()
    {
        return GetUlong(readIndex);
    }

    public int GetInt(int index)
    {
        return BitConverter.ToInt32(Get(index, 4), 0);
    }

    public int GetInt()
    {
        return GetInt(readIndex);
    }

    public uint GetUint(int index)
    {
        return BitConverter.ToUInt32(Get(index, 4), 0);
    }

    public uint GetUint()
    {
        return GetUint(readIndex);
    }

    public int GetShort(int index)
    {
        return BitConverter.ToInt16(Get(index, 2), 0);
    }

    public int GetShort()
    {
        return GetShort(readIndex);
    }

    public int GetUshort(int index)
    {
        return BitConverter.ToUInt16(Get(index, 2), 0);
    }

    public int GetUshort()
    {
        return GetUshort(readIndex);
    }

    public char GetChar(int index)
    {
        return BitConverter.ToChar(Get(index, 2), 0);
    }

    public char GetChar()
    {
        return GetChar(readIndex);
    }

    public bool GetBoolean(int index)
    {
        return BitConverter.ToBoolean(Get(index, 1), 0);
    }

    public bool GetBoolean()
    {
        return GetBoolean(readIndex);
    }

    public void DiscardReadBytes()
    {
        if (readIndex <= 0) return;
        int len = buf.Length - readIndex;
        byte[] newbuf = new byte[len];
        Array.Copy(buf, readIndex, newbuf, 0, len);
        buf = newbuf;
        writeIndex -= readIndex;
        markReadIndex -= readIndex;
        if (markReadIndex < 0)
        {
            //markReadIndex = readIndex;
            markReadIndex = 0;
        }
        markWirteIndex -= readIndex;
        if (markWirteIndex < 0 || markWirteIndex < readIndex || markWirteIndex < markReadIndex)
        {
            markWirteIndex = writeIndex;
        }
        readIndex = 0;
    }

    public int ReaderIndex {
        get {
            return readIndex;
        }
        set {
            if (value < 0) return;
            readIndex = value;
        }
    }

    public int WriterIndex {
        get {
            return writeIndex;
        }
        set {
            if (value < 0) return;
            writeIndex = value;
        }
    }

    public void MarkReaderIndex()
    {
        markReadIndex = readIndex;
    }

    public void MarkWriterIndex()
    {
        markWirteIndex = writeIndex;
    }

    public void ResetReaderIndex()
    {
        readIndex = markReadIndex;
    }

    public void ResetWriterIndex()
    {
        writeIndex = markWirteIndex;
    }

    public int ReadableBytes {
        get {
            return writeIndex - readIndex;
        }
    }

    public int Capacity {
        get {
            return this.capacity;
        }
    }

    public byte[] ToArray()
    {
        byte[] bytes = new byte[writeIndex];
        Array.Copy(buf, 0, bytes, 0, bytes.Length);
        return bytes;
    }

    public byte[] array()
    {
        return buf;
    }

    public ByteBuffer Copy()
    {
        if (buf == null)
        {
            return new ByteBuffer(16);
        }
        if (readIndex < writeIndex)
        {
            byte[] newbytes = new byte[writeIndex - readIndex];
            Array.Copy(buf, readIndex, newbytes, 0, newbytes.Length);
            ByteBuffer buffer = new ByteBuffer(newbytes.Length);
            buffer.WriteBytes(newbytes);
            buffer.isPool = this.isPool;
            return buffer;
        }
        return new ByteBuffer(16);
    }

    public ByteBuffer Clone()
    {
        if (buf == null)
        {
            return new ByteBuffer(16);
        }
        ByteBuffer newBuf = new ByteBuffer(buf)
        {
            capacity = this.capacity,
            readIndex = this.readIndex,
            writeIndex = this.writeIndex,
            markReadIndex = this.markReadIndex,
            markWirteIndex = this.markWirteIndex,
            isPool = this.isPool
        };
        return newBuf;
    }

    public void ForEach(Action<byte> action)
    {
        for (int i = 0; i < this.ReadableBytes; i++)
        {
            action.Invoke(this.buf[i]);
        }
    }

    public void Clear()
    {
        //buf = new byte[buf.Length];
        for (int i = 0; i < buf.Length; i++)
        {
            buf[i] = 0;
        }
        readIndex = 0;
        writeIndex = 0;
        markReadIndex = 0;
        markWirteIndex = 0;
        capacity = buf.Length;
    }

    public void Dispose()
    {
        if (isPool)
        {
            lock (pool)
            {
                if (pool.Count < poolMaxCount)
                {
                    this.Clear();
                    pool.Add(this);
                }
                else
                {
                    readIndex = 0;
                    writeIndex = 0;
                    markReadIndex = 0;
                    markWirteIndex = 0;
                    capacity = 0;
                    buf = null;
                }
            }
        }
        else
        {
            readIndex = 0;
            writeIndex = 0;
            markReadIndex = 0;
            markWirteIndex = 0;
            capacity = 0;
            buf = null;
        }
    }

    public void order(bool isLittleEndianArg)
    {
        this.isLittleEndian = isLittleEndianArg;
    }

    public void reserve(int capacity)
    {
        this.capacity = capacity;
        byte[] tmp = new byte[capacity];
        Array.Copy(buf, 0, tmp, 0, buf.Length);
        buf = tmp;
    }

    public int remain()
    {
        return capacity - writeIndex;
    }
}