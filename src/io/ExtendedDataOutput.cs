namespace dolphindb.io
{


    public interface ExtendedDataOutput
    {
        void write(byte[] b);
        void writeShort(int s);
        void writeString(string str);

        //2021.01.19 cwj
        void writeBlob(byte[] value);
        //

        void flush();
        void writeShortArray(short[] A);
        void writeShortArray(short[] A, int startIdx, int len);
        void writeIntArray(int[] A);
        void writeIntArray(int[] A, int startIdx, int len);
        void writeLongArray(long[] A);
        void writeLongArray(long[] A, int startIdx, int len);
        void writeDoubleArray(double[] A);
        void writeDoubleArray(double[] A, int startIdx, int len);
        void writeFloatArray(float[] A);
        void writeFloatArray(float[] A, int startIdx, int len);
        void writeStringArray(string[] A);
        void writeStringArray(string[] A, int startIdx, int len);
        void writeLong2Array(Long2[] A);
        void writeLong2Array(Long2[] A, int startIdx, int len);
        void writeBoolean(bool v);
        void writeByte(int v);
        void writeChar(char v);
        void writeFloat(float v);
        void writeDouble(double v);
        void writeBytes(string s);
        void writeChars(string s);
        void writeUTF(string value);
        void writeInt(int value);
        void writeLong(long value);
        void writeLong2(Long2 v);

        bool isLittleEndian();
        void write(byte[] b, int offset, int length);
    }
}