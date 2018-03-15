using System.IO;

namespace dolphindb.io
{


    public interface ExtendedDataInput
    {
        bool readBoolean();

        byte readByte();

        long readLong();

        int readInt();

        char readChar();

        double readDouble();

        float readFloat();

        void readFully(byte[] arg0);

        void readFully(byte[] arg0, int arg1, int arg2);

        string readLine();

        string readString();

        short readShort();

        int readUnsignedByte();

        int skipBytes(int n);


    }
}