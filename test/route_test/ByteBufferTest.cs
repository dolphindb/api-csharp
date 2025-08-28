using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using dolphindb;
using dolphindb.data;
using System.IO;
using System.Net.Sockets;
using dolphindb.io;
using System.Text;
using System.Data;
using System.Collections.Generic;
using System.Threading;
using System.Collections;
using System.Threading.Tasks;
using System.Configuration;
using dolphindb_config;
using dolphindb_csharpapi_net_core.src;

namespace dolphindb_csharp_api_test.route_test
{
    [TestClass]
    public class ByteBufferTest
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;

        [TestMethod]
        public void Test_ByteBuffer()
        {
            ByteBuffer bbf1 = ByteBuffer.Allocate(10, true);
            bbf1.Dispose();
            bbf1 = ByteBuffer.Allocate(10, true);

            ByteBuffer bbf2 = ByteBuffer.Allocate(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            ByteBuffer bbf3 = ByteBuffer.Allocate(new byte[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }, true);
            bbf3.Dispose();
            bbf3 = ByteBuffer.Allocate(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, true);
            ByteBuffer bbf4 = null;
            ByteBuffer bbf5 = ByteBuffer.Allocate(15);
            bbf2.reserve(20);
            bbf3.Write(bbf4);
            bbf3.Write(bbf5);
            bbf5.Write(bbf2);
            ushort num = 12345;
            bbf5.WriteUshort(num);
            bbf5.reserve(30);
            bbf5.WriteInt(3, 3);
            uint x = 13;
            bbf5.WriteUint(x);
            ulong y = 13;
            bbf5.WriteUlong(y);
            ByteBuffer bbf6 = ByteBuffer.Allocate(10, true);
            bbf6.Dispose();
            for(int i = 0; i < 300; i++)
            {
                bbf6.Dispose();
            }
            ByteBuffer bbf7 = ByteBuffer.Allocate(10);
            bbf7.Dispose();
        }


        [TestMethod]
        public void Test_ByteBuffer_DiscardReadBytes()
        {
            ByteBuffer bbf1 = ByteBuffer.Allocate(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            try
            {
                bbf1.WriteByte(4);
            }
            catch (Exception e)
            {
                Assert.IsNotNull(e);
                Console.WriteLine(e.ToString());
            }
            ByteBuffer bbf2 = ByteBuffer.Allocate(10);
            bbf2.DiscardReadBytes();
            bbf1.ReadByte();
            bbf1.DiscardReadBytes();
            bbf1.ReadByte();
            bbf1.MarkReaderIndex();
            bbf1.DiscardReadBytes();
            for (int i = 0; i < 6; i++)
            {
                bbf1.ReadByte();
            }
            bbf1.DiscardReadBytes();
            ByteBuffer bbf3 = ByteBuffer.Allocate(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            for (int i = 0; i < 6; i++)
            {
                bbf3.ReadByte();
            }
            bbf3.MarkReaderIndex();
            bbf3.ResetWriterIndex();
            bbf3.DiscardReadBytes();

            ByteBuffer bbf4 = ByteBuffer.Allocate(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            for (int i = 0; i < 6; i++)
            {
                bbf4.ReadByte();
            }
            bbf4.MarkReaderIndex();
            bbf4.ResetReaderIndex();
            bbf4.DiscardReadBytes();
        }


        [TestMethod]
        public void Test_ByteBuffer_Copy()
        {
            ByteBuffer bbf1 = ByteBuffer.Allocate(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            bbf1.ResetWriterIndex();
            ByteBuffer bbf2 = bbf1.Copy();
            bbf1.WriteByte(11);
            ByteBuffer bbf3 = bbf1.Copy();
            ByteBuffer bbf4 = bbf1.Clone();
        }
    }
}
