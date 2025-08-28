using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using dolphindb;
using dolphindb.data;
using dolphindb.io;
using System.IO;

namespace dolphindb_csharp_api_test.compatibility_test.io_test
{
    [TestClass]
    public class BigEndianDataOutputStream_test
    {
        [TestMethod]
        public void Test_BigEndianDataOutputStream()
        {
            Stream outStream = new MemoryStream();
            BigEndianDataOutputStream output = new BigEndianDataOutputStream(outStream);
            output.writeLong(1);
            output.writeLong2(new Long2(1, 1));
            short[] shortv = { 1, 2, 3, 4, 5 };
            long[] longv = { 1, 2, 3, 4, 5 };
            Long2[] long2v = new Long2[] { new Long2(1, 1), new Long2(1, 1) };
            output.writeShortArray(shortv, 0, 2);
            output.writeLongArray(longv, 0, 2);
            output.writeLong2Array(long2v, 0, 2);
            Assert.AreEqual(false, output.isLittleEndian());
            output.writeBoolean(true);
            output.writeBoolean(false);
            output.writeChar(1);
            output.writeChar((char)1);
            output.writeChars("1");
            output.writeString(null);
            output.writeStringArray(new string[] { "12A", "HELLO" });
            output.writeStringArray(new string[] { "12A", "HELLO" }, 1, 1);
        }
    }

}
