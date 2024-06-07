
using Microsoft.VisualStudio.TestTools.UnitTesting;
using dolphindb_config;

namespace dolphindb_csharp_api_test.data_test
{
    [TestClass]
    public class BasicVoidTest
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;

        [TestMethod]
        public void Test_Void()
        {
            dolphindb.data.Void vo = new dolphindb.data.Void();
            Assert.AreEqual(true, vo.Equals(vo));
            Assert.AreEqual(false, vo.Equals(null));
            Assert.AreEqual(false, vo.Equals(1));
        }

    }
}
