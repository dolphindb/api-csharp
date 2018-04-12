using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using dolphindb.data;

namespace dolphindb_csharp_api_test
{
    /// <summary>
    /// Utils_test 的摘要说明
    /// </summary>
    [TestClass]
    public class Utils_test
    {
        public Utils_test()
        {
            //
            //TODO:  在此处添加构造函数逻辑
            //
        }

        private TestContext testContextInstance;

        /// <summary>
        ///获取或设置测试上下文，该上下文提供
        ///有关当前测试运行及其功能的信息。
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        #region 附加测试特性
        //
        // 编写测试时，可以使用以下附加特性: 
        //
        // 在运行类中的第一个测试之前使用 ClassInitialize 运行代码
        // [ClassInitialize()]
        // public static void MyClassInitialize(TestContext testContext) { }
        //
        // 在类中的所有测试都已运行之后使用 ClassCleanup 运行代码
        // [ClassCleanup()]
        // public static void MyClassCleanup() { }
        //
        // 在运行每个测试之前，使用 TestInitialize 来运行代码
        // [TestInitialize()]
        // public void MyTestInitialize() { }
        //
        // 在每个测试运行完之后，使用 TestCleanup 来运行代码
        // [TestCleanup()]
        // public void MyTestCleanup() { }
        //
        #endregion

        [TestMethod]
        public void Test_Util_countDays()
        {
            int days1 = Utils.countDays(1970, 1, 1);
            int days2 = Utils.countDays(2000, 1, 1);
            int days3 = Utils.countDays(2100, 3, 1);

            Assert.AreEqual(0, days1);
            Assert.AreEqual(10957, days2);
            Assert.AreEqual(47541, days3);//2100 is not a leapyear
        }

        [TestMethod]
        public void Test_Util_parseDate()
        {
            DateTime dt1 = Utils.parseDate(47541);
            DateTime dt2 = Utils.parseDate(10957);

            Assert.AreEqual(new DateTime(2100,3,1), dt1);
            Assert.AreEqual(new DateTime(2000,1, 1), dt2);
        }
    }
}
