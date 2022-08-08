using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using dolphindb.data;
using dolphindb;
namespace dolphindb_csharp_api_test
{
    /// <summary>
    /// Utils_test Summary
    /// </summary>
    [TestClass]
    public class Utils_test
    {
        public Utils_test()
        {
        }

        private TestContext testContextInstance;

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

            Assert.AreEqual(new DateTime(2100, 3, 1), dt1);
            Assert.AreEqual(new DateTime(2000, 1, 1), dt2);
        }

        [TestMethod]
        public void Test_RSAUtil_GetCode()
        {
            string pub_Key_File = @"-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDDl3JJzcEHnVhnbSWvjXAGpy7M
Dfkyw8+cxtBzrE7rdLvMvOSuLXnAAya/BAhB7hx2nIAonSaSwjLxqIVo8n97y7h/
l94eMzaAiTb4is2lew/fZmJeKLEdjvn/IaWDQgCq5TDn4cgLp4kQMtbAsddjoEWq
xeBqwbgg5VAp5wZyjQIDAQAB
-----END PUBLIC KEY-----
";
            string key = RSAUtils.GetKey(pub_Key_File);
            string re = RSAUtils.RSA("admin", key);
            Assert.AreEqual("MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDDl3JJzcEHnVhnbSWvjXAGpy7MDfkyw8+cxtBzrE7rdLvMvOSuLXnAAya/BAhB7hx2nIAonSaSwjLxqIVo8n97y7h/l94eMzaAiTb4is2lew/fZmJeKLEdjvn/IaWDQgCq5TDn4cgLp4kQMtbAsddjoEWqxeBqwbgg5VAp5wZyjQIDAQAB", RSAUtils.GetKey(pub_Key_File));
            //byte[] obj = Convert.FromBase64String(RSAUtils.GetKey(pub_Key_File));
        }

        [TestMethod]
        public void Test_Util_parseDate1()
        {
            DBConnection conn = new DBConnection();
            //bool flag = conn.connect("192.168.100.15",11040);
            bool flag = conn.connect("﻿192.168.1.167", 18921, "admin", "123456", "", false, null);
            int a = 1;
        }
    }
}
