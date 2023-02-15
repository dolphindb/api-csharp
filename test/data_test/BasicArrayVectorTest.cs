﻿using System;
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
using dolphindb.data;
using dolphindb_config;

namespace dolphindb_csharp_api_test.data_test
{
    [TestClass]
    public class BasicArrayVectorTest
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;


        static void compareArrayVector(BasicArrayVector t1, BasicArrayVector t2)
        {
            int row = t1.rows();
            for(int i = 0; i < row; ++i)
            {
                IVector v1 = t1.getSubVector(i);
                IVector v2 = t2.getSubVector(i);
                for(int j = 0; j < v1.rows(); ++j)
                {
                    int failCase = 0;
                    AbstractScalar e1 = (AbstractScalar)v1.get(j);
                    AbstractScalar e2 = (AbstractScalar)v2.get(j);
                    if (e1.Equals(e2) == false)
                    {
                        Console.WriteLine("Column " + i + ", row " + j + " expected: " + e1.getString() + " actual: " + e2.getString());
                        failCase++;
                    }
                    Assert.AreEqual(0, failCase);
                }
            }
        }

        static void compareArrayVectorWithAnyVector(BasicArrayVector t1, BasicAnyVector t2)
        {
            Assert.AreEqual(t1.rows(), t2.rows());
            int len = t1.rows();
            for(int i = 0; i < len; i++)
            {
                AbstractVector v1 = (AbstractVector)t1.getSubVector(i);
                AbstractVector v2 = (AbstractVector)t2.getEntity(i);
                int subLen = v1.rows();
                if (v1.Equals(v2)==false)
                {
                    if (v1.ToString().Equals(v2.ToString()) == false)
                    {
                        for (int j = 0; j < subLen; j++)
                        {
                            int failCase = 0;
                            AbstractScalar e1 = (AbstractScalar)v1.get(j);
                            AbstractScalar e2 = (AbstractScalar)v2.get(j);
                            if (e1.Equals(e2) == false)
                            {
                                Console.WriteLine("subVector " + i + ", index " + j + " expected: " + e2.getString() + " actual: " + e1.getString());
                                failCase++;
                            }
                            Assert.AreEqual(0, failCase);
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void Test_download_bool_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(BOOL[], 0, 10); a.append!(arrayVector(1..20, take(bool(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);

            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(BOOL[], 0, 10); a.append!(arrayVector(1..20, rand([true, false, NULL], 20)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(BOOL[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(BOOL[], 0, 10); a.append!(arrayVector(1..20 * 10, rand([true, false, NULL], 200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(BOOL[], 0, 10); a.append!(arrayVector(1..20 * 10000, rand([true, false, NULL], 200000)));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(BOOL[], 0, 10); a.append!(arrayVector(1..20 * 100000, rand([true, false, NULL], 2000000)));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(BOOL[], 0, 10); a.append!(arrayVector(index, rand([true, false, NULL], last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_int_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(INT[], 0, 10); a.append!(arrayVector(1..20, take(int(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);

            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(INT[], 0, 10); a.append!(arrayVector(1..20, 1..20));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(INT[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(INT[], 0, 10); a.append!(arrayVector(1..20 * 10, 1..200));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(INT[], 0, 10); a.append!(arrayVector(1..20 * 10000, 1..200000));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(INT[], 0, 10); a.append!(arrayVector(1..20 * 100000, 1..2000000));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(INT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_short_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(SHORT[], 0, 10); a.append!(arrayVector(1..20, take(short(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);

            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(SHORT[], 0, 10); a.append!(arrayVector(1..20, short(1..20)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(SHORT[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(SHORT[], 0, 10); a.append!(arrayVector(1..20 * 10, short(1..200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(SHORT[], 0, 10); a.append!(arrayVector(1..20 * 10000, short(rand(-100..100 join NULL, 200000))));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(SHORT[], 0, 10); a.append!(arrayVector(1..20 * 100000, short(rand(-100..100 join NULL, 2000000))));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_long_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(LONG[], 0, 10); a.append!(arrayVector(1..20, take(long(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);

            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(LONG[], 0, 10); a.append!(arrayVector(1..20, long(1..20)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(LONG[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(LONG[], 0, 10); a.append!(arrayVector(1..20 * 10, long(1..200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(LONG[], 0, 10); a.append!(arrayVector(1..20 * 10000, long(rand(-100..100 join NULL, 200000))));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(LONG[], 0, 10); a.append!(arrayVector(1..20 * 100000, long(rand(-100..100 join NULL, 2000000))));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_date_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(DATE[], 0, 10); a.append!(arrayVector(1..20, take(date(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);

            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(DATE[], 0, 10); a.append!(arrayVector(1..20, date(1..20)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(DATE[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(DATE[], 0, 10); a.append!(arrayVector(1..20 * 10, date(1..200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(DATE[], 0, 10); a.append!(arrayVector(1..20 * 10000, date(rand(-100..100 join NULL, 200000))));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(DATE[], 0, 10); a.append!(arrayVector(1..20 * 100000, date(rand(-100..100 join NULL, 2000000))));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_month_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(MONTH[], 0, 10); a.append!(arrayVector(1..20, take(month(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);

            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(MONTH[], 0, 10); a.append!(arrayVector(1..20, month(21..40)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(MONTH[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(MONTH[], 0, 10); a.append!(arrayVector(1..20 * 10, month(date(1..200))));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(MONTH[], 0, 10); a.append!(arrayVector(1..20 * 10000, month(date(rand(-100..100 join NULL, 200000)))));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(MONTH[], 0, 10); a.append!(arrayVector(1..20 * 100000, month(date(rand(-100..100 join NULL, 2000000)))));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_time_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(TIME[], 0, 10); a.append!(arrayVector(1..20, take(time(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);

            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(TIME[], 0, 10); a.append!(arrayVector(1..20, time(21..40)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(TIME[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(TIME[], 0, 10); a.append!(arrayVector(1..20 * 10, time(1..200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(TIME[], 0, 10); a.append!(arrayVector(1..20 * 10000, time(rand(0..100 join NULL, 200000))));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(TIME[], 0, 10); a.append!(arrayVector(1..20 * 100000, time(rand(0..100 join NULL, 2000000))));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_minute_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(MINUTE[], 0, 10); a.append!(arrayVector(1..20, take(minute(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);

            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(MINUTE[], 0, 10); a.append!(arrayVector(1..20, minute(21..40)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(MINUTE[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(MINUTE[], 0, 10); a.append!(arrayVector(1..20 * 10, minute(1..200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(MINUTE[], 0, 10); a.append!(arrayVector(1..20 * 10000, minute(rand(0..100 join NULL, 200000))));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(MINUTE[], 0, 10); a.append!(arrayVector(1..20 * 100000, minute(rand(0..100 join NULL, 2000000))));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_second_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(SECOND[], 0, 10); a.append!(arrayVector(1..20, take(second(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);

            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(SECOND[], 0, 10); a.append!(arrayVector(1..20, second(21..40)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(SECOND[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(SECOND[], 0, 10); a.append!(arrayVector(1..20 * 10, second(1..200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(SECOND[], 0, 10); a.append!(arrayVector(1..20 * 10000, second(rand(0..100 join NULL, 200000))));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(SECOND[], 0, 10); a.append!(arrayVector(1..20 * 100000, second(rand(0..100 join NULL, 2000000))));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_datetime_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(DATETIME[], 0, 10); a.append!(arrayVector(1..20, take(datetime(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);

            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(DATETIME[], 0, 10); a.append!(arrayVector(1..20, datetime(21..40)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(DATETIME[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(DATETIME[], 0, 10); a.append!(arrayVector(1..20 * 10, datetime(1..200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(DATETIME[], 0, 10); a.append!(arrayVector(1..20 * 10000, datetime(rand(-100..100 join NULL, 200000))));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(DATETIME[], 0, 10); a.append!(arrayVector(1..20 * 100000, datetime(rand(-100..100 join NULL, 2000000))));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_timestamp_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(TIMESTAMP[], 0, 10); a.append!(arrayVector(1..20, take(timestamp(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);

            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(TIMESTAMP[], 0, 10); a.append!(arrayVector(1..20, timestamp(21..40)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(TIMESTAMP[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(TIMESTAMP[], 0, 10); a.append!(arrayVector(1..20 * 10, timestamp(1..200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(TIMESTAMP[], 0, 10); a.append!(arrayVector(1..20 * 10000, timestamp(rand(-100..100 join NULL, 200000))));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(TIMESTAMP[], 0, 10); a.append!(arrayVector(1..20 * 100000, timestamp(rand(-100..100 join NULL, 2000000))));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_nanotime_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(NANOTIME[], 0, 10); a.append!(arrayVector(1..20, take(nanotime(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);


            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(NANOTIME[], 0, 10); a.append!(arrayVector(1..20, nanotime(21..40)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(NANOTIME[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(NANOTIME[], 0, 10); a.append!(arrayVector(1..20 * 10, nanotime(1..200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(NANOTIME[], 0, 10); a.append!(arrayVector(1..20 * 10000, nanotime(rand(0..100 join NULL, 200000))));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(NANOTIME[], 0, 10); a.append!(arrayVector(1..20 * 100000, nanotime(rand(0..100 join NULL, 2000000))));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_nanotimestamp_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(NANOTIMESTAMP[], 0, 10); a.append!(arrayVector(1..20, take(nanotimestamp(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);


            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(NANOTIMESTAMP[], 0, 10); a.append!(arrayVector(1..20, nanotimestamp(21..40)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(NANOTIMESTAMP[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(NANOTIMESTAMP[], 0, 10); a.append!(arrayVector(1..20 * 10, nanotimestamp(1..200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(NANOTIMESTAMP[], 0, 10); a.append!(arrayVector(1..20 * 10000, nanotimestamp(rand(0..100 join NULL, 200000))));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(NANOTIMESTAMP[], 0, 10); a.append!(arrayVector(1..20 * 100000, nanotimestamp(rand(0..100 join NULL, 2000000))));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_double_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(DOUBLE[], 0, 10); a.append!(arrayVector(1..20, take(double(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);


            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(DOUBLE[], 0, 10); a.append!(arrayVector(1..20, 21..40+0.1));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(DOUBLE[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(DOUBLE[], 0, 10); a.append!(arrayVector(1..20 * 10, 1..200+0.2));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(DOUBLE[], 0, 10); a.append!(arrayVector(1..20 * 10000, rand(0..100 join NULL, 200000)+0.1));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(DOUBLE[], 0, 10); a.append!(arrayVector(1..20 * 100000, rand(0..100 join NULL, 2000000)+0.1));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_float_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(FLOAT[], 0, 10); a.append!(arrayVector(1..20, take(float(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);


            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(FLOAT[], 0, 10); a.append!(arrayVector(1..20, 21..40+0.1f));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(FLOAT[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(FLOAT[], 0, 10); a.append!(arrayVector(1..20 * 10, 1..200+0.2f));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(FLOAT[], 0, 10); a.append!(arrayVector(1..20 * 10000, rand(0..100 join NULL, 200000)+0.1f));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(FLOAT[], 0, 10); a.append!(arrayVector(1..20 * 100000, rand(0..100 join NULL, 2000000)+0.1f));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_uuid_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(UUID[], 0, 10); a.append!(arrayVector(1..20, take(uuid(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);


            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(UUID[], 0, 10); a.append!(arrayVector(1..20, rand(uuid(), 20)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(UUID[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(UUID[], 0, 10); a.append!(arrayVector(1..20 * 10, rand(uuid(), 200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(UUID[], 0, 10); a.append!(arrayVector(1..20 * 10000, rand(uuid(), 200000)));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(UUID[], 0, 10); a.append!(arrayVector(1..20 * 100000, rand(uuid(), 2000000)));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }
        //[TestMethod]//not support
        public void Test_download_decimal64_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(DECIMAL64(4)[], 0, 10); a.append!(arrayVector(1..20, take(decimal64(4,4), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);


            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(UUID[], 0, 10); a.append!(arrayVector(1..20, rand(uuid(), 20)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(UUID[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(UUID[], 0, 10); a.append!(arrayVector(1..20 * 10, rand(uuid(), 200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(UUID[], 0, 10); a.append!(arrayVector(1..20 * 10000, rand(uuid(), 200000)));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(UUID[], 0, 10); a.append!(arrayVector(1..20 * 100000, rand(uuid(), 2000000)));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_ipaddr_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(IPADDR[], 0, 10); a.append!(arrayVector(1..20, take(ipaddr(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);


            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(IPADDR[], 0, 10); a.append!(arrayVector(1..20, rand(ipaddr(), 20)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(IPADDR[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(IPADDR[], 0, 10); a.append!(arrayVector(1..20 * 10, rand(ipaddr(), 200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(IPADDR[], 0, 10); a.append!(arrayVector(1..20 * 10000, rand(ipaddr(), 200000)));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(IPADDR[], 0, 10); a.append!(arrayVector(1..20 * 100000, rand(ipaddr(), 2000000)));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_int128_array_vector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a1 = (BasicArrayVector)conn.run("a = array(INT128[], 0, 10); a.append!(arrayVector(1..20, take(int128(), 20)));a;");
            BasicAnyVector ex1 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a1, ex1);


            //elements scalar not null
            BasicArrayVector a2 = (BasicArrayVector)conn.run("a = array(INT128[], 0, 10); a.append!(arrayVector(1..20, rand(int128(), 20)));a;");
            BasicAnyVector ex2 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a2, ex2);

            //array vector len 0
            BasicArrayVector a3 = (BasicArrayVector)conn.run("a = array(INT128[], 0, 10); a;");
            Assert.AreEqual(a3.rows(), 0);

            //array vector len<256
            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(INT128[], 0, 10); a.append!(arrayVector(1..20 * 10, rand(int128(), 200)));a;");
            BasicAnyVector ex4 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a4, ex4);

            //array vector 256<=len<65535
            BasicArrayVector a5 = (BasicArrayVector)conn.run("a = array(INT128[], 0, 10); a.append!(arrayVector(1..20 * 10000, rand(int128(), 200000)));a;");
            BasicAnyVector ex5 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a5, ex5);

            //array vector len>=65535
            BasicArrayVector a6 = (BasicArrayVector)conn.run("a = array(INT128[], 0, 10); a.append!(arrayVector(1..20 * 100000, rand(int128(), 2000000)));a;");
            BasicAnyVector ex6 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            compareArrayVectorWithAnyVector(a6, ex6);

            //len inconsistent
            //BasicArrayVector a7 = (BasicArrayVector)conn.run("index = int(cumsum(rand(1..10000, 10)));a = array(SHORT[], 0, 10); a.append!(arrayVector(index, rand(100, last(index))));a;");
            //BasicAnyVector ex7 = (BasicAnyVector)conn.run("loop(def (x):x, a)");
            //compareArrayVectorWithAnyVector(a7, ex7);

        }

        [TestMethod]
        public void Test_download_array_vector_from_in_memory_table_len_smaller_1024()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            String script1 = " ";
            script1 += "n=1000;bidpricev = arrayVector(1..n*20, rand(1000, n*20));bidvolumev = arrayVector(1..n*20, rand(1000, n*20));askpricev = arrayVector(1..n*20, rand(1000, n*20));askvolumev = arrayVector(1..n*20, rand(1000, n*20));";
            script1 += "t = table(1..n as id, 'AA'+string(1..n) as sym, bidpricev as bidprice, bidvolumev as bidvolume, askpricev as askprice, askvolumev as askvolume);t";
            BasicTable re = (BasicTable)conn.run(script1);
            BasicIntVector colid = (BasicIntVector)re.getColumn(0);
            BasicStringVector colsym = (BasicStringVector)re.getColumn(1);
            for (int i = 0; i < 1000; i++)
            {
                Assert.AreEqual(((BasicInt)colid.get(i)).getValue(), i + 1);
                Assert.AreEqual(((BasicString)colsym.get(i)).getValue(), "AA"+(i + 1).ToString());
            }
            BasicArrayVector colbidprice = (BasicArrayVector)re.getColumn(2);
            BasicAnyVector colbidpriceex = (BasicAnyVector)conn.run("loop(def (x):x, bidpricev)");
            compareArrayVectorWithAnyVector(colbidprice, colbidpriceex);

            BasicArrayVector colbidvolume = (BasicArrayVector)re.getColumn(3);
            BasicAnyVector colbidvolumeex = (BasicAnyVector)conn.run("loop(def (x):x, bidvolumev)");
            compareArrayVectorWithAnyVector(colbidvolume, colbidvolumeex);

            BasicArrayVector colaskprice = (BasicArrayVector)re.getColumn(4);
            BasicAnyVector colaskpriceex = (BasicAnyVector)conn.run("loop(def (x):x, askpricev)");
            compareArrayVectorWithAnyVector(colaskprice, colaskpriceex);

            BasicArrayVector colaskvolume = (BasicArrayVector)re.getColumn(5);
            BasicAnyVector colaskvolumeex = (BasicAnyVector)conn.run("loop(def (x):x, askvolumev)");
            compareArrayVectorWithAnyVector(colaskvolume, colaskvolumeex);

        }

        [TestMethod]
        public void Test_download_array_vector_from_in_memory_table_len_larger_1024()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            String script1 = " ";
            script1 += "n=5000;bidpricev = arrayVector(1..n*20, rand(1000, n*20));bidvolumev = arrayVector(1..n*20, rand(1000, n*20));askpricev = arrayVector(1..n*20, rand(1000, n*20));askvolumev = arrayVector(1..n*20, rand(1000, n*20));";
            script1 += "t = table(1..n as id, 'AA'+string(1..n) as sym, bidpricev as bidprice, bidvolumev as bidvolume, askpricev as askprice, askvolumev as askvolume);t";
            BasicTable re = (BasicTable)conn.run(script1);
            BasicIntVector colid = (BasicIntVector)re.getColumn(0);
            BasicStringVector colsym = (BasicStringVector)re.getColumn(1);
            for (int i = 0; i < 5000; i++)
            {
                Assert.AreEqual(((BasicInt)colid.get(i)).getValue(), i + 1);
                Assert.AreEqual(((BasicString)colsym.get(i)).getValue(), "AA" + (i + 1).ToString());
            }
            BasicArrayVector colbidprice = (BasicArrayVector)re.getColumn(2);
            BasicAnyVector colbidpriceex = (BasicAnyVector)conn.run("loop(def (x):x, bidpricev)");
            compareArrayVectorWithAnyVector(colbidprice, colbidpriceex);

            BasicArrayVector colbidvolume = (BasicArrayVector)re.getColumn(3);
            BasicAnyVector colbidvolumeex = (BasicAnyVector)conn.run("loop(def (x):x, bidvolumev)");
            compareArrayVectorWithAnyVector(colbidvolume, colbidvolumeex);

            BasicArrayVector colaskprice = (BasicArrayVector)re.getColumn(4);
            BasicAnyVector colaskpriceex = (BasicAnyVector)conn.run("loop(def (x):x, askpricev)");
            compareArrayVectorWithAnyVector(colaskprice, colaskpriceex);

            BasicArrayVector colaskvolume = (BasicArrayVector)re.getColumn(5);
            BasicAnyVector colaskvolumeex = (BasicAnyVector)conn.run("loop(def (x):x, askvolumev)");
            compareArrayVectorWithAnyVector(colaskvolume, colaskvolumeex);

        }

        [TestMethod]
        public void Test_download_array_vector_from_in_memory_table_len_larger_1048576()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            String script1 = " ";
            script1 += "n=1048576;bidpricev = arrayVector(1..n*20, rand(1000, n*20));bidvolumev = arrayVector(1..n*20, rand(1000, n*20));askpricev = arrayVector(1..n*20, rand(1000, n*20));askvolumev = arrayVector(1..n*20, rand(1000, n*20));";
            script1 += "t = table(1..n as id, 'AA'+string(1..n) as sym, bidpricev as bidprice, bidvolumev as bidvolume, askpricev as askprice, askvolumev as askvolume);t";
            BasicTable re = (BasicTable)conn.run(script1);
            BasicIntVector colid = (BasicIntVector)re.getColumn(0);
            BasicStringVector colsym = (BasicStringVector)re.getColumn(1);
            for (int i = 0; i < 1048576; i++)
            {
                Assert.AreEqual(((BasicInt)colid.get(i)).getValue(), i + 1);
                Assert.AreEqual(((BasicString)colsym.get(i)).getValue(), "AA" + (i + 1).ToString());
            }
            BasicArrayVector colbidprice = (BasicArrayVector)re.getColumn(2);
            BasicAnyVector colbidpriceex = (BasicAnyVector)conn.run("loop(def (x):x, bidpricev)");
            compareArrayVectorWithAnyVector(colbidprice, colbidpriceex);

            BasicArrayVector colbidvolume = (BasicArrayVector)re.getColumn(3);
            BasicAnyVector colbidvolumeex = (BasicAnyVector)conn.run("loop(def (x):x, bidvolumev)");
            compareArrayVectorWithAnyVector(colbidvolume, colbidvolumeex);

            BasicArrayVector colaskprice = (BasicArrayVector)re.getColumn(4);
            BasicAnyVector colaskpriceex = (BasicAnyVector)conn.run("loop(def (x):x, askpricev)");
            compareArrayVectorWithAnyVector(colaskprice, colaskpriceex);

            BasicArrayVector colaskvolume = (BasicArrayVector)re.getColumn(5);
            BasicAnyVector colaskvolumeex = (BasicAnyVector)conn.run("loop(def (x):x, askvolumev)");
            compareArrayVectorWithAnyVector(colaskvolume, colaskvolumeex);

        }

        [TestMethod]
        public void Test_upload_array_vector_bool()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            byte[] value1 = new byte[100];
            for (int i = 0; i < 100; i++)
            {
                if (i % 2 == 0)
                {
                    value1[i] = 0;
                }
                else
                {
                    value1[i] = 1;
                }
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicBooleanVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, take(false true, 100)))");
            Assert.AreEqual(re1.getValue(), true);
            conn.run("undef(`x)");

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            byte[] value2 = new byte[100000];
            for (int i = 0; i < 100000; i++)
            {
                if (i % 2 == 0)
                {
                    value2[i] = 0;
                }
                else
                {
                    value2[i] = 1;
                }
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicBooleanVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, take(false true, 100000)))");
            Assert.AreEqual(re2.getValue(), true);
            conn.run("undef(`x)");

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            byte[] value3 = new byte[1000000];
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    value3[i] = 0;
                }
                else
                {
                    value3[i] = 1;
                }
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicBooleanVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, take(false true, 1000000)))");
            Assert.AreEqual(re3.getValue(), true);
            conn.run("undef(`x)");
            conn.close();

        }

        [TestMethod]
        public void Test_upload_array_vector_int()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            int[] value1 = new int[100];
            for (int i = 0; i < 100; i++)
            {
                value1[i] = i + 1;
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicIntVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, 1..100))");
            Assert.AreEqual(re1.getValue(), true);

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            int[] value2 = new int[100000];
            for (int i = 0; i < 100000; i++)
            {
                value2[i] = i + 1;
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicIntVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, 1..100000))");
            Assert.AreEqual(re2.getValue(), true);

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            int[] value3 = new int[1000000];
            for (int i = 0; i < 1000000; i++)
            {
                value3[i] = i + 1;
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicIntVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, 1..1000000))");
            Assert.AreEqual(re3.getValue(), true);
            conn.close();

        }

        [TestMethod]
        public void Test_upload_array_vector_short()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            short[] value1 = new short[100];
            for (int i = 0; i < 100; i++)
            {
                value1[i] = (short)(i + 1);
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicShortVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, short(1..100)))");
            Assert.AreEqual(re1.getValue(), true);

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            short[] value2 = new short[100000];
            for (int i = 0; i < 100000; i++)
            {
                value2[i] = (short)((i + 1)%99);
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicShortVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, short(1..100000%99)))");
            Assert.AreEqual(re2.getValue(), true);

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            short[] value3 = new short[1000000];
            for (int i = 0; i < 1000000; i++)
            {
                value3[i] = (short)((i + 1) % 99);
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicShortVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, short(1..1000000%99)))");
            Assert.AreEqual(re3.getValue(), true);
            conn.close();

        }

        [TestMethod]
        public void Test_upload_array_vector_long()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            long[] value1 = new long[100];
            for (int i = 0; i < 100; i++)
            {
                value1[i] = i + 1;
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicLongVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, long(1..100)))");
            Assert.AreEqual(re1.getValue(), true);

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            long[] value2 = new long[100000];
            for (int i = 0; i < 100000; i++)
            {
                value2[i] = i + 1;
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicLongVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, long(1..100000)))");
            Assert.AreEqual(re2.getValue(), true);

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            long[] value3 = new long[1000000];
            for (int i = 0; i < 1000000; i++)
            {
                value3[i] = i + 1;
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicLongVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, long(1..1000000)))");
            Assert.AreEqual(re3.getValue(), true);
            conn.close();

        }

        [TestMethod]
        public void Test_upload_array_vector_date()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            int[] value1 = new int[100];
            for (int i = 0; i < 100; i++)
            {
                value1[i] = Utils.countDays(new DateTime(2018, 2, 14))+i;
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicDateVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, 2018.02.14+0..99))");
            Assert.AreEqual(re1.getValue(), true);

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            int[] value2 = new int[100000];
            for (int i = 0; i < 100000; i++)
            {
                value2[i] = Utils.countDays(new DateTime(1970, 2, 14)) + i;
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicDateVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, 1970.02.14+0..99999))");
            Assert.AreEqual(re2.getValue(), true);

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            int[] value3 = new int[1000000];
            for (int i = 0; i < 1000000; i++)
            {
                value3[i] = Utils.countDays(new DateTime(1970, 1, 1)) + i;
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicDateVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, 1970.01.01+0..999999))");
            Assert.AreEqual(re3.getValue(), true);
            conn.close();

        }

        [TestMethod]
        public void Test_upload_array_vector_month()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            int[] value1 = new int[100];
            for (int i = 20; i < 120; i++)
            {
                value1[i-20] = i;
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicMonthVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, month(20..119)))");
            Assert.AreEqual(re1.getValue(), true);

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            int[] value2 = new int[100000];
            for (int i = 20; i < 100020; i++)
            {
                value2[i-20] = i%60;
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicMonthVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, month(20..100019 % 60)))");
            Assert.AreEqual(re2.getValue(), true);

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            int[] value3 = new int[1000000];
            for (int i = 20; i < 1000020; i++)
            {
                value3[i-20] = i%60;
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicMonthVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, month(20..1000019 % 60)))");
            Assert.AreEqual(re3.getValue(), true);
            conn.close();

        }

        [TestMethod]
        public void Test_upload_array_vector_time()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            int[] value1 = new int[100];
            for (int i = 20; i < 120; i++)
            {
                value1[i - 20] = i;
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicTimeVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, time(20..119)))");
            Assert.AreEqual(re1.getValue(), true);

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            int[] value2 = new int[100000];
            for (int i = 20; i < 100020; i++)
            {
                value2[i - 20] = i;
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicTimeVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, time(20..100019)))");
            Assert.AreEqual(re2.getValue(), true);

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            int[] value3 = new int[1000000];
            for (int i = 20; i < 1000020; i++)
            {
                value3[i - 20] = i % 60;
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicTimeVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, time(20..1000019 % 60)))");
            Assert.AreEqual(re3.getValue(), true);
            conn.close();

        }

        [TestMethod]
        public void Test_upload_array_vector_minute()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            int[] value1 = new int[100];
            for (int i = 20; i < 120; i++)
            {
                value1[i - 20] = i;
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicMinuteVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, minute(20..119)))");
            Assert.AreEqual(re1.getValue(), true);

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            int[] value2 = new int[100000];
            for (int i = 20; i < 100020; i++)
            {
                value2[i - 20] = i%60;
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicMinuteVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, minute(20..100019 % 60)))");
            Assert.AreEqual(re2.getValue(), true);

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            int[] value3 = new int[1000000];
            for (int i = 20; i < 1000020; i++)
            {
                value3[i - 20] = i % 60;
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicMinuteVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, minute(20..1000019 % 60)))");
            Assert.AreEqual(re3.getValue(), true);
            conn.close();

        }

        [TestMethod]
        public void Test_upload_array_vector_second()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            int[] value1 = new int[100];
            for (int i = 20; i < 120; i++)
            {
                value1[i - 20] = i;
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicSecondVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, second(20..119)))");
            Assert.AreEqual(re1.getValue(), true);

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            int[] value2 = new int[100000];
            for (int i = 20; i < 100020; i++)
            {
                value2[i - 20] = i % 1000;
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicSecondVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, second(20..100019 % 1000)))");
            Assert.AreEqual(re2.getValue(), true);

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            int[] value3 = new int[1000000];
            for (int i = 20; i < 1000020; i++)
            {
                value3[i - 20] = i % 1000;
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicSecondVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, second(20..1000019 % 1000)))");
            Assert.AreEqual(re3.getValue(), true);
            conn.close();

        }

        [TestMethod]
        public void Test_upload_array_vector_datetime()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            int[] value1 = new int[100];
            for (int i = 20; i < 120; i++)
            {
                value1[i - 20] = i;
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicDateTimeVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, datetime(20..119)))");
            Assert.AreEqual(re1.getValue(), true);

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            int[] value2 = new int[100000];
            for (int i = 20; i < 100020; i++)
            {
                value2[i - 20] = i;
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicDateTimeVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, datetime(20..100019)))");
            Assert.AreEqual(re2.getValue(), true);

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            int[] value3 = new int[1000000];
            for (int i = 20; i < 1000020; i++)
            {
                value3[i - 20] = i;
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicDateTimeVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, datetime(20..1000019)))");
            Assert.AreEqual(re3.getValue(), true);
            conn.close();

        }

        [TestMethod]
        public void Test_upload_array_vector_timestamp()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            long[] value1 = new long[100];
            for (int i = 20; i < 120; i++)
            {
                value1[i - 20] = i;
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicTimestampVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, timestamp(20..119)))");
            Assert.AreEqual(re1.getValue(), true);

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            long[] value2 = new long[100000];
            for (int i = 20; i < 100020; i++)
            {
                value2[i - 20] = i;
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicTimestampVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, timestamp(20..100019)))");
            Assert.AreEqual(re2.getValue(), true);

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            long[] value3 = new long[1000000];
            for (int i = 20; i < 1000020; i++)
            {
                value3[i - 20] = i;
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicTimestampVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, timestamp(20..1000019)))");
            Assert.AreEqual(re3.getValue(), true);
            conn.close();

        }

        [TestMethod]
        public void Test_upload_array_vector_nanotime()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            long[] value1 = new long[100];
            for (int i = 20; i < 120; i++)
            {
                value1[i - 20] = i;
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicNanoTimeVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, nanotime(20..119)))");
            Assert.AreEqual(re1.getValue(), true);

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            long[] value2 = new long[100000];
            for (int i = 20; i < 100020; i++)
            {
                value2[i - 20] = i;
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicNanoTimeVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, nanotime(20..100019)))");
            Assert.AreEqual(re2.getValue(), true);

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            long[] value3 = new long[1000000];
            for (int i = 20; i < 1000020; i++)
            {
                value3[i - 20] = i;
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicNanoTimeVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, nanotime(20..1000019)))");
            Assert.AreEqual(re3.getValue(), true);
            conn.close();

        }

        [TestMethod]
        public void Test_upload_array_vector_nanotimestamp()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            long[] value1 = new long[100];
            for (int i = 20; i < 120; i++)
            {
                value1[i - 20] = i;
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicNanoTimestampVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, nanotimestamp(20..119)))");
            Assert.AreEqual(re1.getValue(), true);

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            long[] value2 = new long[100000];
            for (int i = 20; i < 100020; i++)
            {
                value2[i - 20] = i;
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicNanoTimestampVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, nanotimestamp(20..100019)))");
            Assert.AreEqual(re2.getValue(), true);

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            long[] value3 = new long[1000000];
            for (int i = 20; i < 1000020; i++)
            {
                value3[i - 20] = i;
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicNanoTimestampVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, nanotimestamp(20..1000019)))");
            Assert.AreEqual(re3.getValue(), true);
            conn.close();

        }


        [TestMethod]
        public void Test_upload_array_vector_float()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            float[] value1 = new float[100];
            for (int i = 20; i < 120; i++)
            {
                value1[i - 20] = i;
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicFloatVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, float(20..119)))");
            Assert.AreEqual(re1.getValue(), true);

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            float[] value2 = new float[100000];
            for (int i = 20; i < 100020; i++)
            {
                value2[i - 20] = i;
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicFloatVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, float(20..100019)))");
            Assert.AreEqual(re2.getValue(), true);

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            float[] value3 = new float[1000000];
            for (int i = 20; i < 1000020; i++)
            {
                value3[i - 20] = i;
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicFloatVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, float(20..1000019)))");
            Assert.AreEqual(re3.getValue(), true);
            conn.close();

        }

        [TestMethod]
        public void Test_upload_array_vector_double()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            //len<256
            int[] index1 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            double[] value1 = new double[100];
            for (int i = 20; i < 120; i++)
            {
                value1[i - 20] = i;
            }
            BasicArrayVector a1 = new BasicArrayVector(index1, new BasicDoubleVector(value1));
            Dictionary<String, IEntity> var1 = new Dictionary<string, IEntity>();
            var1.Add("x", a1);
            conn.upload(var1);
            BasicBoolean re1 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10, double(20..119)))");
            Assert.AreEqual(re1.getValue(), true);

            //256<=len<65535
            int[] index2 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index2[i] = (i + 1) * 10000;
            }
            double[] value2 = new double[100000];
            for (int i = 20; i < 100020; i++)
            {
                value2[i - 20] = i;
            }
            BasicArrayVector a2 = new BasicArrayVector(index2, new BasicDoubleVector(value2));
            Dictionary<String, IEntity> var2 = new Dictionary<string, IEntity>();
            var2.Add("x", a2);
            conn.upload(var2);
            BasicBoolean re2 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *10000, double(20..100019)))");
            Assert.AreEqual(re2.getValue(), true);

            //len>=65535
            int[] index3 = new int[10];
            for (int i = 0; i < 10; i++)
            {
                index3[i] = (i + 1) * 100000;
            }
            double[] value3 = new double[1000000];
            for (int i = 20; i < 1000020; i++)
            {
                value3[i - 20] = i;
            }
            BasicArrayVector a3 = new BasicArrayVector(index3, new BasicDoubleVector(value3));
            Dictionary<String, IEntity> var3 = new Dictionary<string, IEntity>();
            var3.Add("x", a3);
            conn.upload(var3);
            BasicBoolean re3 = (BasicBoolean)conn.run("eqObj(x, arrayVector(1..10 *100000, double(20..1000019)))");
            Assert.AreEqual(re3.getValue(), true);
            conn.close();

        }

        [TestMethod]
        public void Test_upload_array_vector_int128()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(INT128[], 0, 10); a.append!(arrayVector(1..20 * 1000, rand(int128(), 20000)));a;");
            Dictionary<String, IEntity> var = new Dictionary<string, IEntity>();
            var.Add("x", a4);
            conn.upload(var);
            BasicBoolean re = (BasicBoolean)conn.run("eqObj(a, x)");
            Assert.AreEqual(re.getValue(), true);
        }

        [TestMethod]
        public void Test_upload_array_vector_ipaddr()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(IPADDR[], 0, 10); a.append!(arrayVector(1..20 * 1000, rand(ipaddr(), 20000)));a;");
            Dictionary<String, IEntity> var = new Dictionary<string, IEntity>();
            var.Add("x", a4);
            conn.upload(var);
            BasicBoolean re = (BasicBoolean)conn.run("eqObj(a, x)");
            Assert.AreEqual(re.getValue(), true);
        }

        [TestMethod]
        public void Test_upload_array_vector_uuid()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            BasicArrayVector a4 = (BasicArrayVector)conn.run("a = array(UUID[], 0, 10); a.append!(arrayVector(1..20 * 1000, rand(uuid(), 20000)));a;");
            Dictionary<String, IEntity> var = new Dictionary<string, IEntity>();
            var.Add("x", a4);
            conn.upload(var);
            BasicBoolean re = (BasicBoolean)conn.run("eqObj(a, x)");
            Assert.AreEqual(re.getValue(), true);
        }

        [TestMethod]
        public void Test_tableInsert_array_vector_in_memory_table()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            String script = " try{undef(`st, SHARED)}catch(ex){};";
            script += "share table(1000:0, `id`sym`bidprice`bidvolume, [INT, SYMBOL, INT[], INT[]]) as st";
            conn.run(script);
            int[] v1 = new int[100];
            string[] v2 = new string[100];
            for (int i = 0; i < 100; i++)
            {
                v1[i] = i;
                v2[i] = "A" + i.ToString();
            }
            BasicIntVector col1 = new BasicIntVector(v1);
            BasicSymbolVector col2 = new BasicSymbolVector(v2);
            int[] index1 = new int[100];
            for (int i = 0; i < 100; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            int[] value1 = new int[1000];
            for (int i = 0; i < 1000; i++)
            {
                value1[i] = i;
            }
            BasicArrayVector col3 = new BasicArrayVector(index1, new BasicIntVector(value1));
            int[] index2 = new int[100];
            for (int i = 0; i < 100; i++)
            {
                index2[i] = (i + 1) * 10;
            }
            int[] value2 = new int[1000];
            for (int i = 0; i < 1000; i++)
            {
                value2[i] = i;
            }
            BasicArrayVector col4 = new BasicArrayVector(index2, new BasicIntVector(value2));
            List<IEntity> args = new List<IEntity>() { col1, col2, col3, col4 };
            List<IVector> args1 = new List<IVector>() { col1, col2, col3, col4 };
            conn.run("tableInsert{st}", args);
            BasicInt total = (BasicInt)conn.run("exec count(*) from st");
            Assert.AreEqual(total.getValue(), 100);
            BasicTable re = (BasicTable)conn.run("st");
            BasicArrayVector x = (BasicArrayVector)re.getColumn(2);
            int[] index3 = new int[100];
            try
            {
                BasicArrayVector col5 = new BasicArrayVector(index3, new BasicIntVector(value2));

            }
            catch (Exception ex)
            {
                string s = ex.Message;
                Assert.AreEqual("The last element of index must be equal to the length of value. ", s);

            }
            conn.run("undef(`st, SHARED)");

        }

        [TestMethod]
        public void Test_tableInsert_array_vector_in_tsdb_table()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");

            String script = " ";
            script += "dbName='dfs://test_arrayVector';tableName = 'pt';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db=database(dbName, VALUE, 1..10, engine='TSDB');";
            script += "dummy = table(1000:0, `id`sym`bidprice`bidvolume, [INT, SYMBOL, INT[], INT[]]);";
            script += "pt=db.createPartitionedTable(dummy, tableName, `id, , `id);";
            conn.run(script);
            int[] v1 = new int[100];
            string[] v2 = new string[100];
            for (int i = 0; i < 100; i++)
            {
                v1[i] = i%10;
                v2[i] = "A" + i.ToString();
            }
            BasicIntVector col1 = new BasicIntVector(v1);
            BasicSymbolVector col2 = new BasicSymbolVector(v2);
            int[] index1 = new int[100];
            for (int i = 0; i < 100; i++)
            {
                index1[i] = (i + 1) * 10;
            }
            int[] value1 = new int[1000];
            for (int i = 0; i < 1000; i++)
            {
                value1[i] = i;
            }
            BasicArrayVector col3 = new BasicArrayVector(index1, new BasicIntVector(value1));
            int[] index2 = new int[100];
            for (int i = 0; i < 100; i++)
            {
                index2[i] = (i + 1) * 10;
            }
            int[] value2 = new int[1000];
            for (int i = 0; i < 1000; i++)
            {
                value2[i] = i;
            }
            BasicArrayVector col4 = new BasicArrayVector(index2, new BasicIntVector(value2));
            List<String> colNames = new List<String>() {"id", "sym", "bidprice", "bidvolume" };
            List<IVector> cols = new List<IVector>() { col1, col2, col3, col4 };
            BasicTable t = new BasicTable(colNames, cols);
            List<IEntity> args = new List<IEntity>() { t };
            conn.run("tableInsert{loadTable(dbName, tableName)}", args);
            BasicLong total = (BasicLong)conn.run("exec count(*) from loadTable(dbName, tableName)");
            Assert.AreEqual(total.getValue(), 100);
            BasicTable re = (BasicTable)conn.run("select * from loadTable(dbName, tableName)");
            BasicArrayVector x = (BasicArrayVector)re.getColumn(2);
            conn.run("dropDatabase(dbName)");

        }

        [TestMethod]
        public void read_error_data()
        {

            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            BasicTable data = (BasicTable)db.run("loadText('/home/wsun/Downloads/api_error_data.csv')");
            //BasicTable data = (BasicTable)db.run("table(take('hc2210''hc22101', 100).symbol() as col1, take('hc2210''hc22101', 100).symbol() as col2)");
            int cols = data.columns();
            int rows = data.rows();
            for (int i = 0; i < cols; ++i)
            {
                IVector vec = data.getColumn(i);
                
                if (vec.getDataType() == DATA_TYPE.DT_SYMBOL)
                {
                    for (int j = 0; j < rows; ++j)
                    {
                        string t = ((BasicSymbolVector)vec).getString(j);
                    }
                }
            }
            db.close();
        }

        [TestMethod]
        public void Test_BasicArrayVector_Remain()
        {
            try
            {
                BasicArrayVector bav1 = new BasicArrayVector(new int[0], new BasicIntVector(new int[] { 1, 2, 3, 4 }));
            }
            catch(Exception e)
            {
                Assert.AreEqual("Index must be an array of size greater than 0.", e.Message);
                Console.WriteLine(e.Message);
            }

            BasicIntVector bi1 = new BasicIntVector(0);
            List<IVector> value1 = new List<IVector>();
            value1.Add(bi1);
            BasicArrayVector ba2 = new BasicArrayVector(value1);
        }
    }
}
