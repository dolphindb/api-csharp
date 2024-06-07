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

namespace dolphindb_csharp_api_test.data_test
{
    [TestClass]
    public class AbstractTest
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;

        public class BasicEntity : AbstractEntity
        {
            private DATA_FORM dataForm;
            public BasicEntity(DATA_FORM data_form)
            {
                this.dataForm = data_form;
            }
            public override DATA_FORM getDataForm()
            {
                return dataForm;
            }

        }

        public class BasicMatrix : AbstractMatrix
        {
            private int[] values = null;
            private DATA_TYPE dataType = DATA_TYPE.DT_VOID;

            public BasicMatrix(int rows, int columns) : base(rows, columns)
            {
            }

            public override IScalar get(int row, int column)
            {
                return new BasicInt(values[getIndex(row, column)]);
            }

            public override DATA_CATEGORY getDataCategory()
            {
                throw new NotImplementedException();
            }

            public override DATA_TYPE getDataType()
            {
                return dataType;
            }

            public override Type getElementClass()
            {
                throw new NotImplementedException();
            }

            public override bool isNull(int row, int column)
            {
                throw new NotImplementedException();
            }

            public override void setNull(int row, int column)
            {
                throw new NotImplementedException();
            }

            protected override void readMatrixFromInputStream(int rows, int columns, ExtendedDataInput @in)
            {
                throw new NotImplementedException();
            }

            protected override void writeVectorToOutputStream(ExtendedDataOutput @out)
            {
             
            }
            //public override void write(ExtendedDataOutput @out)
            //{
            //    throw new NotImplementedException();
            //}
        }
        public class BasicScalar : AbstractScalar
        {
                private int v;

                public BasicScalar(int v)
                {
                    this.v = v;
                }

                public override object getObject()
                {
                    throw new NotImplementedException();
                }

                public override string getString()
                {
                    throw new NotImplementedException();
                }

                public override int hashBucket(int buckets)
                {
                    throw new NotImplementedException();
                }

                public override void setObject(object value)
                {
                    throw new NotImplementedException();
                }

                public override void writeScalarToOutputStream(ExtendedDataOutput @out)
                {
                    throw new NotImplementedException();
                }
        }

            [TestMethod]
            public void test_AbstractEntity()
            {

                BasicEntity be = new BasicEntity(DATA_FORM.DF_PAIR);
                Assert.AreEqual("True", be.isPair().ToString());
                Assert.AreEqual("True", new BasicEntity(DATA_FORM.DF_SCALAR).isScalar().ToString());
                Assert.AreEqual("True", new BasicEntity(DATA_FORM.DF_CHART).isChart().ToString());
                Assert.AreEqual("True", new BasicEntity(DATA_FORM.DF_CHUNK).isChunk().ToString());
                Assert.AreEqual("True", new BasicEntity(DATA_FORM.DF_TABLE).isTable().ToString());

            }
            [TestMethod]
            public void test_AbstractMatrix()
            {
                BasicStringVector bsv = new BasicStringVector(new String[] { "MSFT", "GOOG", "META" });
                BasicMatrix bm = new BasicMatrix(3, 3);
                Assert.AreEqual("False", bm.hasRowLabel().ToString());
                Assert.AreEqual("False", bm.hasColumnLabel().ToString());
                bm.setRowLabels(bsv);
                Assert.AreEqual("MSFT", bm.getRowLabel(0).getString());
                Assert.AreEqual("[MSFT,GOOG,META]", bm.getRowLabels().getString());
                Assert.AreEqual("True", bm.hasRowLabel().ToString());
                BasicSymbolVector bsyv = new BasicSymbolVector(3);
                bsyv.set(0, "a");
                bsyv.set(1, "b");
                bsyv.set(2, "c");
                bm.setColumnLabels(bsyv);
                Assert.AreEqual("a", bm.getColumnLabel(0).getString());
                Assert.AreEqual("[a,b,c]", bm.getColumnLabels().getString());
                Exception exception = null;
                try
                {
                    bm.getObject();
                }
                catch (Exception e)
                {
                    exception = e;
                }
                Assert.AreEqual("The method or operation is not implemented.", exception.Message);

                BasicMatrix bm1 = new BasicMatrix(1, 1);
                Exception exception1 = null;
                try
                {
                    bm1.setColumnLabels(new BasicStringVector(new String[] { "XM", "OP" }));
                }
                catch (Exception e)
                {
                    exception1 = e;
                }
                Assert.AreEqual("the column label size doesn't equal to the column number of the matrix.", exception1.Message);
                Exception exception2 = null;
                try
                {
                    bm1.setRowLabels(new BasicStringVector(new String[] { "XM", "OP" }));
                }
                catch (Exception e)
                {
                    exception2 = e;
                }
                Assert.AreEqual("the row label size doesn't equal to the row number of the matrix.", exception2.Message);

            }
            //[TestMethod]
            public void test_AbstractMatrix_getString()
            {
                BasicMatrix bm = new BasicMatrix(2, 2);
                bm.setRowLabels(new BasicStringVector(new String[] { "OP", "VO" }));
                bm.setColumnLabels(new BasicIntVector(new int[] { 10, 20 }));
                Console.WriteLine(bm.getString().ToString());
            }
            [TestMethod]
            public void test_AbstractMatrix_write()
            {
                BasicMatrix bm = new BasicMatrix(2, 2);
                bm.setRowLabels(new BasicStringVector(new String[] { "OP", "VO" }));
                bm.setColumnLabels(new BasicIntVector(new int[] { 10, 20 }));
                Stream outStream = new MemoryStream();
                ExtendedDataOutput out1 = new BigEndianDataOutputStream(outStream);
                bm.write(out1);
            }

            [TestMethod]
            public void test_AbstractScalar()
            {
                BasicScalar bm = new BasicScalar(2);
                Exception exception = null;
                try
                {
                    bm.isNull();
                }
                catch (Exception e)
                {
                    exception = e;
                }
                Assert.AreEqual("The method or operation is not implemented.", exception.Message);
                Exception exception1 = null;
                try
                {
                    bm.setNull();
                }
                catch (Exception e)
                {
                    exception1 = e;
                }
                Assert.AreEqual("The method or operation is not implemented.", exception1.Message);
                Exception exception2 = null;
                try
                {
                    bm.getNumber();
                }
                catch (Exception e)
                {
                    exception2 = e;
                }
                Assert.AreEqual("The method or operation is not implemented.", exception2.Message);
                Exception exception3 = null;
                try
                {
                    bm.getTemporal();
                }
                catch (Exception e)
                {
                    exception3 = e;
                }
                Assert.AreEqual("The method or operation is not implemented.", exception3.Message);
                Exception exception4 = null;
                try
                {
                    bm.getDataCategory();
                }
                catch (Exception e)
                {
                    exception4 = e;
                }
                Assert.AreEqual("The method or operation is not implemented.", exception4.Message);
                Exception exception5 = null;
                try
                {
                    bm.getDataType();
                }
                catch (Exception e)
                {
                    exception5 = e;
                }
                Assert.AreEqual("The method or operation is not implemented.", exception5.Message);
                Exception exception6 = null;
                try
                {
                    bm.toDataTable();
                }
                catch (Exception e)
                {
                    exception6 = e;
                }
                Assert.AreEqual("The scalar can not be convert to datatable", exception6.Message);
                Exception exception7 = null;
                try
                {
                    bm.getScale();
                }
                catch (Exception e)
                {
                    exception7 = e;
                }
                Assert.AreEqual("The method or operation is not implemented.", exception7.Message);
                Exception exception8 = null;
                try
                {
                    bm.toString();
                }
                catch (Exception e)
                {
                    exception8 = e;
                }
                Assert.AreEqual("The method or operation is not implemented.", exception8.Message);
                Assert.AreEqual(1, bm.columns());
            }

        
    }
}
