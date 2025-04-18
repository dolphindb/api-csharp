﻿using System;
using System.IO;
using dolphindb.io;

namespace dolphindb.data
{
    public class BasicEntityFactory : IEntityFactory
    {
        int ARRARY_BASE = 64;
        private TypeFactory[] factories;
        private TypeFactory[] factoriesExt;
        private static IEntityFactory factory = new BasicEntityFactory();
        
        public static IEntityFactory instance()
        {
            return factory;
        }

        public BasicEntityFactory()
        {
            factories = new TypeFactory[Enum.GetValues(typeof(DATA_TYPE)).Length];
            factoriesExt = new TypeFactory[Enum.GetValues(typeof(DATA_TYPE)).Length];
            factories[(int)DATA_TYPE.DT_BOOL] = new BooleanFactory();
            factories[(int)DATA_TYPE.DT_BYTE] = new ByteFactory();
            factories[(int)DATA_TYPE.DT_SHORT] = new ShortFactory();
            factories[(int)DATA_TYPE.DT_INT] = new IntFactory();
            factories[(int)DATA_TYPE.DT_LONG] = new LongFactory();
            factories[(int)DATA_TYPE.DT_FLOAT] = new FloatFactory();
            factories[(int)DATA_TYPE.DT_DOUBLE] = new DoubleFactory();
            factories[(int)DATA_TYPE.DT_MINUTE] = new MinuteFactory();
            factories[(int)DATA_TYPE.DT_SECOND] = new SecondFactory();
            factories[(int)DATA_TYPE.DT_TIME] = new TimeFactory();
            factories[(int)DATA_TYPE.DT_NANOTIME] = new NanoTimeFactory();
            factories[(int)DATA_TYPE.DT_DATE] = new DateFactory();
            factories[(int)DATA_TYPE.DT_MONTH] = new MonthFactory();
            factories[(int)DATA_TYPE.DT_DATETIME] = new DateTimeFactory();
            factories[(int)DATA_TYPE.DT_DATEHOUR] = new DateHourFactory();
            factories[(int)DATA_TYPE.DT_TIMESTAMP] = new TimestampFactory();
            factories[(int)DATA_TYPE.DT_NANOTIMESTAMP] = new NanoTimestampFactory();
            factories[(int)DATA_TYPE.DT_SYMBOL] = new SymbolFactory();
            factories[(int)DATA_TYPE.DT_STRING] = new StringFactory();
            factories[(int)DATA_TYPE.DT_FUNCTIONDEF] = new FunctionDefFactory();
            factories[(int)DATA_TYPE.DT_HANDLE] = new SystemHandleFactory();
            factories[(int)DATA_TYPE.DT_CODE] = new MetaCodeFactory();
            factories[(int)DATA_TYPE.DT_DATASOURCE] = new DataSourceFactory();
            factories[(int)DATA_TYPE.DT_RESOURCE] = new ResourceFactory();
            factories[(int)DATA_TYPE.DT_UUID] = new UuidFactory();
            factories[(int)DATA_TYPE.DT_INT128] = new Int128Factory();
            factories[(int)DATA_TYPE.DT_COMPLEX] = new ComplexFactory();
            factories[(int)DATA_TYPE.DT_POINT] = new PointFactory();
            factories[(int)DATA_TYPE.DT_IPADDR] = new IPAddrFactory();

            //2021.01.19 cwj
            factories[(int)DATA_TYPE.DT_BLOB] = new BlobFactory();
            //
            factoriesExt[(int)DATA_TYPE.DT_SYMBOL] = new ExtendedSymbolFactory();

            factories[(int)DATA_TYPE.DT_DECIMAL32] = new Decimal32Factory();
            factories[(int)DATA_TYPE.DT_DECIMAL64] = new Decimal64Factory();
            factories[(int)DATA_TYPE.DT_DECIMAL128] = new Decimal128Factory();
            factories[(int)DATA_TYPE.DT_VOID] = new VoidFactory();
        }

        public IEntity createEntity(DATA_FORM form, DATA_TYPE type, ExtendedDataInput @in, bool extend)
        {
            if (form == DATA_FORM.DF_TABLE)
            {
                return new BasicTable(@in);
            }
            else if (form == DATA_FORM.DF_CHART)
            {
                //return new BasicChart(@in);
                return null;
            }
            else if (form == DATA_FORM.DF_DICTIONARY)
            {
                return new BasicDictionary(type, @in);
            }
            else if (form == DATA_FORM.DF_SET)
            {
                return new BasicSet(type, @in);
            }
            else if (form == DATA_FORM.DF_CHUNK)
            {
                //return new BasicChunkMeta(@in);
                return null;
            }
            else if (type == DATA_TYPE.DT_ANY && form == DATA_FORM.DF_VECTOR)
            {
                return new BasicAnyVector(@in);
            }
            else if(type >= DATA_TYPE.DT_BOOL_ARRAY && type <= DATA_TYPE.DT_DECIMAL128_ARRAY)
            {
                return new BasicArrayVector(type, @in);
            }
            else if(type == DATA_TYPE.DT_IOTANY)
            {
                return new BasicIotAnyVector(form, @in);
            }
            else
            {
                int index = (int)type;
                if (factories[index] == null)
                {
                    throw new IOException("Data type " + type.ToString() + " is not supported yet.");
                }
                else if (form == DATA_FORM.DF_VECTOR)
                {
                    if (!extend)
                        return factories[index].createVector(@in);
                    else
                        return factoriesExt[index].createVector(@in);
                }
                else if (form == DATA_FORM.DF_SCALAR)
                {
                    return factories[index].createScalar(@in);
                }
                else if (form == DATA_FORM.DF_MATRIX)
                {
                    return factories[index].createMatrix(@in);
                }
                else if (form == DATA_FORM.DF_PAIR)
                {
                    return factories[index].createPair(@in);
                }
                else
                {
                    throw new IOException("Data form " + form.ToString() + " is not supported yet.");
                }
            }
        }

        public IMatrix createMatrixWithDefaultValue(DATA_TYPE type, int rows, int columns, int extra = -1)
        {
            int index = (int)type;
            if (factories[index] == null)
                return null;
            else
                return factories[index].createMatrixWithDefaultValue(rows, columns, extra);
        }


        public IVector createVectorWithDefaultValue(DATA_TYPE type, int size, int extra = -1)
        {
            int index = (int)type;
            if ((int)type > ARRARY_BASE)
            {
                if (size != 0)
                {
                    throw new Exception("If there is no value, an empty ArrayVector can only be created. ");
                }
                else
                {
                    return new BasicArrayVector(type, extra);
                }
            }
            else
            {
                if (factories[index] == null)
                    return null;
                else
                    return factories[index].createVectorWithDefaultValue(size, extra);
            }
        }


        public IVector createPairWithDefaultValue(DATA_TYPE type)
        {
            int index = (int)type;
            if (factories[index] == null)
                return null;
            else
                return factories[index].createPairWithDefaultValue();
        }


        public IScalar createScalarWithDefaultValue(DATA_TYPE type)
        {
            int index = (int)type;
            if (factories[index] == null)
            {
                return null;
            }
            else
            {
                return factories[index].createScalarWithDefaultValue();
            }
        }

        //private class Decimal64Factory : TypeFactory
        //{
        //    public IMatrix createMatrix(ExtendedDataInput @in)
        //    {
        //        throw new NotImplementedException();
        //    }

        //    public IMatrix createMatrixWithDefaultValue(int rows, int columns)
        //    {
        //        throw new NotImplementedException();
        //    }

        //    public IVector createPair(ExtendedDataInput @in)
        //    {
        //        return new BasicDecimal64Vector(DATA_FORM.DF_PAIR, @in, -1);
        //    }

        //    public IVector createPairWithDefaultValue()
        //    {
        //        throw new NotImplementedException();
        //    }

        //    public IScalar createScalar(ExtendedDataInput @in)
        //    {
        //        return new BasicDecimal64(@in);
        //    }

        //    public IScalar createScalarWithDefaultValue()
        //    {
        //        return new BasicDecimal64(0, 0);
        //    }

        //    public IVector createVector(ExtendedDataInput @in)
        //    {
        //        return new BasicDecimal64Vector(DATA_FORM.DF_VECTOR, @in, -1);
        //    }

        //    public IVector createVectorWithDefaultValue(int size)
        //    {
        //        return new BasicDecimal64Vector(size);
        //    }
        //}

        //private class Decimal32Factory : TypeFactory
        //{
        //    public IMatrix createMatrix(ExtendedDataInput @in)
        //    {
        //        throw new NotImplementedException();
        //    }

        //    public IMatrix createMatrixWithDefaultValue(int rows, int columns)
        //    {
        //        throw new NotImplementedException();
        //    }

        //    public IVector createPair(ExtendedDataInput @in)
        //    {
        //        return new BasicDecimal32Vector(DATA_FORM.DF_PAIR, @in, -1);
        //    }

        //    public IVector createPairWithDefaultValue()
        //    {
        //        throw new NotImplementedException();
        //    }

        //    public IScalar createScalar(ExtendedDataInput @in)
        //    {
        //        return new BasicDecimal32(@in);
        //    }

        //    public IScalar createScalarWithDefaultValue()
        //    {
        //        return new BasicDecimal32(0, 0);
        //    }

        //    public IVector createVector(ExtendedDataInput @in)
        //    {
        //        return new BasicDecimal32Vector(DATA_FORM.DF_VECTOR, @in, -1);
        //    }

        //    public IVector createVectorWithDefaultValue(int size)
        //    {
        //        return new BasicDecimal32Vector(size);
        //    }
        //}

        private class BooleanFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicBoolean(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicBooleanVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicBooleanVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicBooleanMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicBoolean(false); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicBooleanVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicBooleanVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicBooleanMatrix(rows, columns); }
        }

        private class ByteFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicByte(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicByteVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicByteVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicByteMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicByte((byte)0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicByteVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicByteVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicByteMatrix(rows, columns); }
        }

        private class ShortFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicShort(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicShortVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicShortVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicShortMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicShort((short)0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicShortVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicShortVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicShortMatrix(rows, columns); }
        }

        private class IntFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicInt(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicInt(0); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicIntVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicIntVector(DATA_FORM.DF_PAIR, @in); }
            public IVector createPairWithDefaultValue() { return new BasicIntVector(DATA_FORM.DF_PAIR, 2); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicIntVector(size); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicIntMatrix(@in); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicIntMatrix(rows, columns); }
        }

        private class LongFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicLong(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicLongVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicLongVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicLongMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicLong(0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicLongVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicLongVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicLongMatrix(rows, columns); }
        }

        private class FloatFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicFloat(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicFloatVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicFloatVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicFloatMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicFloat(0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicFloatVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicFloatVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicFloatMatrix(rows, columns); }
        }

        private class DoubleFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicDouble(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicDoubleVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicDoubleVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicDoubleMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicDouble(0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicDoubleVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicDoubleVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicDoubleMatrix(rows, columns); }
        }

        private class UuidFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicUuid(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicUuidVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicUuidVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { throw new Exception("Matrix for UUID not supported yet.");}
            public IScalar createScalarWithDefaultValue() { return new BasicUuid(0, 0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicUuidVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicUuidVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { throw new Exception("Matrix for UUID not supported yet."); }
	}

        private class IPAddrFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicIPAddr(@in); }
            public IVector createVector(ExtendedDataInput @in){ return new BasicIPAddrVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in){ return new BasicIPAddrVector(DATA_FORM.DF_PAIR, @in);}
            public IMatrix createMatrix(ExtendedDataInput @in){ throw new Exception("Matrix for IPADDR not supported yet.");}
		    public IScalar createScalarWithDefaultValue() { return new BasicIPAddr(0, 0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicIPAddrVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicIPAddrVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { throw new Exception("Matrix for IPADDR not supported yet."); }
	    }

        private class Int128Factory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in){ return new BasicInt128(@in);  }
            public IVector createVector(ExtendedDataInput @in){ return new BasicInt128Vector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in){ return new BasicInt128Vector(DATA_FORM.DF_PAIR, @in);}
		    public IMatrix createMatrix(ExtendedDataInput @in){ throw new Exception("Matrix for INT128 not supported yet.");}
		    public IScalar createScalarWithDefaultValue() { return new BasicInt128(0, 0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicInt128Vector(size); }
            public IVector createPairWithDefaultValue() { return new BasicInt128Vector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { throw new Exception("Matrix for INT128 not supported yet."); }
	    }
        
        private class ComplexFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in){ return new BasicComplex(@in);  }
            public IVector createVector(ExtendedDataInput @in){ return new BasicComplexVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in){ return new BasicComplexVector(DATA_FORM.DF_PAIR, @in);}
		    public IMatrix createMatrix(ExtendedDataInput @in){ return new BasicComplexMatrix(@in);}
		    public IScalar createScalarWithDefaultValue() { return new BasicComplex(-double.MaxValue, -double.MaxValue); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicComplexVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicComplexVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicComplexMatrix(rows, columns); }
	    }

        private class PointFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicPoint(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicPointVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicPointVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { throw new Exception("Matrix for POINT not supported yet."); }
            public IScalar createScalarWithDefaultValue() { return new BasicPoint(-double.MaxValue, -double.MaxValue); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicPointVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicPointVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { throw new Exception("Matrix for POINT not supported yet."); }
        }

        private class StringFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicString(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicStringVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicStringVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicStringMatrix(@in, false); }
            public IScalar createScalarWithDefaultValue() { return new BasicString(""); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicStringVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicStringVector(DATA_FORM.DF_PAIR, 2, false); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicStringMatrix(rows, columns); }
        }

        //2021.01.19 cwj
        private class BlobFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicString(@in,true); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicStringVector(DATA_FORM.DF_VECTOR, @in,false,true ); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicStringVector(DATA_FORM.DF_PAIR, @in,false, true); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicStringMatrix(@in, false); }
            public IScalar createScalarWithDefaultValue() { return new BasicString("", true); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicStringVector(DATA_FORM.DF_VECTOR, size, false, true); }
            public IVector createPairWithDefaultValue() { return new BasicStringVector(DATA_FORM.DF_PAIR, 2, false,true); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicStringMatrix(rows, columns); }
        }
        //
        private class SymbolFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicString(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicStringVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicStringVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicStringMatrix(@in, true); }
            public IScalar createScalarWithDefaultValue() { return new BasicString(""); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicStringVector(DATA_FORM.DF_VECTOR, size, true); }
            public IVector createPairWithDefaultValue() { return new BasicStringVector(DATA_FORM.DF_PAIR, 2, true); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicStringMatrix(rows, columns); }
        }

        private class ExtendedSymbolFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicString(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicSymbolVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicSymbolVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicStringMatrix(@in, true); }
            public IScalar createScalarWithDefaultValue() { return new BasicString(""); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicSymbolVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicStringVector(DATA_FORM.DF_PAIR, 2, true); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicStringMatrix(rows, columns); }
        }

        private class DateFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicDate(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicDateVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicDateVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicDateMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicDate(0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicDateVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicDateVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicDateMatrix(rows, columns); }
        }

        private class DateTimeFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicDateTime(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicDateTimeVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicDateTimeVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicDateTimeMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicDateTime(0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicDateTimeVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicDateTimeVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicDateTimeMatrix(rows, columns); }
        }

        private class DateHourFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicDateHour(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicDateHourVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicDateHourVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicDateHourMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicDateHour(0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicDateHourVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicDateHourVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicDateHourMatrix(rows, columns); }
        }

        private class TimestampFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicTimestamp(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicTimestampVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicTimestampVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicTimestampMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicTimestamp(0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicTimestampVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicTimestampVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicTimestampMatrix(rows, columns); }
        }

        private class MonthFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicMonth(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicMonthVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicMonthVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicMonthMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicMonth(0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicMonthVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicMonthVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicMonthMatrix(rows, columns); }
        }
        private class NanoTimestampFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicNanoTimestamp(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicNanoTimestampVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicNanoTimestampVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicNanoTimestampMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicNanoTimestamp(0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicNanoTimestampVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicNanoTimestampVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicNanoTimestampMatrix(rows, columns); }
        }
        private class MinuteFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicMinute(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicMinuteVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicMinuteVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicMinuteMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicMinute(0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicMinuteVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicMinuteVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicMinuteMatrix(rows, columns); }
        }

        private class SecondFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicSecond(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicSecondVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicSecondVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicSecondMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicSecond(0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicSecondVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicSecondVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicSecondMatrix(rows, columns); }
        }

        private class TimeFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicTime(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicTimeVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicTimeVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicTimeMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicTime(0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicTimeVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicTimeVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicTimeMatrix(rows, columns); }
        }
        private class NanoTimeFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicNanoTime(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicNanoTimeVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicNanoTimeVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicNanoTimeMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicNanoTime(0); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicNanoTimeVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicNanoTimeVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicNanoTimeMatrix(rows, columns); }
        }

        private class Decimal32Factory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicDecimal32(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicDecimal32Vector(DATA_FORM.DF_VECTOR, @in, -1); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicDecimal32Vector(DATA_FORM.DF_PAIR, @in, -1); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicDecimal32Matrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicDecimal32("0"); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicDecimal32Vector(size, extra); }
            public IVector createPairWithDefaultValue() { throw new NotImplementedException(); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicDecimal32Matrix(rows, columns, extra); }
        }

        private class Decimal64Factory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicDecimal64(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicDecimal64Vector(DATA_FORM.DF_VECTOR, @in, -1); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicDecimal64Vector(DATA_FORM.DF_PAIR, @in, -1); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicDecimal64Matrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicDecimal64("0"); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicDecimal64Vector(size, extra); }
            public IVector createPairWithDefaultValue() { throw new NotImplementedException(); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicDecimal64Matrix(rows, columns, extra); }
        }

        private class Decimal128Factory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicDecimal128(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicDecimal128Vector(DATA_FORM.DF_VECTOR, @in, -1); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicDecimal128Vector(DATA_FORM.DF_PAIR, @in, -1); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicDecimal128Matrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicDecimal128("0"); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicDecimal128Vector(size, extra); }
            public IVector createPairWithDefaultValue() { throw new NotImplementedException(); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { return new BasicDecimal128Matrix(rows, columns, extra); }
        }

        private class VoidFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new Void(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicVoidVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicVoidVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IScalar createScalarWithDefaultValue() { return new Void(); }
            public IVector createVectorWithDefaultValue(int size, int extra = -1) { return new BasicVoidVector(size); }
            public IVector createPairWithDefaultValue() { throw new NotImplementedException(); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { throw new NotImplementedException(); }
        }

        private class FunctionDefFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicSystemEntity(@in, DATA_TYPE.DT_FUNCTIONDEF); }
            public IScalar createScalarWithDefaultValue() { throw new NotImplementedException(); }
            public IVector createVector(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IVector createPair(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IMatrix createMatrix(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IVector createVectorWithDefaultValue(int size, int extra) { throw new NotImplementedException(); }
            public IVector createPairWithDefaultValue() { throw new NotImplementedException(); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { throw new NotImplementedException(); }
        }

        private class MetaCodeFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicSystemEntity(@in, DATA_TYPE.DT_CODE); }
            public IScalar createScalarWithDefaultValue() { throw new NotImplementedException(); }
            public IVector createVector(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IVector createPair(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IMatrix createMatrix(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IVector createVectorWithDefaultValue(int size, int extra) { throw new NotImplementedException(); }
            public IVector createPairWithDefaultValue() { throw new NotImplementedException(); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { throw new NotImplementedException(); }
        }

        private class DataSourceFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicSystemEntity(@in, DATA_TYPE.DT_DATASOURCE); }
            public IScalar createScalarWithDefaultValue() { throw new NotImplementedException(); }
            public IVector createVector(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IVector createPair(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IMatrix createMatrix(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IVector createVectorWithDefaultValue(int size, int extra) { throw new NotImplementedException(); }
            public IVector createPairWithDefaultValue() { throw new NotImplementedException(); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { throw new NotImplementedException(); }
        }

        private class SystemHandleFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicSystemEntity(@in, DATA_TYPE.DT_HANDLE); }
            public IScalar createScalarWithDefaultValue() { throw new NotImplementedException(); }
            public IVector createVector(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IVector createPair(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IMatrix createMatrix(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IVector createVectorWithDefaultValue(int size, int extra) { throw new NotImplementedException(); }
            public IVector createPairWithDefaultValue() { throw new NotImplementedException(); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { throw new NotImplementedException(); }
        }

        private class ResourceFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicSystemEntity(@in, DATA_TYPE.DT_RESOURCE); }
            public IScalar createScalarWithDefaultValue() { throw new NotImplementedException(); }
            public IVector createVector(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IVector createPair(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IMatrix createMatrix(ExtendedDataInput @in) { throw new NotImplementedException(); }
            public IVector createVectorWithDefaultValue(int size, int extra) { throw new NotImplementedException(); }
            public IVector createPairWithDefaultValue() { throw new NotImplementedException(); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra) { throw new NotImplementedException(); }
        }
    }

    public interface TypeFactory
    {
        IScalar createScalar(ExtendedDataInput @in);
        IScalar createScalarWithDefaultValue();
        IVector createVector(ExtendedDataInput @in);
        IVector createPair(ExtendedDataInput @in);
        IMatrix createMatrix(ExtendedDataInput @in);
        IVector createVectorWithDefaultValue(int size, int extra);
        IVector createPairWithDefaultValue();
        IMatrix createMatrixWithDefaultValue(int rows, int columns, int extra);
    }
}