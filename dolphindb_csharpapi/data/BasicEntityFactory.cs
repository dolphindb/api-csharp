using System;
using System.IO;
using dolphindb.io;

namespace dolphindb.data
{
    
    public class BasicEntityFactory : IEntityFactory
    {
        private TypeFactory[] factories;

        public BasicEntityFactory()
        {
            factories = new TypeFactory[Enum.GetValues(typeof(DATA_TYPE)).Length];
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
            factories[(int)DATA_TYPE.DT_TIMESTAMP] = new TimestampFactory();
            factories[(int)DATA_TYPE.DT_NANOTIMESTAMP] = new NanoTimestampFactory();
            factories[(int)DATA_TYPE.DT_SYMBOL] = new SymbolFactory();
            factories[(int)DATA_TYPE.DT_STRING] = new StringFactory();
            //factories[(int)DATA_TYPE.DT_FUNCTIONDEF] = new FunctionDefFactory();
            //factories[(int)DATA_TYPE.DT_HANDLE] = new SystemHandleFactory();
            //factories[(int)DATA_TYPE.DT_CODE] = new MetaCodeFactory();
            //factories[(int)DATA_TYPE.DT_DATASOURCE] = new DataSourceFactory();
            //factories[(int)DATA_TYPE.DT_RESOURCE] = new ResourceFactory();
        }

        public IEntity createEntity(DATA_FORM form, DATA_TYPE type, ExtendedDataInput @in)
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
                //return new BasicDictionary(type, @in);
                return null;
            }
            else if (form == DATA_FORM.DF_SET)
            {
                //return new BasicSet(type, @in);
                return null;
            }
            else if (form == DATA_FORM.DF_CHUNK)
            {
                //return new BasicChunkMeta(@in);
                return null;
            }
            else if (type == DATA_TYPE.DT_ANY && form == DATA_FORM.DF_VECTOR)
            {
                //return new BasicAnyVector(@in);
                return null;
            }
            else if (type == DATA_TYPE.DT_VOID && form == DATA_FORM.DF_SCALAR)
            {
                @in.readBoolean();
                return new Void();
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
                    return factories[index].createVector(@in);
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
        private class BooleanFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicBoolean(@in); }
            public IVector createVector(ExtendedDataInput @in){ return new BasicBooleanVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in){ return new BasicBooleanVector(DATA_FORM.DF_PAIR, @in);}
            public IMatrix createMatrix(ExtendedDataInput @in){ return new BasicBooleanMatrix(@in);}
            public IScalar createScalarWithDefaultValue() { return new BasicBoolean(false); }
            public IVector createVectorWithDefaultValue(int size) { return new BasicBooleanVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicBooleanVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicBooleanMatrix(rows, columns); }
        }

        private class ByteFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicByte(@in); }
            public IVector createVector(ExtendedDataInput @in){ return new BasicByteVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in){ return new BasicByteVector(DATA_FORM.DF_PAIR, @in);}
            public IMatrix createMatrix(ExtendedDataInput @in){ return new BasicByteMatrix(@in);}
            public IScalar createScalarWithDefaultValue() { return new BasicByte((byte)0); }

            public IVector createVectorWithDefaultValue(int size) { return new BasicByteVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicByteVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicByteMatrix(rows, columns); }
        }

        private class ShortFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicShort(@in); }
            public IVector createVector(ExtendedDataInput @in){ return new BasicShortVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in){ return new BasicShortVector(DATA_FORM.DF_PAIR, @in);}
            public IMatrix createMatrix(ExtendedDataInput @in){ return new BasicShortMatrix(@in);}
            public IScalar createScalarWithDefaultValue() { return new BasicShort((short)0); }

            
            public IVector createVectorWithDefaultValue(int size) { return new BasicShortVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicShortVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicShortMatrix(rows, columns); }
        }

        private class IntFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicInt(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicInt(0);  }
            public IVector createVector(ExtendedDataInput @in) { return new BasicIntVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in) { return new BasicIntVector(DATA_FORM.DF_PAIR, @in); }
            public IVector createPairWithDefaultValue(){ return new BasicIntVector(DATA_FORM.DF_PAIR,2); }
            public IVector createVectorWithDefaultValue(int size) { return new BasicIntVector(size); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicIntMatrix(@in); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicIntMatrix(rows, columns); }
        }

        private class LongFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicLong(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicLongVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in) { return new BasicLongVector(DATA_FORM.DF_PAIR, @in);}
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicLongMatrix(@in);}
            public IScalar createScalarWithDefaultValue() { return new BasicLong(0); }
            
            public IVector createVectorWithDefaultValue(int size) { return new BasicLongVector(size); }
            public IVector createPairWithDefaultValue(){ return new BasicLongVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicLongMatrix(rows, columns); }
        }

        private class FloatFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicFloat(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicFloatVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in) { return new BasicFloatVector(DATA_FORM.DF_PAIR, @in);}
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicFloatMatrix(@in);}
            public IScalar createScalarWithDefaultValue() { return new BasicFloat(0); }
            public IVector createVectorWithDefaultValue(int size) { return new BasicFloatVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicFloatVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicFloatMatrix(rows, columns); }
        }

        private class DoubleFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicDouble(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicDoubleVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in) { return new BasicDoubleVector(DATA_FORM.DF_PAIR, @in);}
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicDoubleMatrix(@in);}
            public IScalar createScalarWithDefaultValue() { return new BasicDouble(0); }
                        
            public IVector createVectorWithDefaultValue(int size) { return new BasicDoubleVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicDoubleVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicDoubleMatrix(rows, columns); }
        }

        private class StringFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicString(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicStringVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in) { return new BasicStringVector(DATA_FORM.DF_PAIR, @in);}
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicStringMatrix(@in);}
            public IScalar createScalarWithDefaultValue() { return new BasicString(""); }
           
            public IVector createVectorWithDefaultValue(int size) { return new BasicStringVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicStringVector(DATA_FORM.DF_PAIR, 2, false); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicStringMatrix(rows, columns); }
        }

        private class SymbolFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicString(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicStringVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in) { return new BasicStringVector(DATA_FORM.DF_PAIR, @in);}
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicStringMatrix(@in);}
            public IScalar createScalarWithDefaultValue() { return new BasicString(""); }

           
            public IVector createVectorWithDefaultValue(int size) { return new BasicStringVector(DATA_FORM.DF_VECTOR, size, true); }
            public IVector createPairWithDefaultValue() { return new BasicStringVector(DATA_FORM.DF_PAIR, 2, true); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicStringMatrix(rows, columns); }
        }

        private class DateFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicDate(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicDateVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in) { return new BasicDateVector(DATA_FORM.DF_PAIR, @in);}
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicDateMatrix(@in);}
            public IScalar createScalarWithDefaultValue() { return new BasicDate(0); }
            public IVector createVectorWithDefaultValue(int size) { return new BasicDateVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicDateVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicDateMatrix(rows, columns); }
        }
        private class DateTimeFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicDateTime(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicDateTimeVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in) { return new BasicDateTimeVector(DATA_FORM.DF_PAIR, @in);}
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicDateTimeMatrix(@in);}
            public IScalar createScalarWithDefaultValue() { return new BasicDateTime(0); }
            
            public IVector createVectorWithDefaultValue(int size) { return new BasicDateTimeVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicDateTimeVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicDateTimeMatrix(rows, columns); }
        }

        private class TimestampFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicTimestamp(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicTimestampVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in) { return new BasicTimestampVector(DATA_FORM.DF_PAIR, @in);}
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicTimestampMatrix(@in);}
            public IScalar createScalarWithDefaultValue() { return new BasicTimestamp(0); }

            public IVector createVectorWithDefaultValue(int size) { return new BasicTimestampVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicTimestampVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicTimestampMatrix(rows, columns); }
        }

        private class MonthFactory : TypeFactory
        {
            public IScalar createScalar(ExtendedDataInput @in) { return new BasicMonth(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicMonthVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in) { return new BasicMonthVector(DATA_FORM.DF_PAIR, @in);}
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicMonthMatrix(@in);}
            public IScalar createScalarWithDefaultValue() { return new BasicMonth(0); }
            public IVector createVectorWithDefaultValue(int size) { return new BasicMonthVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicMonthVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicMonthMatrix(rows, columns); }
        }
        private class NanoTimestampFactory : TypeFactory
        {
            public IMatrix createMatrix(ExtendedDataInput @in)
            {
                throw new NotImplementedException();
            }

            public IMatrix createMatrixWithDefaultValue(int rows, int columns)
            {
                throw new NotImplementedException();
            }

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicNanoTimestamp(@in); }
            //public IVector createVector(ExtendedDataInput @in) { return new BasicNanoTimestampVector(DATA_FORM.DF_VECTOR, @in);}
            //public IVector createPair(ExtendedDataInput @in) { return new BasicNanoTimestampVector(DATA_FORM.DF_PAIR, @in);}
            //public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicNanoTimestampMatrix(@in);}
            public IScalar createScalarWithDefaultValue() { return new BasicNanoTimestamp(0); }

            IVector TypeFactory.createPair(ExtendedDataInput @in)
            {
                throw new NotImplementedException();
            }

            IVector TypeFactory.createPairWithDefaultValue()
            {
                throw new NotImplementedException();
            }

            IScalar TypeFactory.createScalar(ExtendedDataInput @in)
            {
                throw new NotImplementedException();
            }

            IScalar TypeFactory.createScalarWithDefaultValue()
            {
                throw new NotImplementedException();
            }

            IVector TypeFactory.createVector(ExtendedDataInput @in)
            {
                throw new NotImplementedException();
            }

            IVector TypeFactory.createVectorWithDefaultValue(int size)
            {
                throw new NotImplementedException();
            }
            //public IVector createVectorWithDefaultValue(int size) { return new BasicNanoTimestampVector(size); }
            //public IVector createPairWithDefaultValue() { return new BasicNanoTimestampVector(DATA_FORM.DF_PAIR, 2); }
            //public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicNanoTimestampMatrix(rows, columns); }
        }
        private class MinuteFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicMinute(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicMinuteVector(DATA_FORM.DF_VECTOR, @in);}
            public IVector createPair(ExtendedDataInput @in) { return new BasicMinuteVector(DATA_FORM.DF_PAIR, @in);}
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicMinuteMatrix(@in);}
            public IScalar createScalarWithDefaultValue() { return new BasicMinute(0); }
            public IVector createVectorWithDefaultValue(int size) { return new BasicMinuteVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicMinuteVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicMinuteMatrix(rows, columns); }
        }

        private class SecondFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicSecond(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicSecondVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicSecondVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicSecondMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicInt(0); }

            public IVector createVectorWithDefaultValue(int size) { return new BasicSecondVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicSecondVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicSecondMatrix(rows, columns); }
        }

        private class TimeFactory : TypeFactory
        {

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicTime(@in); }
            public IVector createVector(ExtendedDataInput @in) { return new BasicTimeVector(DATA_FORM.DF_VECTOR, @in); }
            public IVector createPair(ExtendedDataInput @in) { return new BasicTimeVector(DATA_FORM.DF_PAIR, @in); }
            public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicTimeMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicTime(0); }

            
            public IVector createVectorWithDefaultValue(int size) { return new BasicTimeVector(size); }
            public IVector createPairWithDefaultValue() { return new BasicTimeVector(DATA_FORM.DF_PAIR, 2); }
            public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicTimeMatrix(rows, columns); }
        }
        private class NanoTimeFactory : TypeFactory
        {
            public IMatrix createMatrix(ExtendedDataInput @in)
            {
                throw new NotImplementedException();
            }

            public IMatrix createMatrixWithDefaultValue(int rows, int columns)
            {
                throw new NotImplementedException();
            }

            public IScalar createScalar(ExtendedDataInput @in) { return new BasicNanoTime(@in); }
            //public IVector createVector(ExtendedDataInput @in) { return new BasicNanoTimeVector(DATA_FORM.DF_VECTOR, @in); }
            //public IVector createPair(ExtendedDataInput @in) { return new BasicNanoTimeVector(DATA_FORM.DF_PAIR, @in); }
            //public IMatrix createMatrix(ExtendedDataInput @in) { return new BasicNanoTimeMatrix(@in); }
            public IScalar createScalarWithDefaultValue() { return new BasicNanoTime(0); }

            IVector TypeFactory.createPair(ExtendedDataInput @in)
            {
                throw new NotImplementedException();
            }

            IVector TypeFactory.createPairWithDefaultValue()
            {
                throw new NotImplementedException();
            }

            IScalar TypeFactory.createScalar(ExtendedDataInput @in)
            {
                throw new NotImplementedException();
            }

            IScalar TypeFactory.createScalarWithDefaultValue()
            {
                throw new NotImplementedException();
            }

            IVector TypeFactory.createVector(ExtendedDataInput @in)
            {
                throw new NotImplementedException();
            }

            IVector TypeFactory.createVectorWithDefaultValue(int size)
            {
                throw new NotImplementedException();
            }
            //public IVector createVectorWithDefaultValue(int size) { return new BasicNanoTimeVector(size); }
            //public IVector createPairWithDefaultValue() { return new BasicNanoTimeVector(DATA_FORM.DF_PAIR, 2); }
            //public IMatrix createMatrixWithDefaultValue(int rows, int columns) { return new BasicNanoTimeMatrix(rows, columns); }
        }
    }





    public interface TypeFactory
    {
        IScalar createScalar(ExtendedDataInput @in);
        IScalar createScalarWithDefaultValue();
        IVector createVector(ExtendedDataInput @in);
        IVector createPair(ExtendedDataInput @in);
        IMatrix createMatrix(ExtendedDataInput @in);
        IVector createVectorWithDefaultValue(int size);
        IVector createPairWithDefaultValue();
        IMatrix createMatrixWithDefaultValue(int rows, int columns);
    }
}