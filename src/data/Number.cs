﻿using System;

namespace dolphindb.data
{
    public class Number
    {
        private bool _boolVal;
        private int _intVal;
        private double _doubleVal;
        private float _floatVal;
        private long _longVal;
        private short _shortVal;
        private DATA_TYPE _type;

        public Number(int val)
        {
            _intVal = val;
            _type = DATA_TYPE.DT_INT;
        }
        public Number(bool val)
        {
            _boolVal = val;
            _type = DATA_TYPE.DT_BOOL;
        }

        public Number(double val)
        {
            _doubleVal = val;
            _type = DATA_TYPE.DT_DOUBLE;
        }

        public Number(float val)
        {
            _floatVal = val;
            _type = DATA_TYPE.DT_FLOAT;
        }

        public Number(long val)
        {
            _longVal = val;
            _type = DATA_TYPE.DT_LONG;
        }
        public Number(short val)
        {
            _shortVal = val;
            _type = DATA_TYPE.DT_SHORT;
        }

        public byte byteValue()
        {
            switch (_type)
            {
                case DATA_TYPE.DT_BOOL:
                    return Convert.ToByte(_boolVal);
                case DATA_TYPE.DT_INT:
                    return Convert.ToByte(_intVal);
                case DATA_TYPE.DT_DOUBLE:
                    return Convert.ToByte(_doubleVal);
                case DATA_TYPE.DT_FLOAT:
                    return Convert.ToByte(_floatVal);
                case DATA_TYPE.DT_LONG:
                    return Convert.ToByte(_longVal);
                case DATA_TYPE.DT_SHORT:
                    return Convert.ToByte(_shortVal);
                default:
                    throw new InvalidCastException();
            }
        }

        public double doubleValue()
        {
            return _doubleVal;
        }

        public float floatValue()
        {
            return _floatVal;
        }

        public int intValue()
        {
            return _intVal;
        }

        public long longValue()
        {
            return _longVal;
        }

        public short shortValue()
        {
            return _shortVal;
        }





    }
}
