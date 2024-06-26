﻿using System;
using System.Data;
using System.Collections.Generic;
namespace dolphindb.data
{

    public interface IVector : IEntity
    {
        bool isNull(int index);
        void setNull(int index);
        IScalar get(int index);
        IEntity getEntity(int index);
        void set(int index, IScalar value);
        Type getElementClass();
        object getList();
        void set(int index, string value);
        void add(object value);
        void addRange(object list);
        int hashBucket(int index, int buckets);
        IVector getSubVector(int[] indices);
        int asof(IScalar value);
        void append(IScalar value);
        void append(IVector value);
    }

    public static class Vector_Fields
    {
        public const int DISPLAY_ROWS = 10;
        public const int COMPRESS_LZ4 = 1;
        public const int COMPRESS_DELTA = 2;
    }

}