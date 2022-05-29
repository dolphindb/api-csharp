using System;
using System.Collections.Generic;

namespace dolphindb.data
{
    using ExtendedDataInput = io.ExtendedDataInput;
    using ExtendedDataOutput = io.ExtendedDataOutput;

    public class SymbolBase
    {
        private List<string> syms = new List<string>();
        private IDictionary<string, int> symMap = null;
        private int id;


        public SymbolBase(int id)
        {
            this.id = id;
        }

        public SymbolBase(ExtendedDataInput @in) : this(@in.readInt(), @in)
        {
        }

        public SymbolBase(int id, ExtendedDataInput @in)
        {
            this.id = id;
            int size = @in.readInt();
            for (int i = 0; i < size; ++i)
            {
                syms.Add(@in.readString());
            }
        }

        public virtual int Id
        {
            get
            {
                return id;
            }
        }

        public virtual int size()
        {
            return syms.Count;
        }

        public virtual string getSymbol(int index)
        {
            return syms[index];
        }

        public virtual int find(string key)
        {
            if (symMap == null)
            {
                symMap = new Dictionary<string, int>();
                if (syms.Count > 0 && syms[0].Length > 0)
                {
                    throw new Exception("A symbol base's first key must be empty string.");
                }
                if (syms.Count == 0)
                {
                    symMap[""] = 0;
                    syms.Add("");
                }
                else
                {
                    int count = syms.Count;
                    for (int i = 0; i < count; ++i)
                    {
                        symMap[syms[i]] = i;
                    }
                }
            }
            return symMap.GetOrDefault(key, -1);
        }

        public virtual int find(string key, bool insertIfNotPresent)
        {
            if (string.ReferenceEquals(key, null))
            {
                throw new Exception("A symbol base key string can't be null.");
            }
            if (symMap == null)
            {
                symMap = new Dictionary<string, int>();
                if (syms.Count > 0 && syms[0].Length > 0)
                {
                    throw new Exception("A symbol base's first key must be empty string.");
                }
                if (syms.Count == 0)
                {
                    symMap[""] = 0;
                    syms.Add("");
                }
                else
                {
                    int count = syms.Count;
                    for (int i = 0; i < count; ++i)
                    {
                        symMap[syms[i]] = i;
                    }
                }
            }
            int index;
            if (symMap.ContainsKey(key))
            {
                index = symMap[key];
            }
            else
            {
                index = symMap.Count;
                symMap[key] = index;
                syms.Add(key);
            }
            return index;
        }

        public virtual void write(ExtendedDataOutput @out)
        {
            int count = syms.Count;
            @out.writeInt(0);
            @out.writeInt(count);
            for (int i = 0; i < count; ++i)
            {
                @out.writeString(syms[i]);
            }
        }
    }
}

internal static class HashMapHelper
{
    public static HashSet<KeyValuePair<TKey, TValue>> SetOfKeyValuePairs<TKey, TValue>(this IDictionary<TKey, TValue> dictionary)
    {
        HashSet<KeyValuePair<TKey, TValue>> entries = new HashSet<KeyValuePair<TKey, TValue>>();
        foreach (KeyValuePair<TKey, TValue> keyValuePair in dictionary)
        {
            entries.Add(keyValuePair);
        }
        return entries;
    }

    public static TValue GetValueOrNull<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key)
    {
        TValue ret;
        dictionary.TryGetValue(key, out ret);
        return ret;
    }

    public static TValue GetOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, TValue defaultValue)
    {
        TValue ret;
        if (dictionary.TryGetValue(key, out ret))
            return ret;
        else
            return defaultValue;
    }

    public static void PutAll<TKey, TValue>(this IDictionary<TKey, TValue> d1, IDictionary<TKey, TValue> d2)
    {
        if (d2 is null)
            throw new NullReferenceException();

        foreach (TKey key in d2.Keys)
        {
            d1[key] = d2[key];
        }
    }

}