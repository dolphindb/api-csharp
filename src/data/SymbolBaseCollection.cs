using System.Collections.Generic;
using System.IO;


namespace dolphindb.data
{
    using ExtendedDataInput = io.ExtendedDataInput;
    using ExtendedDataOutput = io.ExtendedDataOutput;

    public class SymbolBaseCollection
    {
        private IDictionary<int, SymbolBase> symbaseMap = new Dictionary<int, SymbolBase>();
        private IDictionary<SymbolBase, int> existingBases;
        private SymbolBase lastSymbase = null;

        public virtual SymbolBase add(ExtendedDataInput @in)
        {
            int id = @in.readInt();
            if (symbaseMap.ContainsKey(id))
            {
                int size = @in.readInt();
                if (size != 0)
                {
                    throw new IOException("Invalid symbol base.");
                }
                lastSymbase = symbaseMap[id];
            }
            else
            {
                SymbolBase cur = new SymbolBase(id, @in);
                symbaseMap[id] = cur;
                lastSymbase = cur;
            }
            return lastSymbase;
        }

        public virtual void write(ExtendedDataOutput @out, SymbolBase @base)
        {
            bool existing = false;
            int id = 0;
            if (existingBases == null)
            {
                existingBases = new Dictionary<SymbolBase, int>();
                existingBases[@base] = 0;
            }
            else
            {
                int? curId = existingBases[@base];
                if (curId != null)
                {
                    existing = true;
                    id = curId.Value;
                }
                else
                {
                    id = existingBases.Count;
                    existingBases[@base] = id;
                }
            }
            @out.writeInt(id);
            if (existing)
            {
                @out.writeInt(0);
            }
            else
            {
                int size = @base.size();
                @out.writeInt(size);
                for (int i = 0; i < size; ++i)
                {
                    @out.writeString(@base.getSymbol(i));
                }
            }
        }

        public virtual SymbolBase LastSymbolBase
        {
            get
            {
                return lastSymbase;
            }
        }

        public virtual void clear()
        {
            symbaseMap.Clear();
            lastSymbase = null;
        }
    }

}