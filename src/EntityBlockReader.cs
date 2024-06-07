                                                                                                                                                                                                                                        //-------------------------------------------------------------------------------------------
//	Copyright © 2021 DolphinDB Inc.
//	Date   : 2021.01.15
//  Author : zhikun.luo
//-------------------------------------------------------------------------------------------


using System;
using dolphindb.io;
using dolphindb.data;
using System.Data;

namespace dolphindb
{
    public class EntityBlockReader:IEntity
    {
        private IEntity currentValue;
        private int size;
        private int currentIndex = 0;
        private ExtendedDataInput instream;

        public EntityBlockReader(ExtendedDataInput input) 
        {
            int rows = input.readInt();
            int cols = input.readInt();
            size = rows * cols;
            currentIndex = 0;
            instream = input;
        }

        public object getObject() 
        {
            BasicEntityFactory factory = new BasicEntityFactory();
            if (currentIndex>=size) return null;

            short flag = instream.readShort();
            int form = flag >> 8;
            int type = flag & 0xff;
            bool extended = type >= 128;
            if (type >= 128)
                type -= 128;
            currentValue = factory.createEntity((DATA_FORM)form, (DATA_TYPE)type, instream, extended);
            currentIndex++;
            return currentValue;
        }

        public void skipAll()
        {
             BasicEntityFactory factory = new BasicEntityFactory();
             for (int i = currentIndex; i < size; i++)
             {
                 short flag = instream.readShort();
                 int form = flag >> 8;
                 int type = flag & 0xff;
                bool extended = type >= 128;
                if (type >= 128)
                    type -= 128;
                currentValue = factory.createEntity((DATA_FORM)form, (DATA_TYPE)type, instream, extended);
                 currentIndex++;
             }
        }

        public DataTable toDataTable()
        {
            throw new NotImplementedException();
        }
        public Boolean hasNext()
{
    return currentIndex < size;
}


    public DATA_FORM getDataForm()
{
    return DATA_FORM.DF_TABLE;
}


    public DATA_CATEGORY getDataCategory()
{
    return DATA_CATEGORY.MIXED;
}


    public DATA_TYPE getDataType()
{
    return DATA_TYPE.DT_ANY;
}


    public int rows()
{
    return 0;
}


    public int columns()
{
    return 0;
}


    public String getString()
{
    return null;
}


    public void write(ExtendedDataOutput output)
{

}


    public bool isScalar()
{
    return false;
}


    public bool isVector()
{
    return false;
}


    public bool isPair()
{
    return false;
}


    public bool isTable()
{
    return true;
}


    public bool isMatrix()
{
    return false;
}


    public bool isDictionary()
{
    return false;
}


    public bool isChart()
{
    return false;
}


    public bool isChunk()
{
    return false;
}

        public void writeCompressed(ExtendedDataOutput output)
        {
            throw new NotImplementedException();
        }
    }

}
