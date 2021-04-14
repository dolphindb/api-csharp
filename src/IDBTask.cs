//-------------------------------------------------------------------------------------------
//	Copyright © 2021 DolphinDB Inc.
//	Date   : 2021.01.21
//  Author : zhikun.luo
//-------------------------------------------------------------------------------------------

using dolphindb.data;
namespace dolphindb
{
    public interface IDBTask
    {
        void setDBConnection(DBConnection conn);
        IEntity call();
        IEntity getResults();
        string getErrorMsg();
        bool isSuccessful();
    }
}
