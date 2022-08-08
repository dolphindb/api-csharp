using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.route
{
    public class ErrorCodeInfo
    {
        public enum Code
        {
            EC_None = 0,
            EC_InvalidObject,
            EC_InvalidParameter,
            EC_InvalidTable,
            EC_InvalidColumnType,
            EC_Server,
            EC_UserBreak,
            EC_DestroyedObject,
            EC_Other

        }
        public ErrorCodeInfo()
        {
            set(0, "");
        }
        public ErrorCodeInfo(Code code, String info)
        {
            set(code, info);
        }
        public ErrorCodeInfo(ErrorCodeInfo src)
        {
            set(src.errorCode, src.errorInfo);
        }
        public void set(ErrorCodeInfo errorCodeInfo)
        {
            set(errorCodeInfo.errorCode, errorCodeInfo.errorInfo);
        }
        public void set(Code code, String info)
        {
            this.errorCode = formatApiCode((int)code);
            this.errorInfo = info;
        }
        public void set(string code, String info)
        {
            this.errorCode = code;
            this.errorInfo = info;
        }
        public override string ToString()
        {
            return "code = " + errorCode + " info = " + errorInfo;
        }
        public static string formatApiCode(int code)
        {
            if (code != (int)Code.EC_None)
                return "A" + code;
            else
                return "";
        }
        public void clearError()
        {
            errorCode = "";
            errorInfo = "";
        }
        public bool hasError()
        {
            return errorCode != "";
        }
        public bool succeed()
        {
            return errorCode == "";
        }
        public string errorCode = "";
        public string errorInfo;
    }
}
