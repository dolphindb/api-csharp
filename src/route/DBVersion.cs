using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.route
{
    internal class DBVersion
    {
        int[] v = null;
        string version = null;
        public DBVersion() { }

        public DBVersion(string version)
        {
            v = new int[4];
            this.version = version;
            string[] parts = version.Split(' ')[0].Split('.');
            v[0] = int.Parse(parts[0]);
            v[1] = int.Parse(parts[1]);
            v[2] = int.Parse(parts[2]);
            if (parts.Length > 3)
            {
                v[3] = int.Parse(parts[3]);
            }
        }

        public int getVerNum()
        {
            try
            {
                string[] s = version.Split(' ');
                if (s.Length >= 2)
                {
                    string vernum = s[0].Replace(".", "");
                    return int.Parse(vernum);
                }
            }
            catch (Exception)
            {
            }
            return 0;
        }

        public string getVersion() { return version; }

        public int getSubV(int index)
        {
            return v[index];
        }
    }
}
