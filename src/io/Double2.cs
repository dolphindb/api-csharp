using System;
using dolphindb.data;

namespace dolphindb.io
{

    public struct Double2
    {
        public double x;
        public double y;

        public Double2(double x = -double.MaxValue, double y = -double.MaxValue)
        {
            this.x = x;
            this.y = y;
        }

        public bool isNull()
        {
            return x == -double.MaxValue && y == -double.MaxValue;
        }

        public void setNull()
        {
            x = -double.MaxValue;
            y = -double.MaxValue;
        }

        public bool equals(object o)
        {
            if (!(o is Double2) || o == null)
                return false;
            else
                return x == ((Double2)o).x && y == ((Double2)o).y;
        }

        public int hashBucket(int buckets)
        {
            return -1;
        }

        public override bool Equals(object o)
        {
            if (!(o is Double2) || o == null)
            {
                return false;
            }
            else
            {
                if (this.x != ((Double2)o).x || this.y != ((Double2)o).y)
                {
                    return false;
                }
                else
                    return true;
            }
        }

        public override int GetHashCode()
        {
            return new BasicDouble(x).GetHashCode() ^ new BasicDouble(y).GetHashCode();
        }
    }
}
