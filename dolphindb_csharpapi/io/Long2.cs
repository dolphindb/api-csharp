namespace dolphindb.io
{

    public class Long2
    {
        public long high;
        public long low;

        public Long2(long high, long low)
        {
            this.high = high;
            this.low = low;
        }

        public bool isNull()
        {
            return high == 0 && low == 0;
        }

        public void setNull()
        {
            high = 0;
            low = 0;
        }

        public bool equals(object o)
        {
            if (!(o is Long2) || o == null)
			return false;
		else
			return high == ((Long2)o).high && low == ((Long2)o).low;
        }

        public int hashCode()
        {
            return (int)(high ^ low >> 32);
        }
    }
}

