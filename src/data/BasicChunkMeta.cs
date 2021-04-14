using System.Collections.Generic;
using System.Text;

namespace dolphindb.data
{
	public class BasicChunkMeta : AbstractEntity, Entity
	{
		private string path;
		private sbyte[] id;
		private int version;

		private int size_Renamed;
		private sbyte flag;
		private IList<string> sites;

		public BasicChunkMeta(ExtendedDataInput @in)
		{
			@in.readShort(); //skip the length of the data
			path = @in.readString();
			id = new sbyte[16];
			@in.readFully(id);
			version = @in.readInt();
			size_Renamed = @in.readInt();
			flag = @in.readByte();
			sites = new List<>();
			int copyCount = @in.readByte();
			for (int i = 0; i < copyCount; ++i)
			{
				sites.Add(@in.readString());
			}
		}

		public virtual string Path
		{
			get
			{
				return path;
			}
		}

		public virtual string Id
		{
			get
			{
				return getUUIDString(id);
			}
		}

		public virtual int Version
		{
			get
			{
				return version;
			}
		}

		public virtual int size()
		{
			return size_Renamed;
		}

		public virtual int CopyCount
		{
			get
			{
				return sites.Count;
			}
		}

		public virtual bool FileBlock
		{
			get
			{
				return (flag & 3) == 1;
			}
		}

		public virtual bool Tablet
		{
			get
			{
				return (flag & 3) == 0;
			}
		}

		public virtual bool Splittable
		{
			get
			{
				return (flag & 4) == 1;
			}
		}

		public override DATA_FORM DataForm
		{
			get
			{
				return DATA_FORM.DF_CHUNK;
			}
		}

		public virtual DATA_CATEGORY DataCategory
		{
			get
			{
				return null;
			}
		}

		public virtual DATA_TYPE DataType
		{
			get
			{
				return null;
			}
		}

		public virtual int rows()
		{
			return 1;
		}

		public virtual int columns()
		{
			return 0;
		}

		public virtual string String
		{
			get
			{
				StringBuilder str = new StringBuilder(Tablet ? "Tablet[" : "FileBlock[");
				str.Append(path);
				str.Append(", ");
				str.Append(getUUIDString(id));
				str.Append(", {");
				for (int i = 0; i < sites.Count; ++i)
				{
					if (i > 0)
					{
						str.Append(", ");
					}
					str.Append(sites[i]);
				}
				str.Append("}, v");
				str.Append(version);
				str.Append(", ");
				str.Append(size_Renamed);
				if (Splittable)
				{
					str.Append(", splittable]");
				}
				else
				{
					str.Append("]");
				}
				return str.ToString();
			}
		}


		public virtual void write(ExtendedDataOutput output)
		{
			int length = 27 + AbstractExtendedDataOutputStream.getUTFlength(path, 0, 0) + sites.Count;
			foreach (string site in sites)
			{
				length += AbstractExtendedDataOutputStream.getUTFlength(site, 0, 0);
			}
			output.writeShort(length);
			output.writeString(path);
			output.write(id);
			output.writeInt(version);
			output.writeInt(size_Renamed);
			output.writeByte(flag);
			output.writeByte(sites.Count);
			foreach (string site in sites)
			{
				output.writeUTF(site);
			}
		}

		private string getUUIDString(sbyte[] uuid)
		{
			char[] buf = new char[37];

			buf[8] = '-';
			buf[13] = '-';
			buf[18] = '-';
			buf[23] = '-';
			buf[36] = (char)0;
			for (int i = 0; i < 4; ++i)
			{
				charToHexPair(uuid[i], buf, 2 * i);
			}
			charToHexPair(uuid[4], buf, 9);
			charToHexPair(uuid[5], buf, 11);
			charToHexPair(uuid[6], buf, 14);
			charToHexPair(uuid[7], buf, 16);
			charToHexPair(uuid[8], buf, 19);
			charToHexPair(uuid[9], buf, 21);
			for (int i = 10; i < 16; ++i)
			{
				charToHexPair(uuid[i], buf, 4 + 2 * i);
			}
			return new string(buf);
		}

		private void charToHexPair(sbyte v, char[] buf, int offset)
		{
			int low = v & 15;
			int high = (v & 255) >> 4;
			buf[0 + offset] = (char)(high < 10 ? high + 48 : high + 87);
			buf[1 + offset] = (char)(low < 10 ? low + 48 : low + 87);
		}
	}

}