using System;
using System.Collections.Generic;

namespace com.xxdb.streaming.client
{


	using com.xxdb.data;
	using BigEndianDataInputStream = com.xxdb.io.BigEndianDataInputStream;
	using ExtendedDataInput = com.xxdb.io.ExtendedDataInput;
	using LittleEndianDataInputStream = com.xxdb.io.LittleEndianDataInputStream;

	internal class MessageParser : Runnable
	{
		private readonly int MAX_FORM_VALUE = Enum.GetValues(typeof(Entity_DATA_FORM)).length - 1;
		private readonly int MAX_TYPE_VALUE = Enum.GetValues(typeof(Entity_DATA_TYPE)).length - 1;

		internal BufferedInputStream bis = null;
		internal Socket socket = null;
		internal MessageDispatcher dispatcher;
		public MessageParser(Socket socket, MessageDispatcher dispatcher)
		{
			this.socket = socket;
			this.dispatcher = dispatcher;
		}
		private static readonly char[] hexArray = "0123456789ABCDEF".ToCharArray();
		public static string bytesToHex(sbyte[] bytes)
		{
			char[] hexChars = new char[bytes.Length * 2];
			for (int j = 0; j < bytes.Length; j++)
			{
				int v = bytes[j] & 0xFF;
				hexChars[j * 2] = hexArray[(int)((uint)v >> 4)];
				hexChars[j * 2 + 1] = hexArray[v & 0x0F];
			}
			return new string(hexChars);
		}
		public virtual void run()
		{
			Socket socket = this.socket;
			try
			{
			if (bis == null)
			{
				bis = new BufferedInputStream(socket.InputStream);
			}
			long offset = -1;

			ExtendedDataInput @in = null; //isRemoteLittleEndian ? new LittleEndianDataInputStream(bis) : new BigEndianDataInputStream(bis);

			while (true)
			{

				if (@in == null)
				{
					bool? b = bis.read() != 0; //true/false : little/big
					if (b == true)
					{
						@in = new LittleEndianDataInputStream(bis);
					}
					else
					{
						@in = new BigEndianDataInputStream(bis);
					}
				}
				else
				{
					@in.readBoolean();
				}
				long msgid = @in.readLong();
				if (offset == -1)
				{
					offset = msgid;
				}
				else
				{
					assert(offset == msgid);
				}
				string topic = @in.readString();

				short flag = @in.readShort();
				EntityFactory factory = new BasicEntityFactory();
				int form = flag >> 8;

				int type = flag & 0xff;

				if (form < 0 || form > MAX_FORM_VALUE)
				{
					throw new IOException("Invalid form value: " + form);
				}
				if (type < 0 || type > MAX_TYPE_VALUE)
				{
					throw new IOException("Invalid type value: " + type);

				}
				Entity_DATA_FORM df = Enum.GetValues(typeof(Entity_DATA_FORM))[form];
				Entity_DATA_TYPE dt = Enum.GetValues(typeof(Entity_DATA_TYPE))[type];
				Entity body;
				try
				{
					body = factory.createEntity(df, dt, @in);
				}
				catch (Exception exception)
				{
					throw exception;
				}
				if (body.Vector)
				{
					BasicAnyVector dTable = (BasicAnyVector)body;

					int colSize = dTable.rows();
					int rowSize = dTable.getEntity(0).rows();

					if (rowSize >= 1)
					{
						if (rowSize == 1)
						{
							BasicMessage rec = new BasicMessage(msgid,topic,dTable);
							dispatcher.dispatch(rec);
						}
						else
						{
							IList<IMessage> messages = new List<IMessage>(rowSize);
							for (int i = 0;i < rowSize;i++)
							{
								BasicAnyVector row = new BasicAnyVector(colSize);

								for (int j = 0;j < colSize;j++)
								{
										AbstractVector vector = (AbstractVector)dTable.getEntity(j);
										Entity entity = vector.get(i);
										row.setEntity(j, entity);
								}
								BasicMessage rec = new BasicMessage(msgid,topic,row);
								messages.Add(rec);
								msgid++;
							}
							dispatcher.batchDispatch(messages);
						}
					}
					offset += rowSize;
				}
				else
				{
					throw new Exception("message body has an invalid format.vector is expected");
				}
			}
			}
		catch (Exception e)
		{
			Console.WriteLine(e.ToString());
			Console.Write(e.StackTrace);
		}
		finally
		{
			try
			{
				socket.close();
			}
			catch (IOException e)
			{
				Console.WriteLine(e.ToString());
				Console.Write(e.StackTrace);
			}
		}
		}

	}
}