using dolphindb.data;
using dolphindb.io;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;

namespace dolphindb.streaming
{
    public class MessageParser
    {
        private readonly int MAX_FORM_VALUE = Enum.GetNames(typeof(DATA_FORM)).Length;
        private readonly int MAX_TYPE_VALUE = Enum.GetNames(typeof(DATA_TYPE)).Length;

        Socket socket = null;
        MessageDispatcher dispatcher;
        BufferedStream bis = null;

        public MessageParser(Socket socket, MessageDispatcher dispatcher)
        {
            this.socket = socket;
            this.dispatcher = dispatcher;
        }

        private static readonly char[] hexArray = "0123456789ABCDEF".ToCharArray();
        public static string bytesToHex(byte[] bytes)
        {
            char[] hexChars = new char[bytes.Length * 2];
            for (int j = 0; j < bytes.Length; j++)
            {
                uint v = (uint)bytes[j] & 0xFF;
                hexChars[j * 2] = hexArray[v >> 4];
                hexChars[j * 2 + 1] = hexArray[v & 0x0f];
            }
            return new string(hexChars);
        }

        public void run()
        {
            Socket socket = this.socket;
            try
            {
                long offset = -1;
                if (bis == null)
                    bis = new BufferedStream(new NetworkStream(socket));
                ExtendedDataInput @in = null;

                while (true)
                {
                    if (@in == null)
                    {
                        bool isLittle = bis.ReadByte() != 0;
                        if (isLittle)
                            @in = new LittleEndianDataInputStream(bis);
                        else
                            @in = new BigEndianDataInputStream(bis);
                    }
                    else
                    {
                        @in.readBoolean();
                    }

                    @in.readLong();
                    long msgid = @in.readLong();

                    if (offset == -1)
                        offset = msgid;
                    else
                        Debug.Assert(offset == msgid);
                    string topic = @in.readString();
                    short flag = @in.readShort();
                    IEntityFactory factory = new BasicEntityFactory();
                    int form = flag >> 8;
                    int type = flag & 0xff;

                    if (form < 0 || form > MAX_FORM_VALUE)
                        throw new IOException("Invalid form value: " + form);
                    if (type < 0 || type > MAX_TYPE_VALUE)
                        throw new IOException("Invalid type value: " + type);

                    DATA_FORM df = (DATA_FORM)form;
                    DATA_TYPE dt = (DATA_TYPE)type;

                    IEntity body;
                    try
                    {
                        body = factory.createEntity(df, dt, @in);
                    }
                    catch
                    {
                        throw;
                    }
                    if (body.isTable())
                    {
                        if (body.rows() != 0)
                            throw new Exception("When message is table, it should be empty.");
                    }
                    else if (body.isVector())
                    {
                        BasicAnyVector dTable = (BasicAnyVector)body;

                        int colSize = dTable.rows();
                        int rowSize = dTable.getEntity(0).rows();
                        if (rowSize == 1)
                        {
                            BasicMessage rec = new BasicMessage(msgid, topic, dTable);
                            dispatcher.dispatch(rec);
                        }
                        else if (rowSize > 1)
                        {
                            List<IMessage> messages = new List<IMessage>(rowSize);
                            for (int i = 0; i < rowSize; i++)
                            {
                                BasicAnyVector row = new BasicAnyVector(colSize);

                                for (int j = 0; j < colSize; j++)
                                {
                                    AbstractVector vector = (AbstractVector)dTable.getEntity(j);
                                    IEntity entity = vector.get(i);
                                    row.setEntity(j, entity);
                                }
                                BasicMessage rec = new BasicMessage(msgid, topic, row);
                                messages.Add(rec);
                                msgid++;
                            }
                            dispatcher.batchDispatch(messages);
                        }
                        offset += rowSize;
                    }
                    else
                    {
                        throw new Exception("message body has an invalid format. Vector or table is expected");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.Write(ex.StackTrace);
            }
            finally
            {
                try
                {
                    socket.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    Console.Write(ex.StackTrace);
                }
            }
        }
    }
}
