using dolphindb.io;
using System;
using System.Data;
using System.Text;
using System.Collections.Generic;
using dolphindb.compression;
using System.IO;
using System.Threading;

namespace dolphindb.data
{

    public abstract class AbstractVector : AbstractEntity, IVector
    {
        public abstract int rows();
        public abstract DATA_TYPE getDataType();
        public abstract DATA_CATEGORY getDataCategory();
        public abstract Type getElementClass();
        public abstract void set(int index, IScalar value);
        public abstract IScalar get(int index);
        public abstract void setNull(int index);
        public abstract bool isNull(int index);
        public virtual int getExtraParamForType(){
            return 0;
        }

        private DATA_FORM df_;

        protected static int BUF_SIZE = 4096;

        public static int ARRAY_VECTOR_BASE = 64;

        protected ThreadLocal<byte[]> buf = new ThreadLocal<byte[]>( () => new byte[BUF_SIZE]);

        protected int compressedMethod = Vector_Fields.COMPRESS_LZ4;

        public virtual DataTable toDataTable()
        {
            DataTable dt = buildTable();

            for (int i = 0; i < rows(); i++)
            {
                DataRow dr = dt.NewRow();
                dr[0] = Utils.getRowDataTableObject(this, i);
                dt.Rows.Add(dr);
            }
            return dt;
        }

        protected internal abstract void writeVectorToOutputStream(ExtendedDataOutput @out);

        public AbstractVector(DATA_FORM df)
        {
            df_ = df;
        }

        public override DATA_FORM getDataForm()
        {
            return df_;
        }

        public virtual int columns()
        {
            return 1;
        }

        public virtual string getString()
        {
            StringBuilder sb = new StringBuilder("[");
            int size = Math.Min(Vector_Fields.DISPLAY_ROWS, rows());
            if (size > 0)
            {
                sb.Append(getEntity(0).getString());
            }
            for (int i = 1; i < size; ++i)
            {
                sb.Append(", ");
                sb.Append(getEntity(i).getString());
            }
            if (size < rows())
            {
                sb.Append(",...");
            }
            sb.Append("]");
            return sb.ToString();
        }

        public virtual void write(ExtendedDataOutput @out)
        {
            int flag = ((int)df_ << 8) + (int)getDataType();
            if (this is BasicSymbolVector && rows() != 0)
                flag += 128;
            @out.writeShort(flag);
            if(getDataType() == DATA_TYPE.DT_IOTANY)
            {
                @out.writeInt(((BasicIotAnyVector)this).serializeAnyVectorRows());
            }
            else
            {
                @out.writeInt(rows());
            }
            @out.writeInt(columns());
            writeVectorToOutputStream(@out);
        }

        protected DataTable buildTable()
        {
            DataTable dt = new DataTable();
            if(df_ == DATA_FORM.DF_PAIR)
                dt.Columns.Add("dolphinPair", Utils.getSystemType(getDataType()));
            else
                dt.Columns.Add("dolphinVector", Utils.getSystemType(getDataType()));

            return dt;
        }

        public object getObject()
        {
            throw new NotImplementedException();
        }

        public virtual object getList()
        {
            throw new NotImplementedException();
        }


        public abstract void set(int index, string value);
        public abstract void add(object value);
        public abstract void addRange(object list);

        public abstract int hashBucket(int index, int buckets);
        public abstract IVector getSubVector(int[] indices);

        public abstract int asof(IScalar value);

        public static void checkCompressedMethod(DATA_TYPE type, int method){
            if ((int)type > ARRAY_VECTOR_BASE)
            {
                type -= ARRAY_VECTOR_BASE;
            }
            switch (method)
            {
                case Vector_Fields.COMPRESS_DELTA:
                    {
                        switch (type)
                        {
                            case DATA_TYPE.DT_SHORT:
                            case DATA_TYPE.DT_INT:
                            case DATA_TYPE.DT_DATE:
                            case DATA_TYPE.DT_MONTH:
                            case DATA_TYPE.DT_TIME:
                            case DATA_TYPE.DT_MINUTE:
                            case DATA_TYPE.DT_SECOND:
                            case DATA_TYPE.DT_DATETIME:
                            case DATA_TYPE.DT_DATEHOUR:
                            case DATA_TYPE.DT_DATEMINUTE:
                            case DATA_TYPE.DT_LONG:
                            case DATA_TYPE.DT_NANOTIME:
                            case DATA_TYPE.DT_TIMESTAMP:
                            case DATA_TYPE.DT_NANOTIMESTAMP:
                            case DATA_TYPE.DT_DECIMAL32:
                            case DATA_TYPE.DT_DECIMAL64:
                            case DATA_TYPE.DT_DECIMAL128:
                                break;
                            default:
                                throw new InvalidOperationException("The data type of " + type + " does not support delta compression");
                        }
                        break;
                    }
                case Vector_Fields.COMPRESS_LZ4:
                    {
                        switch (type)
                        {
                            case DATA_TYPE.DT_BOOL:
                            case DATA_TYPE.DT_BYTE:
                            case DATA_TYPE.DT_SHORT:
                            case DATA_TYPE.DT_INT:
                            case DATA_TYPE.DT_DATE:
                            case DATA_TYPE.DT_MONTH:
                            case DATA_TYPE.DT_TIME:
                            case DATA_TYPE.DT_MINUTE:
                            case DATA_TYPE.DT_SECOND:
                            case DATA_TYPE.DT_DATETIME:
                            case DATA_TYPE.DT_DATEHOUR:
                            case DATA_TYPE.DT_DATEMINUTE:
                            case DATA_TYPE.DT_LONG:
                            case DATA_TYPE.DT_NANOTIME:
                            case DATA_TYPE.DT_TIMESTAMP:
                            case DATA_TYPE.DT_NANOTIMESTAMP:

                            case DATA_TYPE.DT_INT128:
                            case DATA_TYPE.DT_COMPLEX:
                            case DATA_TYPE.DT_UUID:
                            case DATA_TYPE.DT_IPADDR:
                            case DATA_TYPE.DT_FLOAT:
                            case DATA_TYPE.DT_DOUBLE:
                            case DATA_TYPE.DT_STRING:
                            case DATA_TYPE.DT_SYMBOL:
                            case DATA_TYPE.DT_BLOB:
                            case DATA_TYPE.DT_DECIMAL32:
                            case DATA_TYPE.DT_DECIMAL64:
                            case DATA_TYPE.DT_DECIMAL128:
                                break;
                            default:
                                throw new InvalidOperationException("The data type of " + type + " does not support lz4 compression");
                        }
                        break;
                    }

                default: throw new InvalidOperationException("Invalid data compression method. ");

            }
        }

        public void setCompressedMethod(int method)
        {
            DATA_TYPE type = this.getDataType();
            checkCompressedMethod(type, method);
            this.compressedMethod = method;
        }

        public static int getUnitLength(DATA_TYPE type)
        {
            int unitLength = 0;
            if ((int)type > 64)
                type -= 64;
            switch (type)
            {
                case DATA_TYPE.DT_STRING:
                case DATA_TYPE.DT_BLOB:
                    unitLength = 0;
                    break;
                case DATA_TYPE.DT_BOOL:
                case DATA_TYPE.DT_BYTE:
                    unitLength = 1;
                    break;
                case DATA_TYPE.DT_SHORT:
                    unitLength = 2;
                    break;
                case DATA_TYPE.DT_INT:
                case DATA_TYPE.DT_DATE:
                case DATA_TYPE.DT_MONTH:
                case DATA_TYPE.DT_TIME:
                case DATA_TYPE.DT_MINUTE:
                case DATA_TYPE.DT_SECOND:
                case DATA_TYPE.DT_DATETIME:
                case DATA_TYPE.DT_FLOAT:
                case DATA_TYPE.DT_DATEHOUR:
                case DATA_TYPE.DT_DATEMINUTE:
                case DATA_TYPE.DT_SYMBOL:
                case DATA_TYPE.DT_DECIMAL32:
                    unitLength = 4;
                    break;
                case DATA_TYPE.DT_LONG:
                case DATA_TYPE.DT_DOUBLE:
                case DATA_TYPE.DT_NANOTIME:
                case DATA_TYPE.DT_TIMESTAMP:
                case DATA_TYPE.DT_NANOTIMESTAMP:
                case DATA_TYPE.DT_DECIMAL64:
                    unitLength = 8;
                    break;
                case DATA_TYPE.DT_INT128:
                case DATA_TYPE.DT_COMPLEX:
                case DATA_TYPE.DT_UUID:
                case DATA_TYPE.DT_IPADDR:
                case DATA_TYPE.DT_DECIMAL128:
                    unitLength = 16;
                    break;
                default:
                    throw new InvalidOperationException("Type " + type + " is an unsupported Dolphindb data type");
            }
            return unitLength;
        }

        public virtual void writeCompressed(ExtendedDataOutput output) {
            int dataType = (int)this.getDataType();
            int unitLength = getUnitLength(this.getDataType());
		    int elementCount = this.rows();
		    int maxCompressedLength = this.rows() * sizeof(long) * 8 * 2 + 64 * 3;

            ByteBuffer outBuffer = ByteBuffer.Allocate(Math.Max(maxCompressedLength, 65536));
            outBuffer.order(output.GetType() == typeof(LittleEndianDataOutputStream));
            short flag = (short) ((short)DATA_FORM.DF_VECTOR<< 8 | (short)DATA_TYPE.DT_COMPRESS & 0xff);

            outBuffer.WriteShort(flag);
            outBuffer.WriteInt(0);// compressedBytes
            outBuffer.WriteInt(1);// cols
            outBuffer.WriteByte((byte) 0); // version
            outBuffer.WriteByte((byte) 1); // flag bit0:littleEndian bit1:containChecksum
            outBuffer.WriteByte(unchecked((byte) -1)); // charcode
            outBuffer.WriteByte((byte) compressedMethod);
            outBuffer.WriteByte((byte) dataType);
            outBuffer.WriteByte((byte) unitLength);
            if (Utils.getCategory((DATA_TYPE)(dataType - (dataType >　ARRAY_VECTOR_BASE ? 64 : 0))) == DATA_CATEGORY.DENARY)
                outBuffer.WriteShort((short)getExtraParamForType());
            else
                outBuffer.WriteShort((short)-1);
            outBuffer.WriteInt(-1); //extra
            outBuffer.WriteInt(elementCount);
            outBuffer.WriteInt(-1); //TODO: checkSum
            EncoderFactory.Get(compressedMethod).compress(this, elementCount, unitLength, maxCompressedLength, outBuffer);
            int compressedLength = outBuffer.ReadableBytes - 10;
            outBuffer.PutInt(compressedLength, 2);
            byte[] tmp = new byte[outBuffer.ReadableBytes];
            output.write(outBuffer.ToArray());
        }

        public abstract void deserialize(int start, int count, ExtendedDataInput @in);

        public abstract void serialize(int start, int count, ExtendedDataOutput @out);

        public abstract int serialize(int indexStart, int offect, int targetNumElement, out int numElement, out int partial,  ByteBuffer @out);

        public abstract int getUnitLength();

        public abstract void append(IScalar value);

        public abstract void append(IVector value);
        public abstract IEntity getEntity(int index);

        public virtual int getScale()
        {
            throw new NotImplementedException();
        }
    }
};
