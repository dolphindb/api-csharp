using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.data;
using dolphindb.route;
using dolphindb.io;

namespace dolphindb.data
{
    public class BasicArrayVector : AbstractVector
    {
        private DATA_TYPE type;
        private List<int> rowIndices;
        private AbstractVector valueVec;
        private int baseUnitLength_;

        public BasicArrayVector(DATA_TYPE type) : base(DATA_FORM.DF_VECTOR)
        {
            this.type = type;
            this.rowIndices = new List<int>();
            DATA_TYPE valueType = type - 64;
            valueVec = (AbstractVector)BasicEntityFactory.instance().createVectorWithDefaultValue(valueType, 0);
            this.baseUnitLength_ = valueVec.getUnitLength();
        }
        
        public BasicArrayVector(DATA_TYPE type, ExtendedDataInput @in) : base(DATA_FORM.DF_VECTOR)
        {
            this.type = type;
            int rows = @in.readInt();
            int cols = @in.readInt();
            rowIndices = new List<int>(new int[rows]);
            DATA_TYPE valueType = type - 64;
            valueVec = (AbstractVector)BasicEntityFactory.instance().createVectorWithDefaultValue(valueType, cols);
            this.baseUnitLength_ = valueVec.getUnitLength();

            int rowsRead = 0;
            int rowsReadInBlock = 0;
            int prevIndex = 0;
            int totalBytes = 0;
             while(rowsRead < rows)
            {
                //read block header
                int blockRows = @in.readShort();
                int countBytes = @in.readChar();
                @in.skipBytes(1);

                //read array of count
                totalBytes = blockRows * countBytes;
                rowsReadInBlock = 0;
                int offect = 0;
                while(offect < totalBytes)
                {
                    int len = Math.Min(BUF_SIZE, totalBytes - offect);
                    @in.readFully(buf, 0, len);
                    int curRows = len / countBytes;
                    if(countBytes == 1)
                    {
                        for(int i = 0; i < curRows; i++)
                        {
                             int curRowCells = buf[i];
                            rowIndices[rowsRead + rowsReadInBlock + i] = prevIndex + curRowCells;
                            prevIndex += curRowCells;
                        }
                    }
                    else if(countBytes == 2)
                    {
                        for (int i = 0; i < curRows; ++i) {
                            int curRowCells = BitConverter.ToInt16(buf, i * 2);
                            rowIndices[rowsRead + rowsReadInBlock + i] = prevIndex + curRowCells;
                            prevIndex += curRowCells;
                        }
                    }
                    else
                    {
                        for (int i = 0; i < curRows; ++i)
                        {
                            int curRowCells = BitConverter.ToInt32(buf, i * 4);
                            rowIndices[rowsRead + rowsReadInBlock + i] = prevIndex + curRowCells;
                            prevIndex += curRowCells;
                        }
                    }
                    rowsReadInBlock += curRows;
                    offect += len;
                }

                //read array of values
                int rowStart = rowsRead == 0 ? 0 : rowIndices[rowsRead - 1];
                int valueCount = rowIndices[rowsRead + rowsReadInBlock - 1] - rowStart;
                valueVec.deserialize(rowStart, valueCount, @in);

                rowsRead += rowsReadInBlock;
            }
        }

        public BasicArrayVector(int[] index, IVector value):base(DATA_FORM.DF_VECTOR)
        {
            DATA_TYPE valueType = value.getDataType();
            this.type = valueType + 64;
            this.valueVec = (AbstractVector)value;
            int indexCount = index.Length;
            if (indexCount == 0)
                throw new Exception("Index must be an array of size greater than 0.");
            if (index[indexCount - 1] != value.rows())
                throw new Exception("The last element of index must be equal to the length of value. ");
            this.baseUnitLength_ = ((AbstractVector)value).getUnitLength();
            rowIndices = new List<int>(index);
        }

        public BasicArrayVector(List<IVector> value):base(DATA_FORM.DF_VECTOR)
        {
            DATA_TYPE valueType = value[0].getDataType();
            this.type = valueType + 64;
            int count = 0;
            int indexCount = value.Count;
            foreach (IVector temp in value)
            {
                if (temp.rows() == 0)
                    count += 1;
                else
                    count += temp.rows();
            }
            int indexPos = 0;
            this.rowIndices = new List<int>(new int[indexCount]);
            this.valueVec = (AbstractVector)BasicEntityFactory.instance().createVectorWithDefaultValue(valueType, count);
            int index = 0;
            int curRows = 0;
            for(int valuePos = 0; valuePos < indexCount; ++valuePos) {
                IVector temp = value[valuePos];
                int size = temp.rows();
                for (int i = 0; i < size; ++i)
                {
                    this.valueVec.set(index, temp.get(i));
                    index++;
                }
                if (size == 0)
                {
                    size = 1;
                    this.valueVec.setNull(index);
                    index++;
                }
                curRows += size;
                this.rowIndices[indexPos++] = curRows;
            }
            this.baseUnitLength_ = (this.valueVec).getUnitLength();
        }

        public override void deserialize(int start, int count, ExtendedDataInput @in)
        {
            throw new NotImplementedException();
        }

        public override void setNull(int index)
        {
            throw new NotImplementedException();
        }

        public override bool isNull(int index)
        {
            throw new NotImplementedException();
        }

        public override int rows()
        {
            return rowIndices.Count;
        }

        public override int columns()
        {
            return valueVec.rows();
        }

        public override void add(object value)
        {
            throw new NotImplementedException();
        }

        public override void addRange(object list)
        {
            throw new NotImplementedException();
        }

        public override void set(int index, IScalar value)
        {
            throw new NotImplementedException();
        }

        public override DATA_CATEGORY getDataCategory()
        {
            throw new NotImplementedException();
        }

        public override Type getElementClass()
        {
            throw new NotImplementedException();
        }

        public IVector getSubVector(int index)
        {
            int rowStart = index == 0 ? 0 : rowIndices[index - 1];
            int count = rowIndices[index] - rowStart;
            int[] indexVec = new int[count];
            for(int i = 0; i < count; ++i)
            {
                indexVec[i] = rowStart + i;
            }
            return valueVec.getSubVector(indexVec);
        }

        public override DATA_TYPE getDataType()
        {
            return type;
        }

        public override void set(int index, string value)
        {
            throw new NotImplementedException();
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            int indexCount = rowIndices.Count;
            int cols = valueVec.rows();
            int countBytes = 1;
            UInt32 maxCount = 255;
            int indicesPos = 0;
            int valuesOffect = 0;
            while (indicesPos < indexCount)
            {
                int byteRequest = 4;
                int curRows = 0;
                int indiceCount = 1;
                while (byteRequest < BUF_SIZE && indicesPos + indiceCount - 1 < indexCount && indiceCount < 65536)
                {
                    int curIndiceOffect = indicesPos + indiceCount - 1;
                    int index = curIndiceOffect == 0 ? rowIndices[curIndiceOffect] : rowIndices[curIndiceOffect] - rowIndices[curIndiceOffect - 1];
                    while(index > maxCount)
                    {
                        byteRequest += (indiceCount - 1) * countBytes;
                        countBytes *= 2;
                        maxCount = Math.Min(UInt32.MaxValue, (UInt32)1<<(8 * countBytes) - 1);
                    }
                    curRows += index;
                    indiceCount++;
                    byteRequest += countBytes + baseUnitLength_ * index; 
                }
                indiceCount--;
                @out.writeShort(indiceCount);
                @out.writeByte(countBytes);
                @out.writeByte(0);
                for (int i = 0; i < indiceCount; ++i)
                {
                    int index = indicesPos + i == 0 ? rowIndices[indicesPos + i] : rowIndices[indicesPos + i] - rowIndices[indicesPos + i - 1];
                    if (countBytes == 1)
                        @out.writeByte(index);
                    else if (countBytes == 2)
                        @out.writeShort(index);
                    else
                        @out.writeInt(index);
                }
                valueVec.serialize(valuesOffect, curRows, @out);
                indicesPos += indiceCount;
                valuesOffect += curRows;
            }
        }

        public override void serialize(int start, int count, ExtendedDataOutput @in)
        {
            throw new NotImplementedException();
        }

        public override IScalar get(int index)
        {
            throw new NotImplementedException();
        }

        public override int getUnitLength()
        {
            throw new NotImplementedException();
        }

        public override int serialize(int indexStart, int offect, int targetNumElement, out int numElement, out int partial, ByteBuffer @out)
        {
            numElement = 0;
            partial = 0;
            int byteSent = 0;
            int tmpNumElement = 0, tmpPartial = 0;

            if(offect > 0)
            {
                int rowStart = indexStart > 0 ? rowIndices[indexStart - 1] : 0;
                int cellCount = rowIndices[indexStart] - rowStart;
                int cellCountToSerialize = Math.Min(@out.remain() / baseUnitLength_, cellCount - offect);
                byteSent += valueVec.serialize(rowStart + offect, cellCount, numElement, out tmpNumElement, out tmpPartial, @out);
                if(cellCountToSerialize < cellCount - offect)
                {
                    partial = offect + cellCountToSerialize;
                    return byteSent;
                }
                else
                {
                    --targetNumElement;
                    ++numElement;
                    ++indexStart;
                }
            }

            int remainingBytes = @out.remain() - 4;
            int curCountBytes = 1;
            UInt32 maxCount = 255;
            int prestart = indexStart == 0 ? 0 : rowIndices[indexStart - 1];

            //one block can't exeed 65535 rows
            if (targetNumElement > 65535)
                targetNumElement = 65535;


            int i = 0;
            for(; i < targetNumElement && remainingBytes > 0; ++i)
            {
                int curStart = rowIndices[indexStart + i];
                int curCount = curStart - prestart;
                prestart = curStart;
                int byteRequired = 0;

                while(curCount > maxCount)
                {
                    byteRequired += i * curCountBytes;
                    curCountBytes *= 2;
                    maxCount = Math.Min(int.MaxValue, ((UInt32)1) << (8 * curCountBytes) - 1);
                }
                byteRequired += curCountBytes + curCount * baseUnitLength_;
                if(byteRequired > remainingBytes)
                {
                    if(numElement == 0)
                    {
                        partial = (remainingBytes - curCountBytes) / baseUnitLength_;
                        if(partial <= 0)
                        {
                            partial = 0;
                        }
                        else
                        {
                            ++i;
                            remainingBytes -= curCountBytes + partial * baseUnitLength_;
                        }
                    }
                    break;
                }
                else
                {
                    remainingBytes -= byteRequired;
                    ++numElement;
                }
            }

            if(i == 0)
                return byteSent;

            UInt16 rows = (UInt16)i;
            @out.WriteShort((short)rows);
            @out.WriteByte(curCountBytes);
            @out.WriteByte(0);
            byteSent += 4;

            //output array of counts
            prestart = indexStart == 0 ? 0 : rowIndices[indexStart - 1];
            for (int k = 0; k < rows; ++k)
            {
                int curStart = rowIndices[indexStart + k];
                int index = curStart - prestart;
                prestart = curStart;
                if (curCountBytes == 1)
                    @out.WriteByte((byte)index);
                else if (curCountBytes == 2)
                    @out.WriteShort((short)index);
                else
                    @out.WriteInt(index);
            }

            byteSent += curCountBytes * i;
            prestart = indexStart == 0 ? 0 : rowIndices[indexStart - 1];
            int count = (indexStart + numElement == 0 ? 0 : rowIndices[indexStart + numElement - 1]) + partial - prestart;
            int bytes;
            bytes = valueVec.serialize(prestart, 0, count, out tmpNumElement, out tmpPartial, @out);
            return byteSent + bytes;
        }

        public override void append(IVector value)
        {
            if (value is IVector)
            {
                int indexCount = rowIndices.Count;
                int prev = indexCount == 0 ? 0 : rowIndices[indexCount - 1];
                if (((IVector)value).rows() != 0)
                {
                    rowIndices.Add(prev + value.rows());
                    valueVec.append(value);
                }
                else
                {
                    rowIndices.Add(prev + 1);
                    IScalar scalar = BasicEntityFactory.instance().createScalarWithDefaultValue(value.getDataType());
                    scalar.setNull();
                    valueVec.append(scalar);
                }
            }
            else
                throw new Exception("Append to arrayctor must be a vector. ");
        }

        public override void append(IScalar value)
        {
            throw new NotImplementedException();
        }

        public override IEntity getEntity(int index)
        {
            return getSubVector(index);
        }
    }
}
