using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.data
{
    public class BasicIotAnyVector : AbstractVector
    {
        Dictionary<int, AbstractVector> subVectors;

        BasicIntVector dataTypes;

        BasicIntVector indexs;

        public BasicIotAnyVector(List<IScalar> scalars) : base(DATA_FORM.DF_VECTOR)
        {
            if (scalars == null || scalars.Count == 0)
                throw new Exception("The param 'scalars' cannot be null or empty.");
            int size = scalars.Count;
            subVectors = new Dictionary<int, AbstractVector>();
            dataTypes = new BasicIntVector(size);
            indexs = new BasicIntVector(size);
            for (int i = 0; i < size; i++)
            {
                DATA_TYPE type = scalars[i].getDataType();
                int typeInt = (int)type;
                if (!subVectors.ContainsKey(typeInt))
                {
                    subVectors.Add(typeInt, (AbstractVector)BasicEntityFactory.instance().createVectorWithDefaultValue(type, 0));
                }
                subVectors[typeInt].append(scalars[i]);
                dataTypes.setInt(i, typeInt);
                indexs.setInt(i, subVectors[typeInt].rows() - 1);
            }
        }

        protected internal BasicIotAnyVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
        {
            BasicAnyVector basicAnyVector = new BasicAnyVector(@in);
            BasicIntVector intVector = (BasicIntVector)basicAnyVector.getEntity(0);
            int indexSize = intVector.getInt(0);
            int typeSize = intVector.getInt(1);

            int[] indexValue = new int[indexSize];
            int[] datatypeValue = new int[indexSize];
            intVector.getdataArray().CopyTo(2, indexValue, 0, indexSize);
            intVector.getdataArray().CopyTo(2 + indexSize, datatypeValue, 0, indexSize);
            indexs = new BasicIntVector(indexValue);
            dataTypes = new BasicIntVector(datatypeValue);
            subVectors = new Dictionary<int, AbstractVector>();
            for(int i = 0; i < typeSize; ++i)
            {
                IEntity entity = basicAnyVector.getEntity(i + 1);
                subVectors.Add((int)entity.getDataType(), (AbstractVector)entity);
            }
        }

        public override void add(object value)
        {
            throw new NotImplementedException();
        }

        public override void addRange(object list)
        {
            throw new NotImplementedException();
        }

        public override void append(IScalar value)
        {
            throw new NotImplementedException();
        }

        public override void append(IVector value)
        {
            throw new NotImplementedException();
        }

        public override void deserialize(int start, int count, ExtendedDataInput @in)
        {
            throw new NotImplementedException();
        }

        public override IScalar get(int index)
        {
            if(dataTypes.getInt(index) == 0)
            {
                return new Void();
            }
            else
            {
                return subVectors[dataTypes.getInt(index)].get(indexs.getInt(index));
            }
        }

        public override DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.MIXED;
        }

        public override DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_IOTANY;
        }

        public override Type getElementClass()
        {
            return typeof(IScalar);
        }

        public override IEntity getEntity(int index)
        {
            return get(index);
        }

        public override int getUnitLength()
        {
            throw new NotImplementedException();
        }

        public override bool isNull(int index)
        {
            return get(index).isNull();
        }

        public override int rows()
        {
            return dataTypes.rows();
        }

        public override void serialize(int start, int count, ExtendedDataOutput @out)
        {
            throw new NotImplementedException();
        }

        public override int serialize(int indexStart, int offect, int targetNumElement, out int numElement, out int partial, ByteBuffer @out)
        {
            throw new NotImplementedException();
        }

        public override void set(int index, IScalar value)
        {
            throw new NotImplementedException();
        }

        public override void set(int index, string value)
        {
            throw new NotImplementedException();
        }

        public override void setNull(int index)
        {
            throw new NotImplementedException();
        }

        protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
        {
            BasicIntVector intVector = new BasicIntVector(new int[2]{ indexs.rows(), subVectors.Count});
            intVector.append(indexs);
            intVector.append(dataTypes);
            BasicAnyVector anyVector = new BasicAnyVector(new IEntity[] { intVector });
            foreach (KeyValuePair<int, AbstractVector> kvp in subVectors)
            {
                anyVector.append(kvp.Value);
            }
            anyVector.writeVectorToOutputStream(@out);
        }
        protected internal int serializeAnyVectorRows()
        {
            return subVectors.Count + 1;
        }

        public override int hashBucket(int index, int buckets)
        {
            throw new NotImplementedException();
        }

        public override int asof(IScalar value)
        {
            throw new NotImplementedException();
        }

        public override IVector getSubVector(int[] indices)
        {
            throw new NotImplementedException();
        }
    }
}
