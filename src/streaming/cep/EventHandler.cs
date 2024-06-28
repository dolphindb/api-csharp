using dolphindb.data;
using dolphindb.io;
using dolphindb.route;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace dolphindb.streaming.cep
{
    public class EventSchema
    {
        public EventSchema(string eventType, List<string> fieldNames, List<DATA_TYPE> fieldTypes, List<DATA_FORM> fieldForms, List<int> fieldExtraParams = null)
        {
            Utils.checkStringNotNullAndEmpty(eventType, "eventType");
            Utils.checkParamIsNull(fieldNames, "fieldNames");
            Utils.checkParamIsNull(fieldTypes, "fieldTypes");
            Utils.checkParamIsNull(fieldForms, "fieldForms");
            eventType_ = eventType;
            fieldNames_ = fieldNames;
            fieldTypes_ = fieldTypes;
            fieldForms_ = fieldForms;
            for(int i = 0; i < fieldNames_.Count; ++i)
            {
                Utils.checkStringNotNullAndEmpty(fieldNames_[i], "the element of fieldNames");
            }
            for(int i = 0; i < fieldForms.Count; ++i)
            {
                if (fieldForms[i] != DATA_FORM.DF_VECTOR && fieldForms[i] != DATA_FORM.DF_SCALAR)
                {
                    throw new Exception("FieldForm only can be DF_SCALAR or DF_VECTOR.");
                }
            }

            for (int i = 0; i < fieldTypes_.Count; ++i)
            {
                DATA_CATEGORY category = Utils.getCategory(fieldTypes_[i]);
                if(category == DATA_CATEGORY.NOTHING || category == DATA_CATEGORY.SYSTEM || category == DATA_CATEGORY.MIXED)
                {
                    throw new Exception("not support " + Utils.getDataTypeString(fieldTypes_[i]) + " type. ");
                }
            }
            if (fieldExtraParams == null)
            {
                fieldExtraParams_ = new List<int> { };
                for(int  i = 0; i < fieldNames_.Count; ++i)
                {
                    fieldExtraParams_.Add(0);
                }
            }
            else
            {
                fieldExtraParams_ = new List<int> { };
                fieldExtraParams_.AddRange(fieldExtraParams);
            }
        }

        public string geteEventType()
        {
            return eventType_;
        }

        public List<string> getFieldNames()
        {
            return fieldNames_;
        }

        public List<DATA_TYPE> getFieldTypes()
        {
            return fieldTypes_;
        }

        public List<DATA_FORM> getFieldForms()
        {
            return fieldForms_;
        }

        public List<int> getFieldExtraParams()
        {
            return fieldExtraParams_;
        }

        private string eventType_;
        private List<string> fieldNames_;
        private List<DATA_TYPE> fieldTypes_;
        private List<DATA_FORM> fieldForms_;
        private List<int> fieldExtraParams_;
    };



    class EventSchemaEx
    {
        public EventSchema schema_;
        public int timeIndex_;
        public List<int> commonKeyIndex_ = new List<int>();
    };

    class AttributeSerializer
    {
        public AttributeSerializer()
        {
        }
        public virtual void serialize(IEntity attribute, AbstractExtendedDataOutputStream outStream, int scale)
        {
            if (attribute.isScalar())
            {
                if(((AbstractScalar)attribute).getDataCategory() == DATA_CATEGORY.DENARY && ((AbstractScalar)attribute).getScale() != scale)
                {
                    attribute = Utils.createObject(attribute.getDataType(), attribute.getString(), scale);
                }
                ((AbstractScalar)attribute).writeScalarToOutputStream(outStream);
            }
            else if (attribute.isVector())
            {
                int type = (int)attribute.getDataType();
                if(attribute is BasicSymbolVector)
                {
                    type += 128;
                }
                int flag = ((int)DATA_FORM.DF_VECTOR << 8) + type;
                outStream.writeShort(flag);
                outStream.writeInt(attribute.rows());
                outStream.writeInt(attribute.columns());
                ((AbstractVector)attribute).writeVectorToOutputStream(outStream);
            }else
            {
                throw new Exception("the " + Utils.getDataFormString(attribute.getDataForm()) + " is not supported. ");
            }
        }
    };

    struct EventInfo
    {
        public List<AttributeSerializer> attributeSerializers_;
        public EventSchemaEx eventSchema_;
    };


    class FastArrayAttributeSerializer : AttributeSerializer
    {
        public FastArrayAttributeSerializer() : base() { }
        public override void serialize(IEntity attribute, AbstractExtendedDataOutputStream outStream, int scale)
        {
            {
                if (attribute.getDataCategory() == DATA_CATEGORY.DENARY)
                {
                    if(((AbstractVector)attribute).getScale() != scale)
                    {
                        int rows = attribute.rows();
                        AbstractVector tmp = (AbstractVector)BasicEntityFactory.instance().createVectorWithDefaultValue(attribute.getDataType(), rows, scale);
                        for(int i = 0; i < rows; i++)
                        {
                            tmp.set(i, ((AbstractVector)attribute).get(i));
                        }
                        attribute = tmp;
                    }
                }
            }
            int curCount = attribute.rows();
            if (curCount == 0)
            {
                short tCnt = (short)curCount;
                outStream.writeShort(tCnt);
                return;
            }

            ushort arrayRows = 1;
            byte curCountBytes = 1;
            byte reserved = 0;
            int maxCount = 255;
            while (curCount > maxCount)
            {
                curCountBytes *= 2;
                maxCount = (int)((1L << (8 * curCountBytes)) - 1);
            }
            outStream.writeShort(arrayRows);
            outStream.writeByte(curCountBytes);
            outStream.writeByte(reserved);
            switch (curCountBytes)
            {
                case 1:
                    {
                        byte tCnt = (byte)curCount;
                        outStream.writeByte(tCnt);
                        break;
                    }
                case 2:
                    {
                        ushort tCnt = (ushort)curCount;
                        outStream.writeShort((short)tCnt);
                        break;
                    }
                default:
                    {
                        uint tCnt = (uint)curCount;
                        outStream.writeInt((int)tCnt);
                    }
                    break;
            }
            ((AbstractVector)attribute).serialize(0, curCount, outStream);
        }
    };



    class EventHandler
    {
        public EventHandler(List<EventSchema> eventSchemas, List<string> eventTimeKeys, List<string> commonKeys)
        {
            Utils.checkParamIsNull(eventSchemas, "eventSchemas");
            Utils.checkParamIsNull(eventTimeKeys, "eventTimeKeys");
            Utils.checkParamIsNull(commonKeys, "commonKeys");
            isNeedEventTime_ = false;
            outputColNums_ = 0;
            //check eventSchemas
            if (eventSchemas.Count == 0)
            {
                throw new Exception("eventSchemas must not be empty.");
            }
            List<EventSchema> expandEventSchemas = eventSchemas;
            foreach (EventSchema item in expandEventSchemas)
            {
                if (item.geteEventType().Length == 0)
                {
                    throw new Exception("eventType must not be empty.");
                }
                int length = item.getFieldNames().Count;
                if (length == 0)
                {
                    throw new Exception("the eventKey in eventSchema must not be empty.");
                }
                if (length != item.getFieldExtraParams().Count() || length != item.getFieldForms().Count() || length != item.getFieldTypes().Count())
                {
                    throw new Exception("The number of eventKey, eventTypes, eventForms and eventExtraParams must have the same length.");
                }
            }
            int eventNum = eventSchemas.Count();
            //check eventTimeKeys
            List<string> expandTimeKeys = new List<string>();
            if (eventTimeKeys.Count != 0)
            {
                //if eventTimeKeys only contain one element, it means every event has this key
                if (eventTimeKeys.Count == 1)
                {
                    for (int i = 0; expandTimeKeys.Count < eventNum; ++i)
                    {
                        expandTimeKeys.Add(eventTimeKeys[0]);
                    }
                }
                else
                {
                    if (eventTimeKeys.Count != eventNum)
                    {
                        throw new Exception("The number of eventTimeKey is inconsistent with the number of events in eventSchemas.");
                    }
                    expandTimeKeys = eventTimeKeys;
                }
                isNeedEventTime_ = true;
            }
            checkSchema(expandEventSchemas, expandTimeKeys, commonKeys);
            commonKeySize_ = commonKeys.Count();
        }
        public void checkOutputTable(BasicTable outputTable, string tableName)
        {
            outputColNums_ = isNeedEventTime_ ? (3 + commonKeySize_) : (2 + commonKeySize_);
            if (outputColNums_ != outputTable.columns())
            {
                throw new Exception("Incompatible table columnns for table " + tableName + ", expected: " + outputColNums_.ToString() + ", got: " + outputTable.columns().ToString() + ".");
            }
            int colIdx = 0;
            if (isNeedEventTime_)
            {
                if (Utils.getCategory(outputTable.getColumn(0).getDataType()) != DATA_CATEGORY.TEMPORAL)
                {
                    throw new Exception("The first column of the output table must be temporal if eventTimeKey is specified.");
                }
                colIdx++;
            }
            int typeIdx_ = colIdx++;
            int blobIdx_ = colIdx++;

            if (outputTable.getColumn(typeIdx_).getDataType() != DATA_TYPE.DT_STRING && outputTable.getColumn(typeIdx_).getDataType() != DATA_TYPE.DT_SYMBOL)
            {
                throw new Exception("The eventType column of the output table must be STRING or SYMBOL type.");
            }
            if (outputTable.getColumn(blobIdx_).getDataType() != DATA_TYPE.DT_BLOB)
            {
                throw new Exception("The event column of the output table must be BLOB type.");
            }
        }
        public void serializeEvent(string eventType, List<IEntity> attributes, List<IEntity> serializedEvent)
        {
            if (!eventInfos_.ContainsKey(eventType))
            {
                throw new Exception("unknown eventType " + eventType + ".");
            }

            EventInfo info = eventInfos_[eventType];
            if (attributes.Count() != info.attributeSerializers_.Count())
            {
                throw new Exception("the number of event values does not match.");
            }


            List<DATA_TYPE> fieldTypes = info.eventSchema_.schema_.getFieldTypes();
            List<string> fieldNames = info.eventSchema_.schema_.getFieldNames();
            List<DATA_FORM> fieldForms = info.eventSchema_.schema_.getFieldForms();
            for (int i = 0; i < attributes.Count(); ++i)
            {
                if (fieldTypes[i] != attributes[i].getDataType())
                {
                    //An exception: when the type in schema is symbol, you can pass a string attribute
                    if (fieldTypes[i] == DATA_TYPE.DT_SYMBOL && attributes[i].getDataType() == DATA_TYPE.DT_STRING)
                    {
                        continue;
                    }
                    throw new Exception("Expected type for the field " + fieldNames[i] + " of " + eventType + " : " + Utils.getDataTypeString(fieldTypes[i]) +
                    ", but now it is " + Utils.getDataTypeString(attributes[i].getDataType()) + ".");
                }
                if (fieldForms[i] != attributes[i].getDataForm())
                {
                    throw new Exception("Expected form for the field " + fieldNames[i] + " of " + eventType + " : " + Utils.getDataFormString(fieldForms[i]) +
                    ", but now it is " + Utils.getDataFormString(attributes[i].getDataForm()) + ".");
                }
            }

            //std::vector<ConstantSP> oneLineContent;
            BasicAnyVector oneLineContent = new BasicAnyVector(0);
            if (isNeedEventTime_)
            {
                if (attributes[info.eventSchema_.timeIndex_].isScalar())
                {
                    oneLineContent.append(attributes[info.eventSchema_.timeIndex_]);
                }
            }
            oneLineContent.append(new BasicString(eventType));
            //serialize all attribute to a blob
            MemoryStream myStream = new MemoryStream();
            LittleEndianDataOutputStream outStream = new LittleEndianDataOutputStream(myStream);
            List<int> extraParams = info.eventSchema_.schema_.getFieldExtraParams();
            for (int i = 0; i < attributes.Count(); ++i)
            {
                info.attributeSerializers_[i].serialize(attributes[i], outStream, extraParams[i]);
            }
            oneLineContent.append(new BasicString(myStream.ToArray(), true));

            foreach (int commonIndex in info.eventSchema_.commonKeyIndex_)
            {
                if (fieldForms[commonIndex] == DATA_FORM.DF_VECTOR)
                {
                    BasicAnyVector any = new BasicAnyVector(0);
                    any.append(attributes[commonIndex]);
                    oneLineContent.append(any);
                }
                else
                {
                    oneLineContent.append(attributes[commonIndex]);
                }
            }
            serializedEvent.Add(oneLineContent);
        }
        public void deserializeEvent(List<IMessage> obj, List<string> eventTypes, List<List<IEntity>> attributes)
        {
            int eventTypeIndex = isNeedEventTime_ ? 1 : 0;
            int blobIndex = isNeedEventTime_ ? 2 : 1;
            int rowSize = obj.Count;
            for (int rowIndex = 0; rowIndex < rowSize; ++rowIndex)
            {
                string eventType = obj[rowIndex].getEntity(eventTypeIndex).getString();
                if (!eventInfos_.ContainsKey(eventType))
                {
                    throw new Exception("UnKnown eventType " + eventType + ".");
                }
                byte[] blob = ((BasicString)obj[rowIndex].getEntity(blobIndex)).getBytes();


                MemoryStream memoryStream = new MemoryStream();
                ExtendedDataOutput writeStream = new BigEndianDataOutputStream(memoryStream);
                writeStream.write(blob);
                LittleEndianDataInputStream input = new LittleEndianDataInputStream(new MemoryStream(memoryStream.GetBuffer(), 0, (int)memoryStream.Position));

                EventSchema schema = eventInfos_[eventType].eventSchema_.schema_;
                List<DATA_TYPE> fieldTypes = schema.getFieldTypes();
                List<string> fieldNames = schema.getFieldNames();
                List<DATA_FORM> fieldForms = schema.getFieldForms();
                List<int> fieldExtraParams = schema.getFieldExtraParams();
                int attrCount = fieldTypes.Count();
                List<IEntity> datas = new List<IEntity>();
                for (int i = 0; i < attrCount; ++i)
                {
                    DATA_FORM form = fieldForms[i];
                    DATA_TYPE type = fieldTypes[i];
                    int extraParam = fieldExtraParams[i];
                    if (form == DATA_FORM.DF_SCALAR)
                    {
                        if (type == DATA_TYPE.DT_ANY)
                        {
                            datas.Add(deserializeAny(input));
                        }
                        else
                        {
                            datas.Add(deserializeScalar(type, extraParam, input));
                        }
                    }
                    else if (form == DATA_FORM.DF_VECTOR)
                    {
                        if (((int)type) < 64 && Utils.getCategory(type) != DATA_CATEGORY.LITERAL)
                        {
                            datas.Add(deserializeFastArray(type, extraParam, input));
                        }
                        else
                        {
                            datas.Add(deserializeAny(input));
                        }
                    }
                    else
                    {
                        datas.Add(deserializeAny(input));
                    }
                    if (datas[i] == null)
                    {
                        throw new Exception("Deserialize blob error.");
                    }
                }
                eventTypes.Add(eventType);
                attributes.Add(datas);
            }
        }
        private void checkSchema(List<EventSchema> eventSchemas, List<string> expandTimeKeys, List<string> commonKeys)
        {
            int index = 0;
            foreach (EventSchema schema in eventSchemas)
            {
                if (eventInfos_.ContainsKey(schema.geteEventType()))
                {
                    throw new Exception("EventType must be unique.");
                }

                EventSchemaEx schemaEx = new EventSchemaEx();
                schemaEx.schema_ = schema;
                List<string> fieldNames = schema.getFieldNames();
                if (isNeedEventTime_)
                {
                    if (!fieldNames.Contains(expandTimeKeys[index]))
                    {
                        throw new Exception("Event " + schema.geteEventType() + " doesn't contain eventTimeKey " + expandTimeKeys[index] + ".");
                    }
                    schemaEx.timeIndex_ = fieldNames.FindIndex(0, x => x == expandTimeKeys[index]);
                }

                foreach (string commonKey in commonKeys)
                {
                    if (!fieldNames.Contains(commonKey))
                    {
                        throw new Exception("Event " + schema.geteEventType() + " doesn't contain commonField " + commonKey + ".");
                    }
                    schemaEx.commonKeyIndex_.Add(fieldNames.FindIndex(0, x => x == commonKey));
                }

                List<AttributeSerializer> serls = new List<AttributeSerializer>();
                if(fieldNames.Distinct().Count() != fieldNames.Count())
                    throw new Exception("fieldNames must be unique.");
                int length = fieldNames.Count;
                List<DATA_TYPE> fieldTypes = schema.getFieldTypes();
                List<DATA_FORM> fieldForms = schema.getFieldForms();
                for (int j = 0; j < length; ++j)
                {
                    DATA_TYPE type = fieldTypes[j];
                    DATA_FORM form = fieldForms[j];
                    if(form == DATA_FORM.DF_VECTOR && ((int)type) < AbstractVector.ARRAY_VECTOR_BASE && type != DATA_TYPE.DT_ANY)
                    {
                        int unitLen = AbstractVector.getUnitLength(type);
                        if(type == DATA_TYPE.DT_SYMBOL)
                        {
                            unitLen = -1;
                        }
                        if(unitLen > 0)
                        {
                            serls.Add(new FastArrayAttributeSerializer());
                            continue;
                        }
                    }
                    serls.Add(new AttributeSerializer());
                }

                EventInfo info;
                info.attributeSerializers_ = serls;
                info.eventSchema_ = schemaEx;
                eventInfos_[schema.geteEventType()] = info;
                index++;
            }
        }
        IEntity deserializeScalar(DATA_TYPE type, int extraParam, AbstractExtendedDataInputStream input)
        {
            if(Utils.getCategory(type) == DATA_CATEGORY.DENARY)
            {
                if(type == DATA_TYPE.DT_DECIMAL128)
                {
                    return new BasicDecimal128(input, extraParam);
                }else if(type == DATA_TYPE.DT_DECIMAL64)
                {
                    return new BasicDecimal64(input, extraParam);
                }
                else
                {
                    return new BasicDecimal32(input, extraParam);
                }
            }
            else
            {
                return (IScalar)BasicEntityFactory.instance().createEntity(DATA_FORM.DF_SCALAR, type, input, false);
            }
        }
        IVector deserializeFastArray(DATA_TYPE type, int extraParam, AbstractExtendedDataInputStream input)
        {
            int totalCount = input.ReadInt16();
            if(totalCount == 0)
            {
                return BasicEntityFactory.instance().createVectorWithDefaultValue(type, 0, extraParam);
            }
            byte countByte = input.ReadByte();
            input.ReadByte();
            int count;
            if (countByte == 1)
            {
                count = input.ReadByte();
            }else if(countByte == 2)
            {
                count = input.ReadUInt16();
            }
            else
            {
                count = input.readInt();
            }
            AbstractVector vector = (AbstractVector)BasicEntityFactory.instance().createVectorWithDefaultValue(type, count, extraParam);
            vector.deserialize(0, count, input);
            return vector;
        }
        IEntity deserializeAny(AbstractExtendedDataInputStream input)
        {
            short flag = input.ReadInt16();
            int form = flag >> 8;
            int type = flag & 0xff;
            bool extended = type >= 128;
            if (type >= 128)
                type -= 128;
            IEntity ret = BasicEntityFactory.instance().createEntity((DATA_FORM)form, (DATA_TYPE)type, input, extended);
            return ret;
        }

        Dictionary<string, EventInfo> eventInfos_ = new Dictionary<string, EventInfo>();
        bool isNeedEventTime_;

        int outputColNums_;                 //the number of columns of the output table
        int commonKeySize_;
    };
}
