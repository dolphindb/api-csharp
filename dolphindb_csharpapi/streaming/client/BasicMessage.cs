namespace com.xxdb.streaming.client
{

	using BasicAnyVector = com.xxdb.data.BasicAnyVector;
	using Entity = com.xxdb.data.Entity;

	public class BasicMessage : IMessage
	{
		private long offset = 0;
		private string topic = "";
		private BasicAnyVector msg = null;

		public BasicMessage(long offset, string topic, BasicAnyVector msg)
		{
			this.offset = offset;
			this.topic = topic;
			this.msg = msg;
		}

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @SuppressWarnings("unchecked") @Override public <T> T getValue(int colIndex)
		public virtual T getValue<T>(int colIndex)
		{
			return (T)this.msg.getEntity(colIndex);
		}

		public virtual string Topic
		{
			get
			{
				return this.topic;
			}
		}

		public virtual long Offset
		{
			get
			{
				return this.offset;
			}
		}

		public virtual Entity getEntity(int colIndex)
		{
			return this.msg.getEntity(colIndex);
		}

	}

}