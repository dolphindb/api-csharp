namespace com.xxdb.streaming.client
{

	public interface MessageHandler : EventListener
	{
		void doEvent(IMessage msg);
	}

}