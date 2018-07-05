using System.Collections.Generic;

namespace com.xxdb.streaming.client
{


	internal interface MessageDispatcher
	{
		bool isRemoteLittleEndian(string host);
		void dispatch(IMessage message);
		void batchDispatch(IList<IMessage> message);
	}

}