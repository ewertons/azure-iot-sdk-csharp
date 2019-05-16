using System;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpSessionResourceHolder : ResourceHolder<IAmqpSessionResource>
    {
        #region Members-Constructor
        public AmqpSessionResourceHolder(IResourceAllocator<IAmqpSessionResource> amqpSessionResourceAllocator, Action onResourceDisconnection) : base(amqpSessionResourceAllocator, onResourceDisconnection)
        {
        }
        #endregion
    }
}
