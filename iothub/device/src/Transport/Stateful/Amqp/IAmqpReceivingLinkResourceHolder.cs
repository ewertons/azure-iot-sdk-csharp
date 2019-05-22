using Microsoft.Azure.Amqp;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal interface IAmqpReceivingLinkResourceHolder : IResourceHolder<IAmqpReceivingLinkResource>
    {
        void DisposeDelivery(AmqpMessage amqpMessage);
    }
}
