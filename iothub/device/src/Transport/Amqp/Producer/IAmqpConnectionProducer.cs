using Microsoft.Azure.Devices.Client.Transport.Amqp.Module;

namespace Microsoft.Azure.Devices.Client.Transport.Amqp.Producer
{
    internal interface IAmqpConnectionProducer
    {
        IAmqpConnection AllocateAmqpConnection();
    }
}
