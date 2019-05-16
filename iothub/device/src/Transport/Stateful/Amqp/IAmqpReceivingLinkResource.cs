using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Framing;
using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal interface IAmqpReceivingLinkResource : IResource
    {
        Task<AmqpMessage> ReceiveAmqpMessageAsync(TimeSpan timeout);
        Task<Outcome> DisposeMessageAsync(string lockToken, Outcome outcome, TimeSpan timeout);
        void DisposeDelivery(AmqpMessage amqpMessage);
        void RegisterMessageListener(Action<AmqpMessage> messageListener);
    }
}
