using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Framing;
using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal interface IAmqpSendingLinkResource : IResource
    {
        Task<Outcome> SendAmqpMessageAsync(AmqpMessage message, TimeSpan timeout);
    }
}
