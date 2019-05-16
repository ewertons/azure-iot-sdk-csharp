using Microsoft.Azure.Amqp;
using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal interface IAmqpSessionResource : IResource
    {
        Task<IAmqpReceivingLinkResource> AllocateReceivingLinkAsync(
            AmqpLinkSettings amqpLinkSettings,
            IResourceStatusListener<IAmqpReceivingLinkResource> resourceStatusListener,
            TimeSpan timeout
        );

        Task<IAmqpSendingLinkResource> AllocateSendingLinkAsync(
            AmqpLinkSettings amqpLinkSettings,
            IResourceStatusListener<IAmqpSendingLinkResource> resourceStatusListener,
            TimeSpan timeout);

    }
}
