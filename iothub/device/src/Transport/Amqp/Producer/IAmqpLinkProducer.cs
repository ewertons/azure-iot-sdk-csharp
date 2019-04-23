using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Azure.Devices.Client.Transport.Amqp.Producer
{
    internal interface IAmqpLinkProducer
    {
        IAmqpLink AllocateAmqpLink();
    }
}
