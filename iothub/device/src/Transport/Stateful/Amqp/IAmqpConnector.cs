using Microsoft.Azure.Amqp;
using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal interface IAmqpConnector
    {
        Task<AmqpConnection> OpenConnectionAsync(AmqpTransportSettings amqpTransportSettings, string hostName, TimeSpan timeout);
    }
}
