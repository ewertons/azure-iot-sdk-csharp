using Microsoft.Azure.Amqp;
using Microsoft.Azure.Devices.Shared;
using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpConnectionAllocator : IResourceAllocator<IAmqpConnectionResource>
    {
        public async Task<IAmqpConnectionResource> AllocateResourceAsync(DeviceIdentity deviceIdentity, IResourceStatusListener<IAmqpConnectionResource> resourceStatusListener, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, deviceIdentity, timeout, $"{nameof(AllocateResourceAsync)}");
            AmqpConnector amqpConnector = new AmqpConnector();
            AmqpConnection amqpConnection = await amqpConnector.OpenConnectionAsync(deviceIdentity.AmqpTransportSettings, deviceIdentity.IotHubConnectionString.HostName, timeout).ConfigureAwait(false);
            AmqpCbsLink amqpCbsLink = new AmqpCbsLink(amqpConnection);
            IAmqpAuthenticationRefresher amqpAuthenticationRefresher = null;
            if (deviceIdentity.AuthenticationModel == AuthenticationModel.SasGrouped)
            {
                try
                {
                    amqpAuthenticationRefresher = await AmqpAuthenticationRefresher.InitializeAsync(
                        amqpCbsLink,
                        deviceIdentity.IotHubConnectionString,
                        deviceIdentity.IotHubConnectionString.AmqpEndpoint,
                        deviceIdentity.Audience,
                        timeout
                    ).ConfigureAwait(false);
                    if (Logging.IsEnabled) Logging.Associate(deviceIdentity, amqpAuthenticationRefresher, $"{nameof(AllocateResourceAsync)}");
                }
                catch (InvalidOperationException)
                {
                    if (amqpConnection.IsClosing())
                    {
                        throw AmqpConnectionResource.s_amqpConnectionDisconnectedException;
                    }
                }
                catch (Exception)
                {
                    amqpConnection.SafeClose();
                    throw;
                }
            }

            IAmqpConnectionResource amqpConnectionResource = new AmqpConnectionResource(amqpConnection, amqpCbsLink, amqpAuthenticationRefresher);
            amqpConnection.Closed += (obj, args) =>
            {
                amqpAuthenticationRefresher?.Dispose();
                resourceStatusListener?.OnResourceStatusChange(amqpConnectionResource, ResourceStatus.Disconnected);
            };

            // safty check, incase connection was closed before event handler attached
            if (amqpConnection.IsClosing())
            {
                amqpConnectionResource.Abort();
                throw AmqpConnectionResource.s_amqpConnectionDisconnectedException;
            }

            if (Logging.IsEnabled) Logging.Exit(this, deviceIdentity, timeout, $"{nameof(AllocateResourceAsync)}");
            return amqpConnectionResource;
        }
    }
}
