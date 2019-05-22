using Microsoft.Azure.Amqp;
using Microsoft.Azure.Devices.Shared;
using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpConnectionAllocator : IResourceAllocator<IAmqpConnectionResource>
    {

        private static readonly IResourceAllocator<IAmqpConnectionResource> s_instance = new AmqpConnectionAllocator();

        private AmqpConnectionAllocator()
        {
        }

        internal static IResourceAllocator<IAmqpConnectionResource> GetInstance()
        {
            return s_instance;
        }

        public async Task<IAmqpConnectionResource> AllocateResourceAsync(DeviceIdentity deviceIdentity, IResourceStatusListener<IAmqpConnectionResource> resourceStatusListener, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, deviceIdentity, timeout, $"{nameof(AllocateResourceAsync)}");

            AmqpConnection amqpConnection = null;
            AmqpCbsLink amqpCbsLink = null;

            try
            {
                amqpConnection = await AmqpConnector.GetInstance().OpenConnectionAsync(deviceIdentity.AmqpTransportSettings, deviceIdentity.IotHubConnectionString.HostName, timeout).ConfigureAwait(false);
                amqpCbsLink = new AmqpCbsLink(amqpConnection);
            }
            catch (Exception e)
            {
                throw AmqpExceptionMapper.MapAmqpException(e);
            }

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
                catch (Exception e) when (e is InvalidOperationException || e is OperationCanceledException)
                {
                    if (amqpConnection.IsClosing())
                    {
                        throw AmqpConnectionResource.s_amqpConnectionDisconnectedException;
                    }
                    else
                    {
                        throw;
                    }
                }
                catch (Exception e)
                {
                    amqpConnection.SafeClose();
                    Exception ex = AmqpExceptionMapper.MapAmqpException(e);
                    if (ReferenceEquals(e, ex))
                    {
                        throw;
                    }
                    else
                    {
                        throw ex;
                    }
                }
            }

            IAmqpConnectionResource amqpConnectionResource = new AmqpConnectionResource(amqpConnection, amqpCbsLink, amqpAuthenticationRefresher);
            amqpConnection.Closed += (obj, args) =>
            {
                amqpAuthenticationRefresher?.Dispose();
                resourceStatusListener?.OnResourceStatusChange(amqpConnectionResource, ResourceStatus.Disconnected);
            };

            if (DeviceEventCounter.IsEnabled) DeviceEventCounter.OnAmqpConnectionEstablished();

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
