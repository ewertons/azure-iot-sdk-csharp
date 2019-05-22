using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.Devices.Client.Exceptions;
using Microsoft.Azure.Devices.Shared;
using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpConnectionResource : IAmqpConnectionResource
    {
        #region Members-Constructor
        internal static readonly Exception s_amqpConnectionDisconnectedException = new IotHubCommunicationException("AMQP Connection is disconnected.");

        private readonly AmqpConnection _amqpConnection;
        private readonly AmqpCbsLink _amqpCbsLink;
        private readonly IAmqpAuthenticationRefresher _amqpAuthenticationRefresher;

        private bool _disposed;

        internal AmqpConnectionResource(AmqpConnection amqpConnection, AmqpCbsLink amqpCbsLink, IAmqpAuthenticationRefresher amqpAuthenticationRefresher)
        {
            _amqpConnection = amqpConnection;
            _amqpCbsLink = amqpCbsLink;
            _amqpAuthenticationRefresher = amqpAuthenticationRefresher;
            if (Logging.IsEnabled) Logging.Associate(this, _amqpConnection, $"{nameof(AmqpConnectionResource)}");
            if (Logging.IsEnabled) Logging.Associate(this, _amqpCbsLink, $"{nameof(AmqpConnectionResource)}");
            if (Logging.IsEnabled) Logging.Associate(this, _amqpAuthenticationRefresher, $"{nameof(AmqpConnectionResource)}");
        }
        #endregion

        #region IResource
        public void Abort()
        {
            if (Logging.IsEnabled) Logging.Enter(this, $"Aborting {this}", $"{nameof(Abort)}");
            _amqpAuthenticationRefresher?.Dispose();
            _amqpConnection.SafeClose();
            if (Logging.IsEnabled) Logging.Exit(this, $"Aborting {this}", $"{nameof(Abort)}");
        }

        public bool IsValid()
        {
            return !_amqpConnection.IsClosing();
        }
        #endregion

        #region IResourceAllocator<IAmqpSessionResource>
        public async Task<IAmqpSessionResource> AllocateResourceAsync(DeviceIdentity deviceIdentity, IResourceStatusListener<IAmqpSessionResource> resourceStatusListener, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, deviceIdentity, timeout, $"{nameof(AllocateResourceAsync)}");
            if (!IsValid())
            {
                throw s_amqpConnectionDisconnectedException;
            }

            AmqpSession amqpSession = new AmqpSession(
                _amqpConnection,
                new AmqpSessionSettings()
                {
                    Properties = new Fields()
                },
                AmqpLinkFactory.GetInstance()
            );

            try
            {
                _amqpConnection.AddSession(amqpSession, new ushort?());
                await amqpSession.OpenAsync(timeout).ConfigureAwait(false);
                if (Logging.IsEnabled) Logging.Associate(deviceIdentity, amqpSession, $"{nameof(AllocateResourceAsync)}");
            }
            catch (Exception e) when (e is InvalidOperationException || e is OperationCanceledException)
            {
                if (IsValid())
                {
                    throw;
                }
                else
                {
                    throw s_amqpConnectionDisconnectedException;
                }
            }
            catch (Exception e)
            {
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

            IAmqpAuthenticationRefresher amqpAuthenticationRefresher = null;
            if (deviceIdentity.AuthenticationModel == AuthenticationModel.SasIndividual)
            {
                try
                {
                    amqpAuthenticationRefresher = await AmqpAuthenticationRefresher.InitializeAsync(
                        _amqpCbsLink, 
                        deviceIdentity.IotHubConnectionString,
                        deviceIdentity.IotHubConnectionString.AmqpEndpoint,
                        deviceIdentity.Audience,
                        timeout
                    ).ConfigureAwait(false);
                    if (Logging.IsEnabled) Logging.Associate(deviceIdentity, amqpAuthenticationRefresher, $"{nameof(AllocateResourceAsync)}");
                }
                catch (Exception e) when (e is InvalidOperationException || e is OperationCanceledException)
                {
                    if (amqpSession.IsClosing())
                    {
                        throw AmqpSessionResource.s_amqpSessionDisconnectedException;
                    }
                    else
                    {
                        throw;
                    }
                }
                catch (Exception e)
                {
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

            IAmqpSessionResource amqpSessionResource = new AmqpSessionResource(amqpSession, amqpAuthenticationRefresher);
            amqpSession.Closed += (obj, args) =>
            {
                amqpAuthenticationRefresher?.Dispose();
                resourceStatusListener?.OnResourceStatusChange(amqpSessionResource, ResourceStatus.Disconnected);
            };

            if (DeviceEventCounter.IsEnabled) DeviceEventCounter.OnAmqpSessionEstablished();

            // safty check, incase connection was closed before event handler attached
            if (amqpSession.IsClosing())
            {
                amqpSessionResource.Abort();
                throw AmqpSessionResource.s_amqpSessionDisconnectedException;
            }

            if (Logging.IsEnabled) Logging.Associate(deviceIdentity, amqpSessionResource, $"{nameof(AllocateResourceAsync)}");
            if (Logging.IsEnabled) Logging.Exit(this, deviceIdentity, timeout, $"{nameof(AllocateResourceAsync)}");
            return amqpSessionResource;
        }
        #endregion

        #region IDisposable
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed) return;
            _disposed = true;
            
            if (Logging.IsEnabled) Logging.Enter(this, disposing, $"{nameof(Dispose)}");
            if (disposing)
            {
                _amqpAuthenticationRefresher?.Dispose();
                _amqpConnection.SafeClose();
            }

            if (Logging.IsEnabled) Logging.Exit(this, disposing, $"{nameof(Dispose)}");
        }
        #endregion
    }
}
