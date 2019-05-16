using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp;
using Microsoft.Azure.Devices.Shared;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpAuthenticationRefresher : IAmqpAuthenticationRefresher
    {
        private static readonly string[] AccessRightsStringArray = AccessRightsHelper.AccessRightsToStringArray(AccessRights.DeviceConnect);
        private readonly CancellationTokenSource _cancellationTokenSource;

        private Task _refresher;
        private volatile bool _disposed;

        private AmqpAuthenticationRefresher(AmqpCbsLink amqpCbsLink, ICbsTokenProvider tokenProvider, Uri namespaceAddress, string audience, DateTime refreshOn, TimeSpan timeout, CancellationTokenSource cancellationTokenSource)
        {
            _cancellationTokenSource = cancellationTokenSource;
            _refresher = RefreshAsync(amqpCbsLink, tokenProvider, namespaceAddress, audience, refreshOn, timeout, cancellationTokenSource.Token);
        }

        private async Task RefreshAsync(AmqpCbsLink amqpCbsLink, ICbsTokenProvider tokenProvider, Uri namespaceAddress, string audience, DateTime refreshesOn, TimeSpan timeout, CancellationToken cancellationToken)
        {
            TimeSpan waitTime = refreshesOn - DateTime.UtcNow;

            while (!cancellationToken.IsCancellationRequested)
            {
                if (Logging.IsEnabled) Logging.Info(this, refreshesOn, $"Before {nameof(RefreshAsync)}");

                if (waitTime > TimeSpan.Zero)
                {
                    await Task.Delay(waitTime, cancellationToken).ConfigureAwait(false);
                }

                if (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        refreshesOn = await amqpCbsLink.SendTokenAsync(
                            tokenProvider,
                            namespaceAddress,
                            audience,
                            audience,
                            AccessRightsStringArray,
                            timeout
                        ).ConfigureAwait(false);
                    }
                    catch (AmqpException ex)
                    {
                        if (Logging.IsEnabled) Logging.Info(this, refreshesOn, $"Refresh token failed {ex}");
                    }
                    finally
                    {
                        if (Logging.IsEnabled) Logging.Info(this, refreshesOn, $"After {nameof(RefreshAsync)}");
                    }

                    waitTime = refreshesOn - DateTime.UtcNow;
                }
            }
        }

        public static async Task<IAmqpAuthenticationRefresher> InitializeAsync(AmqpCbsLink amqpCbsLink, ICbsTokenProvider tokenProvider, Uri namespaceAddress, string audience, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(typeof(AmqpAuthenticationRefresher), tokenProvider, namespaceAddress, audience, $"{nameof(InitializeAsync)}");
            DateTime refreshOn = await amqpCbsLink.SendTokenAsync(
                tokenProvider,
                namespaceAddress,
                audience,
                audience,
                AccessRightsStringArray,
                timeout
            ).ConfigureAwait(false);
            IAmqpAuthenticationRefresher amqpAuthenticationRefresher;
            if (refreshOn < DateTime.MaxValue)
            {
                CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                amqpAuthenticationRefresher = new AmqpAuthenticationRefresher(amqpCbsLink, tokenProvider, namespaceAddress, audience, refreshOn, timeout, cancellationTokenSource);
            }
            else
            {
                amqpAuthenticationRefresher = new NopRefresher();
            }
            if (Logging.IsEnabled) Logging.Exit(typeof(AmqpAuthenticationRefresher), tokenProvider, namespaceAddress, audience, $"{nameof(InitializeAsync)}");
            return amqpAuthenticationRefresher;
        }

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
                _cancellationTokenSource.Cancel();
            }

            if (Logging.IsEnabled) Logging.Exit(this, disposing, $"{nameof(Dispose)}");
        }
    }

    internal class NopRefresher : IAmqpAuthenticationRefresher
    {
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
        }
    }
}
