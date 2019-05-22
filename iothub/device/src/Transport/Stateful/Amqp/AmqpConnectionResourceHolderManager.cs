using Microsoft.Azure.Devices.Shared;
using System;
using System.Collections.Generic;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpConnectionResourceHolderManager : IAmqpConnectionResourceHolderManager
    {
        private static readonly IAmqpConnectionResourceHolderManager s_instance = new AmqpConnectionResourceHolderManager();
        private readonly IDictionary<string, AmqpConnectionResourceHolderPool> _amqpConnectionResourceHolderPools;
        private readonly object _lock;

        private AmqpConnectionResourceHolderManager()
        {
            _amqpConnectionResourceHolderPools = new Dictionary<string, AmqpConnectionResourceHolderPool>();
            _lock = new object();
        }

       internal static IAmqpConnectionResourceHolderManager GetInstance()
        {
            return s_instance;
        }

        public IAmqpConnectionResourceHolder AllocateAmqpConnectionResourceHolder(DeviceIdentity deviceIdentity)
        {
            if (Logging.IsEnabled) Logging.Enter(this, deviceIdentity, $"{nameof(AllocateAmqpConnectionResourceHolder)}");
            IAmqpConnectionResourceHolder amqpConnectionResourceHolder;
            bool pooling = deviceIdentity.AuthenticationModel != AuthenticationModel.X509 && (deviceIdentity?.AmqpTransportSettings?.AmqpConnectionPoolSettings?.Pooling ?? false);
            if (pooling)
            {
                AmqpConnectionResourceHolderPool amqpConnectionResourceHolderPool = ResolveAmqpConnectionResourceHolderPool(deviceIdentity.IotHubConnectionString.HostName);
                if (Logging.IsEnabled) Logging.Associate(deviceIdentity, amqpConnectionResourceHolderPool, $"{nameof(AllocateAmqpConnectionResourceHolder)}");
                amqpConnectionResourceHolder = amqpConnectionResourceHolderPool.ResolveAmqpConnectionResourceHolder(deviceIdentity);
            }
            else
            {
                amqpConnectionResourceHolder = new AmqpConnectionResourceHolder(OnAmqpConnectionDisconnected);
            }

            if (Logging.IsEnabled) Logging.Associate(deviceIdentity, amqpConnectionResourceHolder, $"{nameof(AllocateAmqpConnectionResourceHolder)}");
            if (Logging.IsEnabled) Logging.Exit(this, deviceIdentity, $"{nameof(AllocateAmqpConnectionResourceHolder)}");
            return amqpConnectionResourceHolder;
        }

        private void OnAmqpConnectionDisconnected()
        {
            if (Logging.IsEnabled) Logging.Info(this, "An unexpected AMQP connection disconnection occurs.", $"{nameof(OnAmqpConnectionDisconnected)}");
            if (DeviceEventCounter.IsEnabled) DeviceEventCounter.OnAmqpConnectionDisconnected();
        }

        private AmqpConnectionResourceHolderPool ResolveAmqpConnectionResourceHolderPool(string host)
        {
            lock (_lock)
            {
                _amqpConnectionResourceHolderPools.TryGetValue(host, out AmqpConnectionResourceHolderPool amqpConnectionResourceHolderPool);
                if (amqpConnectionResourceHolderPool == null)
                {
                    amqpConnectionResourceHolderPool = new AmqpConnectionResourceHolderPool();
                    _amqpConnectionResourceHolderPools.Add(host, amqpConnectionResourceHolderPool);
                }

                if (Logging.IsEnabled) Logging.Associate(this, amqpConnectionResourceHolderPool, $"{nameof(ResolveAmqpConnectionResourceHolderPool)}");
                return amqpConnectionResourceHolderPool;
            }
        }

        private class AmqpConnectionResourceHolderPool
        {
            private IAmqpConnectionResourceHolder[] _amqpSasIndividualPool;
            private readonly IDictionary<string, IAmqpConnectionResourceHolder[]> _amqpSasGroupedPool;
            private readonly object _lock;

            internal AmqpConnectionResourceHolderPool()
            {
                _amqpSasGroupedPool = new Dictionary<string, IAmqpConnectionResourceHolder[]>();
                _lock = new object();
            }

            public IAmqpConnectionResourceHolder ResolveAmqpConnectionResourceHolder(DeviceIdentity deviceIdentity)
            {
                if (Logging.IsEnabled) Logging.Enter(this, deviceIdentity, $"{nameof(ResolveAmqpConnectionResourceHolder)}");

                lock(_lock)
                {
                    IAmqpConnectionResourceHolder[] pool  = ResolveAmqpConnectionResourceHolderPool(deviceIdentity);
                    int poolSize = pool.Length;
                    int index = Math.Abs(deviceIdentity.GetHashCode()) % poolSize;
                    if (pool[index] == null)
                    {
                        pool[index] = new AmqpConnectionResourceHolder(OnConnectionDisconnected);
                    }

                    if (Logging.IsEnabled) Logging.Exit(this, deviceIdentity, $"{nameof(ResolveAmqpConnectionResourceHolder)}");
                    return pool[index];
                }
            }

            private IAmqpConnectionResourceHolder[] ResolveAmqpConnectionResourceHolderPool(DeviceIdentity deviceIdentity)
            {
                int poolSize = (int) deviceIdentity.AmqpTransportSettings.AmqpConnectionPoolSettings.MaxPoolSize;
                
                if (deviceIdentity.AuthenticationModel == AuthenticationModel.SasIndividual)
                {
                    if (_amqpSasIndividualPool == null)
                    {
                        _amqpSasIndividualPool = new IAmqpConnectionResourceHolder[poolSize];
                    }

                    return _amqpSasIndividualPool;
                }
                else
                {
                    string scope = deviceIdentity.IotHubConnectionString.SharedAccessKeyName;
                    _amqpSasGroupedPool.TryGetValue(scope, out IAmqpConnectionResourceHolder[] amqpSasGroup);
                    if (amqpSasGroup == null)
                    {
                        amqpSasGroup = new IAmqpConnectionResourceHolder[poolSize];
                        _amqpSasGroupedPool.Add(scope, amqpSasGroup);
                    }

                    return amqpSasGroup;
                }
            }

            private void OnConnectionDisconnected()
            {
                if (Logging.IsEnabled) Logging.Info(this, "An unexpected AMQP connection disconnection occurs.", $"{nameof(OnConnectionDisconnected)}");
            }
        }
    }

}
