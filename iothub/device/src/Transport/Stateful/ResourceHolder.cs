using Microsoft.Azure.Devices.Client.Exceptions;
using Microsoft.Azure.Devices.Shared;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    internal abstract class ResourceHolder<T> : IResourceHolder<T>, IResourceStatusListener<T> where T : IResource
    {
        #region Members-Constructor
        // Any status update should use status lock
        private readonly object _statusLock;
        // Any resource change should use resource lock
        private readonly SemaphoreSlim _resourceLock;
        private readonly IResourceAllocator<T> _resourceAllocator;

        protected T _resource; 
        private OperationStatus _operationStatus;

        private Action _onResourceDisconnection;

        internal ResourceHolder(IResourceAllocator<T> resourceAllocator, Action onResourceDisconnection)
        {
            _statusLock = new object();
            _resourceLock = new SemaphoreSlim(1, 1);
            _operationStatus = OperationStatus.Inactive;
            _resourceAllocator = resourceAllocator;
            _onResourceDisconnection = onResourceDisconnection;
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
            lock (_statusLock)
            {
                if (_operationStatus == OperationStatus.Disposed) return;

                if (Logging.IsEnabled) Logging.Enter(this, disposing, $"{nameof(Dispose)}");
                _operationStatus = OperationStatus.Disposed;
            }

            _resource?.Dispose();

            if (Logging.IsEnabled) Logging.Exit(this, disposing, $"{nameof(Dispose)}");
        }
        #endregion

        #region IResourceHolder
        public OperationStatus GetOperationStatus()
        {
            lock(_statusLock)
            {
                return _operationStatus;
            }
        }

        public async Task<T> EnsureResourceAsync(DeviceIdentity deviceIdentity, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, deviceIdentity, timeout, $"{nameof(EnsureResourceAsync)}");

            // check operation status
            lock (_statusLock)
            {
                ThrowExceptionIfDisposed();
                ThrowExceptionIfInactived();
            }

            // allocate resource if resource is invalid
            bool gain = await _resourceLock.WaitAsync(timeout).ConfigureAwait(false);
            if (!gain)
            {
                throw new IotHubException($"{this} {deviceIdentity} {nameof(EnsureResourceAsync)}({timeout}) failed to gain resource lock.", true);
            }

            try
            {
                if (_resource == null || !_resource.IsValid())
                {
                    _resource = await _resourceAllocator.AllocateResourceAsync(deviceIdentity, this, timeout).ConfigureAwait(false);
                    if (Logging.IsEnabled) Logging.Associate(this, _resource, $"{nameof(EnsureResourceAsync)}");
                    if (Logging.IsEnabled) Logging.Associate(deviceIdentity, _resource, $"{nameof(EnsureResourceAsync)}");
                }
            }
            finally
            {
                _resourceLock.Release();
            }

            // check operation status, clean up resource if operation status was changed, otherwise start monitor resource
            lock (_statusLock)
            {
                if (_operationStatus == OperationStatus.Disposed)
                {
                    _resource.Dispose();
                }
                else if (_operationStatus == OperationStatus.Inactive)
                {
                    _resource.Abort();
                }
            }

            ThrowExceptionIfDisposed();
            ThrowExceptionIfInactived();
            if (Logging.IsEnabled) Logging.Exit(this, deviceIdentity, timeout, $"{nameof(EnsureResourceAsync)}");
            return _resource;
        }

        public async Task<T> OpenAsync(DeviceIdentity deviceIdentity, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, deviceIdentity, timeout, $"{nameof(OpenAsync)}");

            lock (_statusLock)
            {
                ThrowExceptionIfDisposed();
                _operationStatus = OperationStatus.Active;
            }
            
            T resource = await EnsureResourceAsync(deviceIdentity, timeout).ConfigureAwait(false);

            if (Logging.IsEnabled) Logging.Associate(deviceIdentity, this, $"{nameof(OpenAsync)}");
            if (Logging.IsEnabled) Logging.Exit(this, deviceIdentity, timeout, $"{nameof(OpenAsync)}");
            return resource;
        }

        public async Task CloseAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(CloseAsync)}");
            lock (_statusLock)
            {
                ThrowExceptionIfDisposed();
                _operationStatus = OperationStatus.Inactive;
            }

            bool gain = await _resourceLock.WaitAsync(timeout).ConfigureAwait(false);
            if (!gain)
            {
                throw new TimeoutException($"{this} {nameof(CloseAsync)}({timeout}) timeout.");
            }

            try
            {
                if (_resource != null && _resource.IsValid())
                {
                    _resource.Abort();
                    if (Logging.IsEnabled) Logging.Associate(this, _resource, $"{nameof(CloseAsync)}");
                }
            }
            finally
            {
                _resourceLock.Release();
            }

            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(CloseAsync)}");
        }

        public void Abort()
        {
            lock (_statusLock)
            {
                ThrowExceptionIfDisposed();
                if (Logging.IsEnabled) Logging.Enter(this, _resource, $"{nameof(Abort)}");
                _operationStatus = OperationStatus.Inactive;
                if (_resource != null && _resource.IsValid())
                {
                    _resource.Abort();
                    if (Logging.IsEnabled) Logging.Associate(this, _resource, $"{nameof(Abort)}");
                }
                if (Logging.IsEnabled) Logging.Exit(this, _resource, $"{nameof(Abort)}");
            }
        }
        #endregion

        #region IResourceStatusListener
        public void OnResourceStatusChange(T reporter, ResourceStatus resourceStatus)
        {
            if (Logging.IsEnabled) Logging.Enter(this, reporter, resourceStatus, $"{nameof(OnResourceStatusChange)}");
            bool unexceptedDisconnection = false;
            lock (_statusLock)
            {
                if (_operationStatus == OperationStatus.Active && resourceStatus == ResourceStatus.Disconnected && ReferenceEquals(_resource, reporter))
                {
                    _resource = default;
                    unexceptedDisconnection = true;
                }
            }
            if (unexceptedDisconnection)
            {
                _onResourceDisconnection?.Invoke();
            }
            if (Logging.IsEnabled) Logging.Exit(this, reporter, resourceStatus, $"{nameof(OnResourceStatusChange)}");
        }
        #endregion

        #region private helper function
        private void ThrowExceptionIfDisposed()
        {
            if (_operationStatus == OperationStatus.Disposed)
            {
                if (Logging.IsEnabled) Logging.Info(this, $"{this} is disposed.", $"{nameof(ThrowExceptionIfDisposed)}");
                throw new ObjectDisposedException($"{this} is disposed.");
            }
        }

        private void ThrowExceptionIfInactived()
        {
            if (_operationStatus == OperationStatus.Inactive)
            {
                if (Logging.IsEnabled) Logging.Info(this, $"{this} is closed.", $"{nameof(ThrowExceptionIfInactived)}");
                throw new InvalidOperationException($"{this} is inactived.");
            }
        }
        #endregion

    }
}
