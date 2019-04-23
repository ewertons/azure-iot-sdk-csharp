using Microsoft.Azure.Devices.Shared;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    
    internal class ResourceHolder : IResourceHolder
    {
        #region Members-Constructor
        // Any status update should use status lock
        private readonly object _statusLock;
        // Any resource change should use resource lock
        private readonly SemaphoreSlim _resourceLock;
        // Get a local copy with status lock, do Not call status listener in any lock to avoid deadlock, 
        private readonly HashSet<IResourceStatusListener> _resourceStatusListeners;
        private readonly IResourceAllocator _resourceAllocator;

        private IResource _resource; 
        private OperationStatus _operationStatus;

        internal ResourceHolder(IResourceAllocator resourceAllocator)
        {
            _statusLock = new object();
            _resourceLock = new SemaphoreSlim(1, 1);
            _resourceStatusListeners = new HashSet<IResourceStatusListener>();
            _operationStatus = OperationStatus.Active;

            _resourceAllocator = resourceAllocator;
        }
        #region

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

            _resourceAllocator.OnResourceOperationStatusChange(this, _operationStatus);

            _resource?.Dispose();

            lock (_statusLock)
            {
                _resourceStatusListeners.Clear();
            }

            if (Logging.IsEnabled) Logging.Exit(this, disposing, $"{nameof(Dispose)}");
        }
        #endregion

        #region IStatusReporter
        public void AttachResourceStatusListener(IResourceStatusListener resourceStatusListener)
        {
            if (Logging.IsEnabled) Logging.Enter(this, resourceStatusListener, $"{nameof(AttachResourceStatusListener)}");

            ResourceStatus resourceStatus = ResourceStatus.Disconnected;
            lock (_statusLock)
            {
                ThrowExceptionIfDisposed();
                _resourceStatusListeners.Add(resourceStatusListener);

                if (_resource?.IsValid() ?? false)
                {
                    resourceStatus = ResourceStatus.Connected;
                }
                if (Logging.IsEnabled) Logging.Associate(this, resourceStatusListener, $"{nameof(AttachResourceStatusListener)}");
            }

            resourceStatusListener.OnResourceStatusChange(this, resourceStatus);
            if (Logging.IsEnabled) Logging.Exit(this, resourceStatusListener, $"{nameof(AttachResourceStatusListener)}");
        }

        public void DetachResourceStatusListener(IResourceStatusListener resourceStatusListener)
        {
            if (Logging.IsEnabled) Logging.Enter(this, resourceStatusListener, $"{nameof(DetachResourceStatusListener)}");

            lock (_statusLock)
            {
                ThrowExceptionIfDisposed();
                _resourceStatusListeners.Remove(resourceStatusListener);
                if (Logging.IsEnabled) Logging.Associate(this, resourceStatusListener, $"{nameof(DetachResourceStatusListener)}");
            }

            if (Logging.IsEnabled) Logging.Exit(this, resourceStatusListener, $"{nameof(DetachResourceStatusListener)}");

        }
        #endregion

        public async Task<IResource> AllocateOrRetrieveResourceAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(AllocateOrRetrieveResourceAsync)}");

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
                throw new TimeoutException($"{this} {nameof(AllocateOrRetrieveResourceAsync)}({timeout}) timeout.");
            }

            try
            {
                if (_resource?.IsValid() ?? false)
                {
                    _resource = await _resourceAllocator.AllocateResourceAsync(timeout).ConfigureAwait(false);
                    
                    if (Logging.IsEnabled) Logging.Associate(this, _resource, $"{nameof(AllocateOrRetrieveResourceAsync)}");
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

                _resource.AttachResourceStatusListener(this);
            }

            ThrowExceptionIfDisposed();
            ThrowExceptionIfInactived();
            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(AllocateOrRetrieveResourceAsync)}");
            return _resource;
        }

        #region IResource
        public async Task OpenAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(OpenAsync)}");

            lock (_statusLock)
            {
                ThrowExceptionIfDisposed();
                _operationStatus = OperationStatus.Active;
            }

            _resourceAllocator.OnResourceOperationStatusChange(this, _operationStatus);

            await AllocateOrRetrieveResourceAsync(timeout).ConfigureAwait(false);

            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(OpenAsync)}");
        }

        public async Task CloseAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(CloseAsync)}");
            lock (_statusLock)
            {
                ThrowExceptionIfDisposed();
                _operationStatus = OperationStatus.Inactive;
            }

            _resourceAllocator.OnResourceOperationStatusChange(this, _operationStatus);

            bool gain = await _resourceLock.WaitAsync(timeout).ConfigureAwait(false);
            if (!gain)
            {
                throw new TimeoutException($"{this} {nameof(CloseAsync)}({timeout}) timeout.");
            }

            try
            {
                if (!(_resource?.IsValid() ?? false))
                {
                    await _resource.CloseAsync(timeout).ConfigureAwait(false);
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
            if (Logging.IsEnabled) Logging.Enter(this, _resource, $"{nameof(Abort)}");

            lock (_statusLock)
            {
                ThrowExceptionIfDisposed();
                _operationStatus = OperationStatus.Inactive;
            }

            _resourceAllocator.OnResourceOperationStatusChange(this, _operationStatus);

            _resourceLock.Wait();
            try
            {
                _resource.Abort();
                if (Logging.IsEnabled) Logging.Associate(this, _resource, $"{nameof(Abort)}");
            }
            finally
            {
                _resourceLock.Release();
            }

            if (Logging.IsEnabled) Logging.Exit(this, _resource, $"{nameof(Abort)}");
        }

        public bool IsValid()
        {
            lock (_statusLock)
            {
                return _operationStatus == OperationStatus.Active && (_resource?.IsValid() ?? false);
            }
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

        public void OnResourceStatusChange(object reporter, ResourceStatus resourceStatus)
        {
            if (Logging.IsEnabled) Logging.Enter(this, reporter, resourceStatus, $"{nameof(OnResourceStatusChange)}");

            List<IResourceStatusListener> statusListeners;
            lock (_statusLock)
            {
                statusListeners = new List<IResourceStatusListener>(_resourceStatusListeners);
            }

            if (Logging.IsEnabled) Logging.Info(this, $"{this} {_resource} status changed to {resourceStatus}.", $"{nameof(ThrowExceptionIfDisposed)}");

            foreach (IResourceStatusListener statusListener in statusListeners)
            {
                statusListener.OnResourceStatusChange(this, resourceStatus);
            }

            if (Logging.IsEnabled) Logging.Exit(this, reporter, resourceStatus, $"{nameof(OnResourceStatusChange)}");
        }
        #endregion
    }
}
