using Microsoft.Azure.Devices.Shared;
using System;
using System.Collections.Generic;

namespace Microsoft.Azure.Devices.Client.Transport.Amqp
{
    abstract class BaseAmqpObject : IAmqpObject
    {
        private readonly HashSet<IStatusListener> _statusListeners;
        protected readonly object _stateLock;

        protected ControlStatus _controlStatus;
        protected Connectivity _connectivity;

        protected abstract void CleanupResource();
        protected abstract void DisposeResource();

        protected BaseAmqpObject()
        {
            _statusListeners = new HashSet<IStatusListener>();
            _stateLock = new object();
        }

        #region IDisposable

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            lock (_stateLock)
            {
                if (_controlStatus == ControlStatus.Disposed)
                {
                    return;
                }
                if (Logging.IsEnabled) Logging.Enter(this, disposing, $"{nameof(Dispose)}");
                _controlStatus = ControlStatus.Disposed;
                _connectivity = Connectivity.Disconnected;
            }

            if (disposing)
            {
                DisposeResource();
                NotifyStatusChange();
                _statusListeners.Clear();
            }

            if (Logging.IsEnabled) Logging.Exit(this, disposing, $"{nameof(Dispose)}");
        }

        private void NotifyStatusChange()
        {
            foreach (IStatusListener statusListener in _statusListeners)
            {
                statusListener.OnStatusChange(_controlStatus, _connectivity, this);
            }
        }

        #endregion

        protected void Close()
        {
            lock (_stateLock)
            {
                if (_controlStatus == ControlStatus.Inactive || _controlStatus == ControlStatus.Disposed)
                {
                    return;
                }

                if (Logging.IsEnabled) Logging.Enter(this, ControlStatus.Inactive, $"{nameof(Close)}");
                _controlStatus = ControlStatus.Inactive;
                _connectivity = Connectivity.Disconnected;
            }

            CleanupResource();
            NotifyStatusChange();

            if (Logging.IsEnabled) Logging.Exit(this, ControlStatus.Inactive, $"{nameof(Close)}");
        }

        public void AttachStatusListener(IStatusListener statusListener)
        {
            if (Logging.IsEnabled) Logging.Enter(this, statusListener, $"{nameof(AttachStatusListener)}");

            lock (_stateLock)
            {
                ThrowExceptionIfDisposed();
                _statusListeners.Add(statusListener);
                if (Logging.IsEnabled) Logging.Associate(this, statusListener, $"{nameof(AttachStatusListener)}");
            }

            if (Logging.IsEnabled) Logging.Exit(this, statusListener, $"{nameof(AttachStatusListener)}");
        }

        public void DetachStatusListener(IStatusListener statusListener)
        {

            if (Logging.IsEnabled) Logging.Enter(this, statusListener, $"{nameof(DetachStatusListener)}");
            lock (_stateLock)
            {
                _statusListeners.Remove(statusListener);
                if (Logging.IsEnabled) Logging.Associate(this, statusListener, $"{nameof(DetachStatusListener)}");
            }

            if (Logging.IsEnabled) Logging.Exit(this, statusListener, $"{nameof(DetachStatusListener)}");

        }

        protected void ThrowExceptionIfDisposed()
        {
            lock (_stateLock)
            {
                if (_controlStatus == ControlStatus.Disposed)
                {
                    if (Logging.IsEnabled) Logging.Info(this, $"{this} is disposed.", $"{nameof(ThrowExceptionIfDisposed)}");
                    throw new ObjectDisposedException($"{this} is disposed.");
                }
            }
        }

        protected void ThrowExceptionIfClosedOrDisposed()
        {
            lock (_stateLock)
            {
                if (_controlStatus == ControlStatus.Inactive || _controlStatus == ControlStatus.Disposed)
                {
                    if (_disposed)
                    {
                        DisposeResource();
                        if (Logging.IsEnabled) Logging.Info(this, $"{this} is disposed.", $"{nameof(ThrowExceptionIfClosedOrDisposed)}");
                        throw new ObjectDisposedException($"{this} is disposed.");
                    }
                    else
                    {
                        CleanupResource();
                        if (Logging.IsEnabled) Logging.Info(this, $"{this} is closed.", $"{nameof(ThrowExceptionIfClosedOrDisposed)}");
                        throw new InvalidOperationException($"{this} is closed.");
                    }
                }
            }
        }
    }
}
