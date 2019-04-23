using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Amqp
{
    internal interface IAmqpObject : IDisposable
    {
        Task OpenAsync(DeviceIdentity deviceIdentity, TimeSpan timeout);
        Task CloseAsync(TimeSpan timeout);
        void Close();
        void AttachStatusListener(IStatusListener statusListener);
        void DetachStatusListener(IStatusListener statusListener);
    }
    internal interface IStatusListener
    {
        void OnStatusChange(ControlStatus controlStatus, Connectivity connectivity, object reporter);
    }

    internal enum ControlStatus
    {
        Active, Inactive, Disposed
    }

    internal enum Connectivity
    {
        Connected, Disconnected
    }

}
