using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    internal interface IResource : IStatusReporter
    {
        Task OpenAsync(TimeSpan timeout);
        Task CloseAsync(TimeSpan timeout);
        void Abort();
        void Dispose();
        bool IsValid();
    }
}
