using System;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    internal interface IResource : IDisposable
    {
        void Abort();
        bool IsValid();
    }
}
