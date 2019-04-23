using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    internal interface IStatusReporter
    {
        void AttachResourceStatusListener(IResourceStatusListener resourceStatusListener);
        void DetachResourceStatusListener(IResourceStatusListener resourceStatusListener);
    }
}
