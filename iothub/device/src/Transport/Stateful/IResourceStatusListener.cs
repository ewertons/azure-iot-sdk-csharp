namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    internal interface IResourceStatusListener
    {
        void OnResourceStatusChange(object reporter, ResourceStatus resourceStatus);
    }
}
