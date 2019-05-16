namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    internal interface IResourceStatusListener<T> where T : IResource
    {
        void OnResourceStatusChange(T reporter, ResourceStatus resourceStatus);
    }
}
