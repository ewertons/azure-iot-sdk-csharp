using System;
using System.Net;
using System.Net.Security;
using System.Net.WebSockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.Amqp.Sasl;
using Microsoft.Azure.Amqp.Transport;
using Microsoft.Azure.Devices.Shared;

#if !NETSTANDARD1_3
using System.Configuration;
#endif

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpConnector : IAmqpConnector
    {
        #region Members-Constructor
        private const string DisableServerCertificateValidationKeyName = "Microsoft.Azure.Devices.DisableServerCertificateValidation";
        private static readonly AmqpVersion amqpVersion_1_0_0 = new AmqpVersion(1, 0, 0);
        private static readonly bool DisableServerCertificateValidation = InitializeDisableServerCertificateValidation();
        #endregion

        #region Open-Close
        public async Task<AmqpConnection> OpenConnectionAsync(AmqpTransportSettings amqpTransportSettings, string hostName, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(OpenConnectionAsync)}");
            TransportBase transportBase = null;

            AmqpTransportProvider amqpTransportProvider = new AmqpTransportProvider();
            amqpTransportProvider.Versions.Add(amqpVersion_1_0_0);

            AmqpSettings amqpSettings = new AmqpSettings();
            amqpSettings.TransportProviders.Add(amqpTransportProvider);

            try
            {
                TransportType transportType = amqpTransportSettings.GetTransportType();
                if (transportType == TransportType.Amqp_WebSocket_Only)
                {
                    transportBase = await InitializeWebsocketAsync(hostName, amqpTransportSettings.Proxy, amqpTransportSettings.ClientCertificate, amqpTransportProvider, timeout).ConfigureAwait(false);
                }
                else if (transportType == TransportType.Amqp_Tcp_Only)
                {

                    TcpTransportSettings tcpTransportSettings = new TcpTransportSettings()
                    {
                        Host = hostName,
                        Port = AmqpConstants.DefaultSecurePort
                    };

                    TlsTransportSettings tlsTransportSettings = new TlsTransportSettings(tcpTransportSettings)
                    {
                        TargetHost = hostName,
                        Certificate = null,
                        CertificateValidationCallback = amqpTransportSettings.RemoteCertificateValidationCallback ?? OnRemoteCertificateValidation
                    };

                    if (amqpTransportSettings.ClientCertificate != null)
                    {
                        tlsTransportSettings.Certificate = amqpTransportSettings.ClientCertificate;
                    }

                    transportBase = await InitializeSocketAsync(amqpSettings, tlsTransportSettings, timeout).ConfigureAwait(true);
                }
                else
                {
                    throw new InvalidOperationException("AmqpTransportSettings must specify WebSocketOnly or TcpOnly");
                }

                AmqpConnectionSettings amqpConnectionSettings = new AmqpConnectionSettings()
                {
                    MaxFrameSize = AmqpConstants.DefaultMaxFrameSize,
                    ContainerId = CommonResources.GetNewStringGuid(),
                    HostName = hostName
                };

                AmqpConnection amqpConnection = new AmqpConnection(transportBase, amqpSettings, amqpConnectionSettings);
                await amqpConnection.OpenAsync(timeout).ConfigureAwait(false);
                if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(OpenConnectionAsync)}");

                return amqpConnection;
            }
            catch (Exception)
            {
                transportBase?.Close();
                throw;
            }
        }

        private static async Task<TransportBase> InitializeWebsocketAsync(string hostName, IWebProxy webProxy, X509Certificate2 clientCertificate, AmqpTransportProvider amqpTransportProvider, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(typeof(AmqpConnector), timeout, $"{nameof(InitializeWebsocketAsync)}");
            TransportBase transportBase = await CreateClientWebSocketTransportAsync(hostName, webProxy, clientCertificate, timeout).ConfigureAwait(false);
            ProtocolHeader sentProtocolHeader = new ProtocolHeader(amqpTransportProvider.ProtocolId, amqpTransportProvider.DefaultVersion);
            ByteBuffer buffer = new ByteBuffer(new byte[AmqpConstants.ProtocolHeaderSize]);
            sentProtocolHeader.Encode(buffer);
            TaskCompletionSource<TransportBase> taskCompletionSource = new TaskCompletionSource<TransportBase>();
            TransportAsyncCallbackArgs transportAsyncCallbackArgs = new TransportAsyncCallbackArgs();
            transportAsyncCallbackArgs.SetBuffer(buffer.Buffer, buffer.Offset, buffer.Length);
            
            transportAsyncCallbackArgs.CompletedCallback = args =>
            {
                if (args.Exception != null)
                {
                    CompleteOnException(taskCompletionSource, args);
                    return;
                }

                byte[] headerBuffer = new byte[AmqpConstants.ProtocolHeaderSize];
                args.SetBuffer(headerBuffer, 0, headerBuffer.Length);
                
                Action<TransportAsyncCallbackArgs> onReadHeaderComplete = callbackArgs =>
                {
                    if (Logging.IsEnabled) Logging.Enter(typeof(AmqpConnector), "OnReadHeaderComplete");

                    if (args.Exception != null)
                    {
                        CompleteOnException(taskCompletionSource, args);
                        return;
                    }

                    try
                    {
                        ProtocolHeader receivedHeader = new ProtocolHeader();
                        receivedHeader.Decode(new ByteBuffer(args.Buffer, args.Offset, args.Count));

                        if (Logging.IsEnabled) Logging.Info(typeof(AmqpConnector), $"OnReadHeaderComplete: Received Protocol Header: {receivedHeader.ToString()}");

                        if (!receivedHeader.Equals(sentProtocolHeader))
                        {
                            throw new AmqpException(AmqpErrorCode.NotImplemented, $"The requested protocol version {sentProtocolHeader} is not supported. The supported version is {receivedHeader}");
                        }

                        var transport = amqpTransportProvider.CreateTransport(args.Transport, true);
                        if (Logging.IsEnabled) Logging.Info(typeof(AmqpConnector), "OnReadHeaderComplete: Created SaslTransportHandler ");
                        taskCompletionSource.TrySetResult(transport);
                        if (Logging.IsEnabled) Logging.Exit(typeof(AmqpConnector), "OnReadHeaderComplete");
                    }
                    catch (Exception ex)
                    {
                        args.Exception = ex;
                        CompleteOnException(taskCompletionSource, args);
                    }
                };

                bool operationPending = args.Transport.ReadAsync(args);

                if (!operationPending)
                {
                    args.CompletedCallback(args);
                }
            };

            transportBase = await taskCompletionSource.Task.ConfigureAwait(false);
            await transportBase.OpenAsync(timeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(typeof(AmqpConnector), timeout, $"{nameof(InitializeWebsocketAsync)}");
            return transportBase;
        }

        private async Task<TransportBase> InitializeSocketAsync(AmqpSettings amqpSettings, TlsTransportSettings tlsTransportSettings, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(InitializeSocketAsync)}");
            AmqpTransportInitiator amqpTransportInitiator = new AmqpTransportInitiator(amqpSettings, tlsTransportSettings);
            TransportBase transport = await amqpTransportInitiator.ConnectTaskAsync(timeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(InitializeSocketAsync)}");
            return transport;
        }

        protected static async Task<TransportBase> CreateClientWebSocketTransportAsync(string host, IWebProxy webProxy, X509Certificate2 clientCertificate, TimeSpan timeout)
        {
            try
            {
                if (Logging.IsEnabled) Logging.Enter(typeof(AmqpConnector), timeout, $"{nameof(CreateClientWebSocketTransportAsync)}");

                string url = WebSocketConstants.Scheme + host + ":" + WebSocketConstants.SecurePort + WebSocketConstants.UriSuffix;
#if NETSTANDARD1_3
                // NETSTANDARD1_3 implementation doesn't set client certs, so we want to tell the IoT Hub to not ask for them
                url += "?iothub-no-client-cert=true";
#endif
                Uri websocketUri = new Uri(url);
                // Use Legacy WebSocket if it is running on Windows 7 or older. Windows 7/Windows 2008 R2 is version 6.1
#if NET451
                if (Environment.OSVersion.Version.Major < 6 || (Environment.OSVersion.Version.Major == 6 && Environment.OSVersion.Version.Minor <= 1))
                {
                    var websocket = new IotHubClientWebSocket(WebSocketConstants.SubProtocols.Amqpwsb10);
                    await websocket.ConnectAsync(websocketUri.Host, websocketUri.Port, WebSocketConstants.Scheme, clientCertificate, timeout).ConfigureAwait(false);
                    return new LegacyClientWebSocketTransport(
                        websocket,
                        timeout,
                        null,
                        null);
                }
                else
                {
#endif
                var websocket = await CreateClientWebSocketAsync(websocketUri, webProxy, clientCertificate, timeout).ConfigureAwait(false);
                return new ClientWebSocketTransport(
                    websocket,
                    null,
                    null);
#if NET451
                }
#endif
            }
            finally
            {
                if (Logging.IsEnabled) Logging.Exit(typeof(AmqpConnector), timeout, $"{nameof(CreateClientWebSocketTransportAsync)}");
            }
        }

        private static async Task<ClientWebSocket> CreateClientWebSocketAsync(Uri websocketUri, IWebProxy webProxy, X509Certificate2 clientCertificate, TimeSpan timeout)
        {
            try
            {
                if (Logging.IsEnabled) Logging.Enter(typeof(AmqpConnector), timeout, $"{nameof(CreateClientWebSocketAsync)}");

                var websocket = new ClientWebSocket();

                // Set SubProtocol to AMQPWSB10
                websocket.Options.AddSubProtocol(WebSocketConstants.SubProtocols.Amqpwsb10);

                // Check if we're configured to use a proxy server
                try
                {
                    if (webProxy != DefaultWebProxySettings.Instance)
                    {
                        // Configure proxy server
                        websocket.Options.Proxy = webProxy;
                        if (Logging.IsEnabled)
                        {
                            Logging.Info(typeof(AmqpConnector), $"{nameof(CreateClientWebSocketAsync)} Setting ClientWebSocket.Options.Proxy");
                        }
                    }
                }
                catch (PlatformNotSupportedException)
                {
                    // .NET Core 2.0 doesn't support proxy. Ignore this setting.
                    if (Logging.IsEnabled)
                    {
                        Logging.Error(typeof(AmqpConnector), $"{nameof(CreateClientWebSocketAsync)} PlatformNotSupportedException thrown as .NET Core 2.0 doesn't support proxy");
                    }
                }

                if (clientCertificate != null)
                {
                    websocket.Options.ClientCertificates.Add(clientCertificate);
                }

                using (var cancellationTokenSource = new CancellationTokenSource(timeout))
                {
                    await websocket.ConnectAsync(websocketUri, cancellationTokenSource.Token).ConfigureAwait(false);
                }

                return websocket;
            }
            finally
            {
                if (Logging.IsEnabled) Logging.Exit(typeof(AmqpConnector), timeout, $"{nameof(CreateClientWebSocketAsync)}");
            }
        }


        private static void CompleteOnException(TaskCompletionSource<TransportBase> taskCompletionSource, TransportAsyncCallbackArgs args)
        {
            if (Logging.IsEnabled) Logging.Enter(typeof(AmqpConnector), $"{nameof(CompleteOnException)}");

            if (args.Exception != null && args.Transport != null)
            {
                if (Logging.IsEnabled) Logging.Error(typeof(AmqpConnector), $"{nameof(CompleteOnException)}: Exception thrown {args.Exception.Message}");

                args.Transport.SafeClose(args.Exception);
                args.Transport = null;
                taskCompletionSource.TrySetException(args.Exception);
            }
            if (Logging.IsEnabled) Logging.Exit(typeof(AmqpConnector), $"{nameof(CompleteOnException)}");
        }
        #endregion

        #region Authentication
        protected static bool InitializeDisableServerCertificateValidation()
        {
#if NETSTANDARD1_3 // No System.Configuration.ConfigurationManager in NetStandard1.3
            bool flag;
            if (!AppContext.TryGetSwitch("DisableServerCertificateValidationKeyName", out flag))
            {
                return false;
            }
            return flag;
#else
            string value = ConfigurationManager.AppSettings[DisableServerCertificateValidationKeyName];
            if (!string.IsNullOrEmpty(value))
            {
                return bool.Parse(value);
            }
            return false;
#endif
        }
        protected static bool OnRemoteCertificateValidation(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
            {
                return true;
            }

            if (DisableServerCertificateValidation && sslPolicyErrors == SslPolicyErrors.RemoteCertificateNameMismatch)
            {
                return true;
            }

            return false;
        }
        #endregion

    }
}
