using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("ChatApp.TcpGateway")]
[assembly: InternalsVisibleTo("ChatApp.TcpGateway.Tests")]
// BIN-INTEGRATION-3 收益短测 harness：in-proc 组装真网关需要 internal 的 InMemoryPushTokenStore。
[assembly: InternalsVisibleTo("ChatApp.BinaryPayloadShortTest")]
