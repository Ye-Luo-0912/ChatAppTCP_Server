// BIN-INTEGRATION-3 短测 harness 的全局协议别名：
// 与 tests/ChatApp.TcpGateway.Tests/SharedTcpProtocolAliases.cs 保持同一绑定方向，
// 消除共享包（ChatApp.Shared.Protocol.Tcp）与网关本地契约的同名二义。
global using ClientHello = ChatApp.Shared.Protocol.Tcp.ClientHello;
global using GatewayFeature = ChatApp.Shared.Protocol.Tcp.GatewayFeature;
global using GoAway = ChatApp.Shared.Protocol.Tcp.GoAway;
global using PacketCommand = ChatApp.Shared.Protocol.Tcp.PacketCommand;
global using ProtocolErrorCode = ChatApp.Shared.Protocol.Tcp.ProtocolErrorCode;
global using ProtocolErrorFrame = ChatApp.Shared.Protocol.Tcp.ProtocolErrorFrame;
global using ProtocolPayloadFormat = ChatApp.Shared.Protocol.Tcp.ProtocolPayloadFormat;
global using ResumeFailureKind = ChatApp.Shared.Protocol.Tcp.ResumeFailureKind;
global using ResumeResponse = ChatApp.Shared.Protocol.Tcp.ResumeResponse;
global using ServerHello = ChatApp.Shared.Protocol.Tcp.ServerHello;
global using MessageHistoryCursor = ChatApp.Shared.Protocol.Tcp.MessageHistoryCursor;
global using MessageHistoryItem = ChatApp.Shared.Protocol.Tcp.MessageHistoryItem;
global using MessageHistoryRequest = ChatApp.Shared.Protocol.Tcp.MessageHistoryRequest;
global using MessageHistoryResponse = ChatApp.Shared.Protocol.Tcp.MessageHistoryResponse;
global using ConversationHistoryCatchUp = ChatApp.Shared.Protocol.Tcp.ConversationHistoryCatchUp;
global using ConversationSyncWatermark = ChatApp.Shared.Protocol.Tcp.ConversationSyncWatermark;
global using SyncBootstrapRequest = ChatApp.Shared.Protocol.Tcp.SyncBootstrapRequest;
global using SyncBootstrapResponse = ChatApp.Shared.Protocol.Tcp.SyncBootstrapResponse;
global using SyncCursorResetRequired = ChatApp.Shared.Protocol.Tcp.SyncCursorResetRequired;
global using TcpRelationshipListRequest = ChatApp.Shared.Protocol.Tcp.TcpRelationshipListRequest;
global using TcpRelationshipListResponse = ChatApp.Shared.Protocol.Tcp.TcpRelationshipListResponse;
global using TcpCallCommandRequest = ChatApp.Shared.Protocol.Tcp.TcpCallCommandRequest;
global using TcpCallCommandResponse = ChatApp.Shared.Protocol.Tcp.TcpCallCommandResponse;
global using TcpCallSignal = ChatApp.Shared.Protocol.Tcp.TcpCallSignal;
global using TcpCallCommandType = ChatApp.Shared.Protocol.Tcp.TcpCallCommandType;
global using TcpCallErrorCode = ChatApp.Shared.Protocol.Tcp.TcpCallErrorCode;

// 网关本地业务契约（JSON 契约类型与命令处理器签名一致）。
global using AuthenticationRequest = ChatApp.TcpGateway.Core.Messaging.AuthenticationRequest;
global using AuthenticationResponse = ChatApp.TcpGateway.Core.Messaging.AuthenticationResponse;
global using AttachmentFinalizeRequest = ChatApp.TcpGateway.Core.Messaging.Attachments.AttachmentFinalizeRequest;
global using AttachmentFinalizeResponse = ChatApp.TcpGateway.Core.Messaging.Attachments.AttachmentFinalizeResponse;
global using AttachmentDownloadAuthorizeRequest = ChatApp.TcpGateway.Core.Messaging.Attachments.AttachmentDownloadAuthorizeRequest;
global using AttachmentDownloadAuthorizeResponse = ChatApp.TcpGateway.Core.Messaging.Attachments.AttachmentDownloadAuthorizeResponse;

// Realtime 契约别名（与 tests/SharedRealtimeContractAliases.cs 同源）。
global using ChatApp.Realtime.Abstractions.Calls;
global using RelationshipOperation =
    ChatApp.Realtime.Abstractions.Relationships.RelationshipOperation;
global using RelationshipListType =
    ChatApp.Realtime.Abstractions.Relationships.RelationshipListType;
