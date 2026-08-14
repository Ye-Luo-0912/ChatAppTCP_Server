namespace ChatApp.TcpGateway.Core.Protocol;

/// <summary>
/// 网关能力集合。集中区分已经实现的能力与仅保留 wire 编号的未来能力。
/// </summary>
public static class GatewayFeatureSet
{
    /// <summary>当前服务端已实现、可在 ServerHello 中协商的能力。</summary>
    public const GatewayFeature Implemented =
        GatewayFeature.CommandCapabilities |
        GatewayFeature.SessionResume |
        GatewayFeature.ConversationSync |
        GatewayFeature.ConversationPreferences |
        GatewayFeature.MessageMutation |
        GatewayFeature.PresenceAndTyping |
        GatewayFeature.MessageReactions |
        GatewayFeature.GroupManagement |
        GatewayFeature.PushTokenManagement |
        GatewayFeature.CallSignaling |
        GatewayFeature.RelationshipRead;

    /// <summary>协议已经分配的全部能力位，包括尚未实现的预留位。</summary>
    public const GatewayFeature Known =
        GatewayFeature.BinaryPayload |
        GatewayFeature.Compression |
        GatewayFeature.StreamingChat |
        Implemented;

    /// <summary>判断位掩码是否包含指定能力集合。</summary>
    public static bool ContainsAll(uint featureBits, GatewayFeature required)
    {
        var requiredBits = (uint)required;
        return (featureBits & requiredBits) == requiredBits;
    }
}
