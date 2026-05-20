package org.wildfly.extras.a2a.server.apps.jsonrpc.multiversion;

import org.a2aproject.sdk.server.TransportMetadata;
import org.a2aproject.sdk.spec.TransportProtocol;

public class WildFlyJSONRPCTransportMetadata implements TransportMetadata {
    @Override
    public String getTransportProtocol() {
        return TransportProtocol.JSONRPC.asString();
    }
}
