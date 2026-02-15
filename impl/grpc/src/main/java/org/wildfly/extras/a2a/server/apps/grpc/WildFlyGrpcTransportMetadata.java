package org.wildfly.extras.a2a.server.apps.grpc;

import io.a2a.server.TransportMetadata;
import io.a2a.spec.TransportProtocol;

public class WildFlyGrpcTransportMetadata implements TransportMetadata {
    @Override
    public String getTransportProtocol() {
        return TransportProtocol.GRPC.asString();
    }
}
