package org.wildfly.extras.a2a.server.apps.rest;

import io.a2a.server.TransportMetadata;
import io.a2a.spec.TransportProtocol;

public class WildFlyRestTransportMetadata implements TransportMetadata {
    @Override
    public String getTransportProtocol() {
        return TransportProtocol.HTTP_JSON.asString();
    }
}
