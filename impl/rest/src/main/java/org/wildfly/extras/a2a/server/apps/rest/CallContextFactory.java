package org.wildfly.extras.a2a.server.apps.rest;


import jakarta.servlet.http.HttpServletRequest;

import org.a2aproject.sdk.server.ServerCallContext;

public interface CallContextFactory {
    ServerCallContext build(HttpServletRequest request);
}
