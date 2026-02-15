package org.wildfly.extras.a2a.server.apps.grpc;

import java.util.concurrent.Executor;

import io.a2a.server.requesthandlers.RequestHandler;
import io.a2a.spec.AgentCard;
import io.a2a.transport.grpc.handler.CallContextFactory;
import io.a2a.transport.grpc.handler.GrpcHandler;

/**
 * WildFly gRPC Handler that uses static cache for CDI beans.
 *
 * The WildFly gRPC subsystem instantiates this class directly using
 * reflection and the default constructor, bypassing CDI completely.
 *
 * Since CDI is not available on gRPC threads, we use static cache
 * populated during application startup when CDI is available.
 */
public class WildFlyGrpcHandler extends GrpcHandler {

    // Static cache populated during application startup by GrpcBeanInitializer
    private static volatile AgentCard staticAgentCard;
    private static volatile AgentCard staticExtendedAgentCard;
    private static volatile RequestHandler staticRequestHandler;
    private static volatile CallContextFactory staticCallContextFactory;
    private static volatile Executor staticExecutor;

    public WildFlyGrpcHandler() {
        // Default constructor - the only one used by WildFly gRPC subsystem
    }

    /**
     * Called by GrpcBeanInitializer during CDI initialization to cache beans
     * for use by gRPC threads where CDI is not available.
     */
    static void setStaticBeans(AgentCard agentCard, AgentCard extendedAgentCard, RequestHandler requestHandler, CallContextFactory callContextFactory, Executor executor) {
        staticAgentCard = agentCard;
        staticExtendedAgentCard = extendedAgentCard;
        staticRequestHandler = requestHandler;
        staticCallContextFactory = callContextFactory;
        staticExecutor = executor;
    }

    @Override
    protected RequestHandler getRequestHandler() {
        if (staticRequestHandler == null) {
            throw new RuntimeException("RequestHandler not available. ApplicationStartup may not have run yet.");
        }
        return staticRequestHandler;
    }

    @Override
    protected AgentCard getAgentCard() {
        if (staticAgentCard == null) {
            throw new RuntimeException("AgentCard not available. ApplicationStartup may not have run yet.");
        }
        return staticAgentCard;
    }

    @Override
    protected AgentCard getExtendedAgentCard() {
        return staticExtendedAgentCard; // Can be null if not configured
    }

    @Override
    protected CallContextFactory getCallContextFactory() {
        return staticCallContextFactory; // Can be null if not configured
    }

    @Override
    protected Executor getExecutor() {
        if (staticExecutor == null) {
            throw new RuntimeException("Executor not available. ApplicationStartup may not have run yet.");
        }
        return staticExecutor;
    }
}
