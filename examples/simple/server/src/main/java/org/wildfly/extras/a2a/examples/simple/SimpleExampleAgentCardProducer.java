package org.wildfly.extras.a2a.examples.simple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.a2a.server.PublicAgentCard;
import io.a2a.spec.AgentCapabilities;
import io.a2a.spec.AgentCard;
import io.a2a.spec.AgentInterface;
import io.a2a.spec.AgentSkill;
import io.a2a.spec.TransportProtocol;

@ApplicationScoped
public class SimpleExampleAgentCardProducer {

    @Produces
    @PublicAgentCard
    public AgentCard createAgentCard() {
        String jsonRpcUrl = "http://localhost:8080";
        List<AgentInterface> interfaces = new ArrayList<>();
        // At the moment we always add the JSONRPC transport. It is needed to get the AgentCard.
        // This may change in the future
        interfaces.add(
                new AgentInterface(
                        TransportProtocol.JSONRPC.asString(), jsonRpcUrl));
        if (isRest()) {
            interfaces.add(
                    new AgentInterface(
                            TransportProtocol.HTTP_JSON.asString(), jsonRpcUrl));
        }
        if (isGrpcEnabled()) {
            interfaces.add(
                    new AgentInterface(
                            TransportProtocol.GRPC.asString(), "localhost:9555"));
        }

        return AgentCard.builder()
                .name("Hello World Agent")
                .description("Just a hello world agent")
                .version("1.0.0")
                .documentationUrl("http://example.com/docs")
                .capabilities(AgentCapabilities.builder().build())
                .defaultInputModes(Collections.singletonList("text"))
                .defaultOutputModes(Collections.singletonList("text"))
                .skills(Collections.singletonList(AgentSkill.builder()
                        .id("hello_world")
                        .name("Returns hello world")
                        .description("just returns hello world")
                        .tags(Collections.singletonList("hello world"))
                        .examples(List.of("hi", "hello world"))
                        .build()))
                .supportedInterfaces(interfaces)
                .build();
    }

    private boolean isGrpcEnabled() {
        try {
            Class.forName("org.wildfly.extras.a2a.server.apps.grpc.GrpcBeanInitializer");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    private boolean isRest() {
        try {
            Class.forName("org.wildfly.extras.a2a.server.apps.rest.WildFlyRestTransportMetadata");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
