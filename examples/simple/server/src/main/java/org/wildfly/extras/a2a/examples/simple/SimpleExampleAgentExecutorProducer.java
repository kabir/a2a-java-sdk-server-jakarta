package org.wildfly.extras.a2a.examples.simple;

import java.util.Collections;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.a2a.server.agentexecution.AgentExecutor;
import io.a2a.server.agentexecution.RequestContext;
import io.a2a.server.tasks.AgentEmitter;
import io.a2a.spec.A2AError;
import io.a2a.spec.Part;
import io.a2a.spec.TaskNotCancelableError;
import io.a2a.spec.TextPart;

@ApplicationScoped
public class SimpleExampleAgentExecutorProducer {

    @Produces
    public AgentExecutor mockExecutor() {
        return new SimpleExampleAgentExecutor();
    }

    private static class SimpleExampleAgentExecutor implements AgentExecutor {
        @Override
        public void execute(RequestContext context, AgentEmitter emitter) throws A2AError {

            // Signal we've started working
            emitter.startWork();

            // Get the name sent in the user's message
            List<Part<?>> partsList = context.getMessage().parts();
            List<TextPart> textParts = partsList.stream()
                    .filter(p -> p instanceof TextPart)
                    .map(p -> (TextPart) p)
                    .toList();
            String name = textParts.get(textParts.size() - 1).text();

            // Simulate doing work with the LLM, and adding that as an artifact.
            // In this case we just add "Hello <name>" to the list of artifacts
            String response = "Hello " + name;
            emitter.addArtifact(Collections.singletonList(new TextPart(response)), null, "response", null);

            // We have completed our simple example Task
            emitter.complete();
        }

        @Override
        public void cancel(RequestContext context, AgentEmitter emitter) throws A2AError {
            throw new TaskNotCancelableError();
        }
    }
}
