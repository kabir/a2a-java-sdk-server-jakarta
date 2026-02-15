package org.wildfly.extras.a2a.server.apps.jsonrpc;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

import com.google.gson.JsonSyntaxException;

import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.SecurityContext;

import io.a2a.common.A2AHeaders;
import io.a2a.grpc.utils.JSONRPCUtils;
import io.a2a.server.util.sse.SseFormatter;
import io.a2a.jsonrpc.common.json.IdJsonMappingException;
import io.a2a.jsonrpc.common.json.InvalidParamsJsonMappingException;
import io.a2a.jsonrpc.common.json.JsonMappingException;
import io.a2a.jsonrpc.common.json.JsonProcessingException;
import io.a2a.jsonrpc.common.json.MethodNotFoundJsonMappingException;
import io.a2a.jsonrpc.common.wrappers.A2AErrorResponse;
import io.a2a.jsonrpc.common.wrappers.A2ARequest;
import io.a2a.jsonrpc.common.wrappers.A2AResponse;
import io.a2a.jsonrpc.common.wrappers.CancelTaskRequest;
import io.a2a.jsonrpc.common.wrappers.CreateTaskPushNotificationConfigRequest;
import io.a2a.jsonrpc.common.wrappers.DeleteTaskPushNotificationConfigRequest;
import io.a2a.jsonrpc.common.wrappers.GetExtendedAgentCardRequest;
import io.a2a.jsonrpc.common.wrappers.GetTaskPushNotificationConfigRequest;
import io.a2a.jsonrpc.common.wrappers.GetTaskRequest;
import io.a2a.jsonrpc.common.wrappers.ListTaskPushNotificationConfigRequest;
import io.a2a.jsonrpc.common.wrappers.ListTasksRequest;
import io.a2a.jsonrpc.common.wrappers.NonStreamingJSONRPCRequest;
import io.a2a.jsonrpc.common.wrappers.SendMessageRequest;
import io.a2a.jsonrpc.common.wrappers.SendStreamingMessageRequest;
import io.a2a.jsonrpc.common.wrappers.StreamingJSONRPCRequest;
import io.a2a.jsonrpc.common.wrappers.SubscribeToTaskRequest;
import io.a2a.server.ExtendedAgentCard;
import io.a2a.server.ServerCallContext;
import io.a2a.server.auth.UnauthenticatedUser;
import io.a2a.server.auth.User;
import io.a2a.server.extensions.A2AExtensions;
import io.a2a.server.util.async.Internal;
import io.a2a.spec.A2AError;
import io.a2a.spec.AgentCard;
import io.a2a.spec.InternalError;
import io.a2a.spec.InvalidParamsError;
import io.a2a.spec.InvalidRequestError;
import io.a2a.spec.JSONParseError;
import io.a2a.spec.MethodNotFoundError;
import io.a2a.spec.UnsupportedOperationError;
import io.a2a.transport.jsonrpc.handler.JSONRPCHandler;
import io.a2a.grpc.utils.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
public class A2AServerResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(A2AServerResource.class);

    @Inject
    JSONRPCHandler jsonRpcHandler;

    @Inject
    @ExtendedAgentCard
    Instance<AgentCard> extendedAgentCard;

    // Hook so testing can wait until the async Subscription is subscribed.
    private static volatile Runnable streamingIsSubscribedRunnable;

    @Inject
    @Internal
    Executor executor;


    @Inject
    Instance<CallContextFactory> callContextFactory;

    /**
     * Handles incoming POST requests to the main A2A endpoint. Dispatches the
     * request to the appropriate JSON-RPC handler method and returns the response.
     *
     * @param body the JSON-RPC request string
     * @return the JSON-RPC response which may be an error response
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String handleNonStreamingRequests(
            String body, @Context HttpServletRequest httpRequest,
            @Context SecurityContext securityContext) {

        ServerCallContext context = createCallContext(httpRequest, securityContext);
        LOGGER.debug("Handling non-streaming request");
        A2AResponse<?> response;
        try {
            A2ARequest<?> request = JSONRPCUtils.parseRequestBody(body, null);
            response = processNonStreamingRequest((NonStreamingJSONRPCRequest<?>) request, context);
        } catch (InvalidParamsJsonMappingException e) {
            LOGGER.warn("Invalid params in request: {}", e.getMessage());
            response = new A2AErrorResponse(e.getId(), new InvalidParamsError(null, e.getMessage(), null));
        } catch (MethodNotFoundJsonMappingException e) {
            LOGGER.warn("Method not found in request: {}", e.getMessage());
            response = new A2AErrorResponse(e.getId(), new MethodNotFoundError(null, e.getMessage(), null));
        } catch (IdJsonMappingException e) {
            LOGGER.warn("Invalid request ID: {}", e.getMessage());
            response = new A2AErrorResponse(e.getId(), new InvalidRequestError(null, e.getMessage(), null));
        } catch (JsonMappingException e) {
            LOGGER.warn("JSON mapping error: {}", e.getMessage(), e);
            // General JsonMappingException - treat as InvalidRequest
            response = new A2AErrorResponse(new InvalidRequestError(null, e.getMessage(), null));
        } catch (JsonSyntaxException e) {
            LOGGER.warn("JSON syntax error: {}", e.getMessage());
            response = new A2AErrorResponse(new JSONParseError(e.getMessage()));
        } catch (JsonProcessingException e) {
            LOGGER.warn("JSON processing error: {}", e.getMessage());
            response = new A2AErrorResponse(new JSONParseError(e.getMessage()));
        } catch (Throwable t) {
            LOGGER.error("Unexpected error processing request: {}", t.getMessage(), t);
            response = new A2AErrorResponse(new InternalError(t.getMessage()));
        } finally {
            LOGGER.debug("Completed non-streaming request");
        }

        // Serialize response using protobuf conversion
        return serializeResponse(response);
    }

    /**
     * Handles incoming POST requests to the main A2A endpoint that involve Server-Sent Events (SSE).
     * Uses custom SSE response handling to avoid JAX-RS SSE compatibility issues with async publishers.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public void handleStreamingRequests(
            String body,
            @Context HttpServletResponse response,
            @Context HttpServletRequest httpRequest,
            @Context SecurityContext securityContext) throws IOException {

        ServerCallContext context = createCallContext(httpRequest, securityContext);
        LOGGER.debug("Handling streaming request with custom SSE response");

        // Set SSE headers manually for proper streaming
        response.setContentType(MediaType.SERVER_SENT_EVENTS);
        response.setCharacterEncoding("UTF-8");
        response.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");

        A2ARequest<?> request = null;
        try {
            // Parse the request body
            request = JSONRPCUtils.parseRequestBody(body, null);

            // Get the publisher synchronously to avoid connection closure issues
            Flow.Publisher<? extends A2AResponse<?>> publisher = createStreamingPublisher((StreamingJSONRPCRequest<?>) request, context);
            LOGGER.debug("Created streaming publisher: {}", publisher);

            if (publisher != null) {
                // Handle the streaming response with custom SSE formatting
                LOGGER.debug("Handling custom SSE response for publisher: {}", publisher);
                handleCustomSSEResponse(publisher, response, context);
            } else {
                // Handle unsupported request types
                LOGGER.debug("Unsupported streaming request type: {}", request.getClass().getSimpleName());
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unsupported streaming request type");
            }
        } catch (MethodNotFoundJsonMappingException e) {
            LOGGER.warn("Method not found in streaming request: {}", e.getMessage());
            sendErrorSSE(response, e.getId(), new MethodNotFoundError());
        } catch (InvalidParamsJsonMappingException e) {
            LOGGER.warn("Invalid params in streaming request: {}", e.getMessage());
            sendErrorSSE(response, e.getId(), new InvalidParamsError());
        } catch (IdJsonMappingException e) {
            LOGGER.warn("Invalid request ID in streaming request: {}", e.getMessage());
            sendErrorSSE(response, e.getId(), new InvalidRequestError());
        } catch (JsonMappingException e) {
            LOGGER.warn("JSON mapping error in streaming request: {}", e.getMessage(), e);
            // Check if this is a parse error wrapped in a mapping exception
            if (e.getCause() instanceof JsonProcessingException) {
                sendErrorSSE(response, null, new JSONParseError());
            } else {
                // Otherwise it's an invalid request (valid JSON but doesn't match schema)
                sendErrorSSE(response, null, new InvalidRequestError());
            }
        } catch (JsonSyntaxException e) {
            LOGGER.warn("JSON syntax error in streaming request: {}", e.getMessage());
            sendErrorSSE(response, null, new JSONParseError());
        } catch (JsonProcessingException e) {
            LOGGER.warn("JSON processing error in streaming request: {}", e.getMessage());
            sendErrorSSE(response, null, new JSONParseError());
        } catch (Throwable e) {
            LOGGER.error("Unexpected error processing streaming request: {}", e.getMessage(), e);
            sendErrorSSE(response, null, new InternalError(e.getMessage()));
        }

        LOGGER.debug("Completed streaming request processing");
    }

    /**
     * Handles incoming GET requests to the agent card endpoint.
     * Returns the agent card in JSON format.
     *
     * @return the agent card
     */
    @GET
    @Path("/.well-known/agent-card.json")
    @Produces(MediaType.APPLICATION_JSON)
    public AgentCard getAgentCard() {
        return jsonRpcHandler.getAgentCard();
    }

    private A2AResponse<?> processNonStreamingRequest(NonStreamingJSONRPCRequest<?> request,
                                                          ServerCallContext context) {
        if (request instanceof GetTaskRequest req) {
            return jsonRpcHandler.onGetTask(req, context);
        } else if (request instanceof CancelTaskRequest req) {
            return jsonRpcHandler.onCancelTask(req, context);
        } else if (request instanceof ListTasksRequest req) {
            return jsonRpcHandler.onListTasks(req, context);
        } else if (request instanceof CreateTaskPushNotificationConfigRequest req) {
            return jsonRpcHandler.setPushNotificationConfig(req, context);
        } else if (request instanceof GetTaskPushNotificationConfigRequest req) {
            return jsonRpcHandler.getPushNotificationConfig(req, context);
        } else if (request instanceof SendMessageRequest req) {
            return jsonRpcHandler.onMessageSend(req, context);
        } else if (request instanceof ListTaskPushNotificationConfigRequest req) {
            return jsonRpcHandler.listPushNotificationConfig(req, context);
        } else if (request instanceof DeleteTaskPushNotificationConfigRequest req) {
            return jsonRpcHandler.deletePushNotificationConfig(req, context);
        } else if (request instanceof GetExtendedAgentCardRequest req) {
            return jsonRpcHandler.onGetExtendedCardRequest(req, context);
        } else {
            return generateErrorResponse(request, new UnsupportedOperationError());
        }
    }

    /**
     * Creates a streaming publisher for the given request.
     * This method runs synchronously to avoid connection closure issues.
     */
    private Flow.Publisher<? extends A2AResponse<?>> createStreamingPublisher(StreamingJSONRPCRequest<?> request,
                                                                                 ServerCallContext context) {
        if (request instanceof SendStreamingMessageRequest req) {
            return jsonRpcHandler.onMessageSendStream(req, context);
        } else if (request instanceof SubscribeToTaskRequest req) {
            return jsonRpcHandler.onSubscribeToTask(req, context);
        } else {
            return null; // Unsupported request type
        }
    }

    /**
     * Handles the streaming response using custom SSE formatting.
     * This approach avoids JAX-RS SSE compatibility issues with async publishers.
     * Implements proper client disconnect detection and EventConsumer cancellation.
     */
    private void handleCustomSSEResponse(Flow.Publisher<? extends A2AResponse<?>> publisher,
                                       HttpServletResponse response,
                                       ServerCallContext context) throws IOException {

        PrintWriter writer = response.getWriter();
        AtomicLong eventId = new AtomicLong(0);
        CompletableFuture<Void> streamingComplete = new CompletableFuture<>();

        publisher.subscribe(new Flow.Subscriber<A2AResponse<?>>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                LOGGER.debug("Custom SSE subscriber onSubscribe called");
                this.subscription = subscription;
                // Use backpressure: request one item at a time
                subscription.request(1);

                // Notify tests that we are subscribed
                Runnable runnable = streamingIsSubscribedRunnable;
                if (runnable != null) {
                    runnable.run();
                }
            }

            @Override
            public void onNext(A2AResponse<?> item) {
                LOGGER.debug("Custom SSE subscriber onNext called with item: {}", item);
                try {
                    // Format as proper SSE event using centralized SseFormatter
                    long id = eventId.getAndIncrement();
                    String sseEvent = SseFormatter.formatResponseAsSSE(item, id);

                    writer.write(sseEvent);
                    writer.flush();

                    // Check if write failed (client disconnected)
                    // PrintWriter doesn't throw IOException, so we must check for errors explicitly
                    if (writer.checkError()) {
                        LOGGER.info("SSE write failed (likely client disconnect)");
                        handleClientDisconnect();
                        return;
                    }

                    LOGGER.debug("Custom SSE event sent successfully with id: {}", id);

                    // Request next item (backpressure)
                    subscription.request(1);
                } catch (Exception e) {
                    LOGGER.error("Error writing SSE event: {}", e.getMessage(), e);
                    onError(e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                LOGGER.debug("Custom SSE subscriber onError called: {}", throwable.getMessage(), throwable);
                handleClientDisconnect();
                streamingComplete.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                LOGGER.debug("Custom SSE subscriber onComplete called");
                try {
                    writer.close();
                } catch (Exception e) {
                    LOGGER.error("Error closing writer: {}", e.getMessage(), e);
                }
                streamingComplete.complete(null);
            }

            private void handleClientDisconnect() {
                LOGGER.debug("SSE connection closed, calling EventConsumer.cancel() to stop polling loop");
                // Cancel subscription to stop receiving events
                if (subscription != null) {
                    subscription.cancel();
                }
                // Call EventConsumer cancel callback to clean up ChildQueue
                context.invokeEventConsumerCancelCallback();
                try {
                    writer.close();
                } catch (Exception e) {
                    LOGGER.debug("Error closing writer during disconnect: {}", e.getMessage());
                }
            }
        });

        try {
            // Wait for streaming to complete before method returns
            streamingComplete.get();
        } catch (Exception e) {
            LOGGER.error("Error waiting for streaming completion: {}", e.getMessage(), e);
            throw new IOException("Streaming failed", e);
        }
    }


    private A2AResponse<?> generateErrorResponse(A2ARequest<?> request, A2AError error) {
        return new A2AErrorResponse(request.getId(), error);
    }

    /**
     * Sends an error response as a Server-Sent Event.
     */
    private void sendErrorSSE(HttpServletResponse response, Object id, A2AError error) {
        try {
            PrintWriter writer = response.getWriter();
            A2AErrorResponse errorResponse = new A2AErrorResponse(id, error);
            String jsonData = serializeResponse(errorResponse);
            writer.write("data: " + jsonData + "\n");
            writer.write("id: 0\n");
            writer.write("\n"); // Empty line to complete the event
            writer.flush();
            writer.close();
        } catch (Exception e) {
            LOGGER.error("Error sending SSE error response: {}", e.getMessage(), e);
        }
    }

    public static void setStreamingIsSubscribedRunnable(Runnable streamingIsSubscribedRunnable) {
        A2AServerResource.streamingIsSubscribedRunnable = streamingIsSubscribedRunnable;
    }

    private ServerCallContext createCallContext(HttpServletRequest request, SecurityContext securityContext) {

        if (callContextFactory.isUnsatisfied()) {
            User user;

            if (securityContext.getUserPrincipal() == null) {
                user = UnauthenticatedUser.INSTANCE;
            } else {
                user = new User() {
                    @Override
                    public boolean isAuthenticated() {
                        return true;
                    }

                    @Override
                    public String getUsername() {
                        return securityContext.getUserPrincipal().getName();
                    }
                };
            }
            Map<String, Object> state = new HashMap<>();
            // TODO Python's impl has
            //    state['auth'] = request.auth
            //  in jsonrpc_app.py. Figure out what this maps to in what we have here

            Map<String, String> headers = new HashMap<>();
            for (Enumeration<String> headerNames = request.getHeaderNames(); headerNames.hasMoreElements() ; ) {
                String name = headerNames.nextElement();
                headers.put(name, headers.get(name));
            }

            state.put("headers", headers);

            Enumeration<String> en = request.getHeaders(A2AHeaders.X_A2A_EXTENSIONS);
            List<String> extensionHeaderValues = new ArrayList<>();
            while (en.hasMoreElements()) {
                extensionHeaderValues.add(en.nextElement());
            }
            Set<String> requestedExtensions = A2AExtensions.getRequestedExtensions(extensionHeaderValues);
            return new ServerCallContext(user, state, requestedExtensions);
        } else {
            CallContextFactory builder = callContextFactory.get();
            return builder.build(request);
        }
    }

    // Exception mappers removed - all error handling now done in main handler method
    // to avoid JAX-RS double-encoding the JSON error responses

    /**
     * Serializes A2A responses to JSON using protobuf conversion.
     * This ensures enum values are serialized correctly using protobuf JSON format.
     */
    private static String serializeResponse(A2AResponse<?> response) {
        // For error responses, use JSONRPCUtils error serialization
        if (response instanceof A2AErrorResponse error) {
            return JSONRPCUtils.toJsonRPCErrorResponse(error.getId(), error.getError());
        }
        if (response.getError() != null) {
            return JSONRPCUtils.toJsonRPCErrorResponse(response.getId(), response.getError());
        }
        // Convert domain response to protobuf message and serialize
        com.google.protobuf.MessageOrBuilder protoMessage = convertToProto(response);
        return JSONRPCUtils.toJsonRPCResultResponse(response.getId(), protoMessage);
    }

    /**
     * Converts A2A response objects to their protobuf equivalents.
     */
    private static com.google.protobuf.MessageOrBuilder convertToProto(A2AResponse<?> response) {
        if (response instanceof io.a2a.jsonrpc.common.wrappers.GetTaskResponse r) {
            return ProtoUtils.ToProto.task(r.getResult());
        } else if (response instanceof io.a2a.jsonrpc.common.wrappers.CancelTaskResponse r) {
            return ProtoUtils.ToProto.task(r.getResult());
        } else if (response instanceof io.a2a.jsonrpc.common.wrappers.SendMessageResponse r) {
            return ProtoUtils.ToProto.taskOrMessage(r.getResult());
        } else if (response instanceof io.a2a.jsonrpc.common.wrappers.ListTasksResponse r) {
            return ProtoUtils.ToProto.listTasksResult(r.getResult());
        } else if (response instanceof io.a2a.jsonrpc.common.wrappers.CreateTaskPushNotificationConfigResponse r) {
            return ProtoUtils.ToProto.createTaskPushNotificationConfigResponse(r.getResult());
        } else if (response instanceof io.a2a.jsonrpc.common.wrappers.GetTaskPushNotificationConfigResponse r) {
            return ProtoUtils.ToProto.getTaskPushNotificationConfigResponse(r.getResult());
        } else if (response instanceof io.a2a.jsonrpc.common.wrappers.ListTaskPushNotificationConfigResponse r) {
            return ProtoUtils.ToProto.listTaskPushNotificationConfigResponse(r.getResult());
        } else if (response instanceof io.a2a.jsonrpc.common.wrappers.DeleteTaskPushNotificationConfigResponse) {
            // DeleteTaskPushNotificationConfig has no result body, just return empty message
            return com.google.protobuf.Empty.getDefaultInstance();
        } else if (response instanceof io.a2a.jsonrpc.common.wrappers.GetExtendedAgentCardResponse r) {
            return ProtoUtils.ToProto.getExtendedCardResponse(r.getResult());
        } else if (response instanceof io.a2a.jsonrpc.common.wrappers.SendStreamingMessageResponse r) {
            return ProtoUtils.ToProto.taskOrMessageStream(r.getResult());
        } else {
            throw new IllegalArgumentException("Unknown response type: " + response.getClass().getName());
        }
    }
}