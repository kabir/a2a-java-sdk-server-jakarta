package org.wildfly.extras.a2a.server.apps.jsonrpc.multiversion;

import static org.a2aproject.sdk.server.ServerCallContext.TRANSPORT_KEY;
import static org.a2aproject.sdk.transport.jsonrpc.context.JSONRPCContextKeys.HEADERS_KEY;
import static org.a2aproject.sdk.transport.jsonrpc.context.JSONRPCContextKeys.TENANT_KEY;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.a2aproject.sdk.common.A2AHeaders;
import org.a2aproject.sdk.compat03.common.A2AHeaders_v0_3;
import org.a2aproject.sdk.compat03.conversion.A2AProtocol_v0_3;
import org.a2aproject.sdk.compat03.json.JsonUtil_v0_3;
import org.a2aproject.sdk.compat03.json.JsonProcessingException_v0_3;
import org.a2aproject.sdk.compat03.spec.CancelTaskRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.DeleteTaskPushNotificationConfigParams_v0_3;
import org.a2aproject.sdk.compat03.spec.DeleteTaskPushNotificationConfigRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.GetAuthenticatedExtendedCardRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.GetTaskPushNotificationConfigParams_v0_3;
import org.a2aproject.sdk.compat03.spec.GetTaskPushNotificationConfigRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.GetTaskRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.InternalError_v0_3;
import org.a2aproject.sdk.compat03.spec.InvalidParamsError_v0_3;
import org.a2aproject.sdk.compat03.spec.InvalidRequestError_v0_3;
import org.a2aproject.sdk.compat03.spec.JSONParseError_v0_3;
import org.a2aproject.sdk.compat03.spec.JSONRPCError_v0_3;
import org.a2aproject.sdk.compat03.spec.JSONRPCErrorResponse_v0_3;
import org.a2aproject.sdk.compat03.spec.JSONRPCMessage_v0_3;
import org.a2aproject.sdk.compat03.spec.JSONRPCResponse_v0_3;
import org.a2aproject.sdk.compat03.spec.ListTaskPushNotificationConfigParams_v0_3;
import org.a2aproject.sdk.compat03.spec.ListTaskPushNotificationConfigRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.MessageSendParams_v0_3;
import org.a2aproject.sdk.compat03.spec.MethodNotFoundError_v0_3;
import org.a2aproject.sdk.compat03.spec.NonStreamingJSONRPCRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.SendMessageRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.SendStreamingMessageRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.SetTaskPushNotificationConfigRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.StreamingJSONRPCRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.TaskIdParams_v0_3;
import org.a2aproject.sdk.compat03.spec.TaskPushNotificationConfig_v0_3;
import org.a2aproject.sdk.compat03.spec.TaskQueryParams_v0_3;
import org.a2aproject.sdk.compat03.spec.TaskResubscriptionRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.UnsupportedOperationError_v0_3;
import org.a2aproject.sdk.compat03.transport.jsonrpc.handler.JSONRPCHandler_v0_3;
import org.a2aproject.sdk.compat03.util.Utils_v0_3;
import org.a2aproject.sdk.grpc.utils.JSONRPCUtils;
import org.a2aproject.sdk.grpc.utils.ProtoUtils;
import org.a2aproject.sdk.jsonrpc.common.json.IdJsonMappingException;
import org.a2aproject.sdk.jsonrpc.common.json.InvalidParamsJsonMappingException;
import org.a2aproject.sdk.jsonrpc.common.json.JsonMappingException;
import org.a2aproject.sdk.jsonrpc.common.json.JsonProcessingException;
import org.a2aproject.sdk.jsonrpc.common.json.MethodNotFoundJsonMappingException;
import org.a2aproject.sdk.jsonrpc.common.wrappers.A2AErrorResponse;
import org.a2aproject.sdk.jsonrpc.common.wrappers.A2ARequest;
import org.a2aproject.sdk.jsonrpc.common.wrappers.A2AResponse;
import org.a2aproject.sdk.jsonrpc.common.wrappers.CancelTaskRequest;
import org.a2aproject.sdk.jsonrpc.common.wrappers.CreateTaskPushNotificationConfigRequest;
import org.a2aproject.sdk.jsonrpc.common.wrappers.DeleteTaskPushNotificationConfigRequest;
import org.a2aproject.sdk.jsonrpc.common.wrappers.GetExtendedAgentCardRequest;
import org.a2aproject.sdk.jsonrpc.common.wrappers.GetTaskPushNotificationConfigRequest;
import org.a2aproject.sdk.jsonrpc.common.wrappers.GetTaskRequest;
import org.a2aproject.sdk.jsonrpc.common.wrappers.ListTaskPushNotificationConfigsRequest;
import org.a2aproject.sdk.jsonrpc.common.wrappers.ListTasksRequest;
import org.a2aproject.sdk.jsonrpc.common.wrappers.NonStreamingJSONRPCRequest;
import org.a2aproject.sdk.jsonrpc.common.wrappers.SendMessageRequest;
import org.a2aproject.sdk.jsonrpc.common.wrappers.SendStreamingMessageRequest;
import org.a2aproject.sdk.jsonrpc.common.wrappers.StreamingJSONRPCRequest;
import org.a2aproject.sdk.jsonrpc.common.wrappers.SubscribeToTaskRequest;
import org.a2aproject.sdk.server.ServerCallContext;
import org.a2aproject.sdk.server.auth.UnauthenticatedUser;
import org.a2aproject.sdk.server.auth.User;
import org.a2aproject.sdk.server.extensions.A2AExtensions;
import org.a2aproject.sdk.server.util.async.Internal;
import org.a2aproject.sdk.server.util.sse.SseFormatter;
import org.a2aproject.sdk.spec.A2AError;
import org.a2aproject.sdk.spec.AgentCard;
import org.a2aproject.sdk.spec.InternalError;
import org.a2aproject.sdk.spec.InvalidParamsError;
import org.a2aproject.sdk.spec.InvalidRequestError;
import org.a2aproject.sdk.spec.JSONParseError;
import org.a2aproject.sdk.spec.MethodNotFoundError;
import org.a2aproject.sdk.spec.TransportProtocol;
import org.a2aproject.sdk.spec.UnsupportedOperationError;
import org.a2aproject.sdk.spec.VersionNotSupportedError;
import org.a2aproject.sdk.transport.jsonrpc.handler.JSONRPCHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
public class MultiVersionA2AServerResource {

    private static final String VERSION_1_0 = "1.0";
    private static final String VERSION_0_3 = "0.3";

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiVersionA2AServerResource.class);

    @Inject
    JSONRPCHandler jsonRpcHandler;

    @Inject
    JSONRPCHandler_v0_3 jsonRpcHandler_v0_3;

    // Hook so testing can wait until the async Subscription is subscribed.
    private static volatile Runnable streamingIsSubscribedRunnable;

    @Inject
    @Internal
    Executor executor;


    // -----------------------------------------------------------------------
    // Agent card endpoint (serves v1.0 agent card since v1.0 is present)
    // -----------------------------------------------------------------------

    /**
     * Handles incoming GET requests to the agent card endpoint.
     * Returns the v1.0 agent card in JSON format with appropriate caching headers.
     */
    @GET
    @Path("/.well-known/agent-card.json")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAgentCard() {
        AgentCard agentCard = jsonRpcHandler.getAgentCard();

        // Generate ETag based on agent card content hash
        String etag = "\"" + Integer.toHexString(agentCard.hashCode()) + "\"";

        // Set Last-Modified to current time in RFC 1123 format
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("GMT"));
        String lastModified = now.format(DateTimeFormatter.RFC_1123_DATE_TIME);

        return Response.ok(agentCard)
                .header(HttpHeaders.CACHE_CONTROL, "max-age=3600")
                .header(HttpHeaders.ETAG, etag)
                .header("Last-Modified", lastModified)
                .build();
    }

    // -----------------------------------------------------------------------
    // Non-streaming POST / handler
    // -----------------------------------------------------------------------

    /**
     * Handles incoming non-streaming POST requests to the main A2A endpoint.
     * Dispatches to v1.0 or v0.3 handler based on the A2A-Version header.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response handleNonStreamingRequests(
            String body,
            @Context HttpServletRequest httpRequest,
            @Context SecurityContext securityContext) {

        String version = resolveVersion(httpRequest);

        if (isV10(version)) {
            return handleNonStreamingV10(body, httpRequest, securityContext);
        } else if (isV03(version)) {
            return handleNonStreamingV03(body, httpRequest, securityContext);
        } else {
            // Unsupported version
            VersionNotSupportedError error = new VersionNotSupportedError(
                    null,
                    "Protocol version '" + version + "' is not supported. Supported versions: [1.0, 0.3]",
                    null);
            String serialized = JSONRPCUtils.toJsonRPCErrorResponse(null, error);
            return Response.status(Response.Status.OK)
                    .header(HttpHeaders.CONTENT_TYPE, org.a2aproject.sdk.common.MediaType.APPLICATION_JSON)
                    .entity(serialized)
                    .build();
        }
    }

    // -----------------------------------------------------------------------
    // Streaming POST / handler
    // -----------------------------------------------------------------------

    /**
     * Handles incoming streaming POST requests to the main A2A endpoint.
     * Dispatches to v1.0 or v0.3 handler based on the A2A-Version header.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public void handleStreamingRequests(
            String body,
            @Context HttpServletResponse response,
            @Context HttpServletRequest httpRequest,
            @Context SecurityContext securityContext) throws IOException {

        String version = resolveVersion(httpRequest);

        if (isV10(version)) {
            handleStreamingV10(body, response, httpRequest, securityContext);
        } else if (isV03(version)) {
            handleStreamingV03(body, response, httpRequest, securityContext);
        } else {
            // Unsupported version
            VersionNotSupportedError error = new VersionNotSupportedError(
                    null,
                    "Protocol version '" + version + "' is not supported. Supported versions: [1.0, 0.3]",
                    null);
            String serialized = JSONRPCUtils.toJsonRPCErrorResponse(null, error);
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(org.a2aproject.sdk.common.MediaType.APPLICATION_JSON);
            response.getWriter().write(serialized);
            response.getWriter().flush();
        }
    }

    // =======================================================================
    // v1.0 handler logic
    // =======================================================================

    private Response handleNonStreamingV10(
            String body,
            HttpServletRequest httpRequest,
            SecurityContext securityContext) {

        ServerCallContext context = createV10CallContext(httpRequest, securityContext);
        LOGGER.debug("Handling v1.0 non-streaming request");
        A2AResponse<?> response;
        try {
            A2ARequest<?> request = JSONRPCUtils.parseRequestBody(body, null);
            response = processNonStreamingRequestV10((NonStreamingJSONRPCRequest<?>) request, context);
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
        }

        String serialized = serializeV10Response(response);
        String contentType = org.a2aproject.sdk.common.MediaType.APPLICATION_JSON;

        return Response.status(Response.Status.OK)
                .header(HttpHeaders.CONTENT_TYPE, contentType)
                .entity(serialized)
                .build();
    }

    private void handleStreamingV10(
            String body,
            HttpServletResponse response,
            HttpServletRequest httpRequest,
            SecurityContext securityContext) throws IOException {

        ServerCallContext context = createV10CallContext(httpRequest, securityContext);
        LOGGER.debug("Handling v1.0 streaming request");

        A2ARequest<?> request = null;
        try {
            request = JSONRPCUtils.parseRequestBody(body, null);
            validateStreamingRequestV10((StreamingJSONRPCRequest<?>) request);
        } catch (A2AError e) {
            LOGGER.debug("A2AError validating streaming request: {}", e.getMessage());
            sendJsonRpcErrorV10(response, request != null ? request.getId() : null, e);
            return;
        } catch (InvalidParamsJsonMappingException e) {
            LOGGER.warn("Invalid params in streaming request: {}", e.getMessage());
            sendJsonRpcErrorV10(response, e.getId(), new InvalidParamsError(null, e.getMessage(), null));
            return;
        } catch (MethodNotFoundJsonMappingException e) {
            LOGGER.warn("Method not found in streaming request: {}", e.getMessage());
            sendJsonRpcErrorV10(response, e.getId(), new MethodNotFoundError(null, e.getMessage(), null));
            return;
        } catch (IdJsonMappingException e) {
            LOGGER.warn("Invalid request ID in streaming request: {}", e.getMessage());
            sendJsonRpcErrorV10(response, e.getId(), new InvalidRequestError(null, e.getMessage(), null));
            return;
        } catch (JsonMappingException e) {
            LOGGER.warn("JSON mapping error in streaming request: {}", e.getMessage(), e);
            sendJsonRpcErrorV10(response, null, new InvalidRequestError(null, e.getMessage(), null));
            return;
        } catch (JsonSyntaxException e) {
            LOGGER.warn("JSON syntax error in streaming request: {}", e.getMessage());
            sendJsonRpcErrorV10(response, null, new JSONParseError(e.getMessage()));
            return;
        } catch (JsonProcessingException e) {
            LOGGER.warn("JSON processing error in streaming request: {}", e.getMessage());
            sendJsonRpcErrorV10(response, null, new JSONParseError(e.getMessage()));
            return;
        } catch (Throwable e) {
            LOGGER.error("Unexpected error processing streaming request: {}", e.getMessage(), e);
            sendJsonRpcErrorV10(response, null, new InternalError(e.getMessage()));
            return;
        }

        response.setContentType(MediaType.SERVER_SENT_EVENTS);
        response.setCharacterEncoding("UTF-8");
        response.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");

        try {
            Flow.Publisher<? extends A2AResponse<?>> publisher = createStreamingPublisherV10((StreamingJSONRPCRequest<?>) request, context);
            LOGGER.debug("Created v1.0 streaming publisher: {}", publisher);

            if (publisher != null) {
                handleCustomSSEResponseV10(publisher, response, context);
            } else {
                LOGGER.debug("Unsupported streaming request type: {}", request.getClass().getSimpleName());
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unsupported streaming request type");
            }
        } catch (A2AError e) {
            LOGGER.debug("A2AError in streaming request: {}", e.getMessage());
            sendErrorSSEV10(response, request.getId(), e);
        } catch (Throwable e) {
            LOGGER.error("Unexpected error processing streaming request: {}", e.getMessage(), e);
            sendErrorSSEV10(response, null, new InternalError(e.getMessage()));
        }

        LOGGER.debug("Completed v1.0 streaming request processing");
    }

    private A2AResponse<?> processNonStreamingRequestV10(NonStreamingJSONRPCRequest<?> request,
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
        } else if (request instanceof ListTaskPushNotificationConfigsRequest req) {
            return jsonRpcHandler.listPushNotificationConfigs(req, context);
        } else if (request instanceof DeleteTaskPushNotificationConfigRequest req) {
            return jsonRpcHandler.deletePushNotificationConfig(req, context);
        } else if (request instanceof GetExtendedAgentCardRequest req) {
            return jsonRpcHandler.onGetExtendedCardRequest(req, context);
        } else {
            return new A2AErrorResponse(request.getId(), new UnsupportedOperationError());
        }
    }

    private void validateStreamingRequestV10(StreamingJSONRPCRequest<?> request) throws A2AError {
        if (request instanceof SendStreamingMessageRequest req) {
            jsonRpcHandler.validateRequestedTask(req.getParams().message().taskId());
        } else if (request instanceof SubscribeToTaskRequest req) {
            jsonRpcHandler.validateRequestedTask(req.getParams().id());
        }
    }

    private Flow.Publisher<? extends A2AResponse<?>> createStreamingPublisherV10(StreamingJSONRPCRequest<?> request,
                                                                                ServerCallContext context) {
        if (request instanceof SendStreamingMessageRequest req) {
            return jsonRpcHandler.onMessageSendStream(req, context);
        } else if (request instanceof SubscribeToTaskRequest req) {
            return jsonRpcHandler.onSubscribeToTask(req, context);
        } else {
            return null;
        }
    }

    private void handleCustomSSEResponseV10(Flow.Publisher<? extends A2AResponse<?>> publisher,
                                            HttpServletResponse response,
                                            ServerCallContext context) throws IOException {
        PrintWriter writer = response.getWriter();
        AtomicLong eventId = new AtomicLong(0);
        CompletableFuture<Void> streamingComplete = new CompletableFuture<>();

        publisher.subscribe(new Flow.Subscriber<A2AResponse<?>>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                LOGGER.debug("v1.0 SSE subscriber onSubscribe called");
                this.subscription = subscription;
                subscription.request(1);

                Runnable runnable = streamingIsSubscribedRunnable;
                if (runnable != null) {
                    runnable.run();
                }
            }

            @Override
            public void onNext(A2AResponse<?> item) {
                LOGGER.debug("v1.0 SSE subscriber onNext called with item: {}", item);
                try {
                    long id = eventId.getAndIncrement();
                    String sseEvent = SseFormatter.formatResponseAsSSE(item, id);

                    writer.write(sseEvent);
                    writer.flush();

                    if (writer.checkError()) {
                        LOGGER.info("SSE write failed (likely client disconnect)");
                        handleClientDisconnect();
                        return;
                    }

                    LOGGER.debug("v1.0 SSE event sent successfully with id: {}", id);
                    subscription.request(1);
                } catch (Exception e) {
                    LOGGER.error("Error writing SSE event: {}", e.getMessage(), e);
                    onError(e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                LOGGER.debug("v1.0 SSE subscriber onError called: {}", throwable.getMessage(), throwable);
                handleClientDisconnect();
                streamingComplete.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                LOGGER.debug("v1.0 SSE subscriber onComplete called");
                try {
                    writer.close();
                } catch (Exception e) {
                    LOGGER.error("Error closing writer: {}", e.getMessage(), e);
                }
                streamingComplete.complete(null);
            }

            private void handleClientDisconnect() {
                LOGGER.debug("SSE connection closed, calling EventConsumer.cancel() to stop polling loop");
                if (subscription != null) {
                    subscription.cancel();
                }
                context.invokeEventConsumerCancelCallback();
                try {
                    writer.close();
                } catch (Exception e) {
                    LOGGER.debug("Error closing writer during disconnect: {}", e.getMessage());
                }
            }
        });

        try {
            streamingComplete.get();
        } catch (Exception e) {
            LOGGER.error("Error waiting for streaming completion: {}", e.getMessage(), e);
            throw new IOException("Streaming failed", e);
        }
    }

    private void sendJsonRpcErrorV10(HttpServletResponse response, Object id, A2AError error) {
        try {
            A2AErrorResponse errorResponse = new A2AErrorResponse(id, error);
            String jsonData = serializeV10Response(errorResponse);
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(org.a2aproject.sdk.common.MediaType.APPLICATION_JSON);
            response.getWriter().write(jsonData);
            response.getWriter().flush();
        } catch (Exception e) {
            LOGGER.error("Error sending JSON-RPC error response: {}", e.getMessage(), e);
        }
    }

    private void sendErrorSSEV10(HttpServletResponse response, Object id, A2AError error) {
        try {
            PrintWriter writer = response.getWriter();
            A2AErrorResponse errorResponse = new A2AErrorResponse(id, error);
            String jsonData = serializeV10Response(errorResponse);
            writer.write("data: " + jsonData + "\n");
            writer.write("id: 0\n");
            writer.write("\n");
            writer.flush();
            writer.close();
        } catch (Exception e) {
            LOGGER.error("Error sending SSE error response: {}", e.getMessage(), e);
        }
    }

    /**
     * Serializes v1.0 A2A responses to JSON using protobuf conversion.
     */
    private static String serializeV10Response(A2AResponse<?> response) {
        if (response instanceof A2AErrorResponse error) {
            return JSONRPCUtils.toJsonRPCErrorResponse(error.getId(), error.getError());
        }
        if (response.getError() != null) {
            return JSONRPCUtils.toJsonRPCErrorResponse(response.getId(), response.getError());
        }
        com.google.protobuf.MessageOrBuilder protoMessage = convertToProto(response);
        return JSONRPCUtils.toJsonRPCResultResponse(response.getId(), protoMessage);
    }

    private static com.google.protobuf.MessageOrBuilder convertToProto(A2AResponse<?> response) {
        if (response instanceof org.a2aproject.sdk.jsonrpc.common.wrappers.GetTaskResponse r) {
            return ProtoUtils.ToProto.task(r.getResult());
        } else if (response instanceof org.a2aproject.sdk.jsonrpc.common.wrappers.CancelTaskResponse r) {
            return ProtoUtils.ToProto.task(r.getResult());
        } else if (response instanceof org.a2aproject.sdk.jsonrpc.common.wrappers.SendMessageResponse r) {
            return ProtoUtils.ToProto.taskOrMessage(r.getResult());
        } else if (response instanceof org.a2aproject.sdk.jsonrpc.common.wrappers.ListTasksResponse r) {
            return ProtoUtils.ToProto.listTasksResult(r.getResult());
        } else if (response instanceof org.a2aproject.sdk.jsonrpc.common.wrappers.CreateTaskPushNotificationConfigResponse r) {
            return ProtoUtils.ToProto.createTaskPushNotificationConfigResponse(r.getResult());
        } else if (response instanceof org.a2aproject.sdk.jsonrpc.common.wrappers.GetTaskPushNotificationConfigResponse r) {
            return ProtoUtils.ToProto.getTaskPushNotificationConfigResponse(r.getResult());
        } else if (response instanceof org.a2aproject.sdk.jsonrpc.common.wrappers.ListTaskPushNotificationConfigsResponse r) {
            return ProtoUtils.ToProto.listTaskPushNotificationConfigsResponse(r.getResult());
        } else if (response instanceof org.a2aproject.sdk.jsonrpc.common.wrappers.DeleteTaskPushNotificationConfigResponse) {
            return com.google.protobuf.Empty.getDefaultInstance();
        } else if (response instanceof org.a2aproject.sdk.jsonrpc.common.wrappers.GetExtendedAgentCardResponse r) {
            return ProtoUtils.ToProto.getExtendedCardResponse(r.getResult());
        } else if (response instanceof org.a2aproject.sdk.jsonrpc.common.wrappers.SendStreamingMessageResponse r) {
            return ProtoUtils.ToProto.taskOrMessageStream(r.getResult());
        } else {
            throw new IllegalArgumentException("Unknown response type: " + response.getClass().getName());
        }
    }

    // =======================================================================
    // v0.3 handler logic
    // =======================================================================

    private Response handleNonStreamingV03(
            String body,
            HttpServletRequest httpRequest,
            SecurityContext securityContext) {

        ServerCallContext context = createV03CallContext(httpRequest, securityContext);
        LOGGER.debug("Handling v0.3 non-streaming request");

        JSONRPCResponse_v0_3<?> nonStreamingResponse = null;
        JSONRPCErrorResponse_v0_3 error = null;
        Object requestId = null;
        try {
            com.google.gson.JsonObject node;
            try {
                node = JsonParser.parseString(body).getAsJsonObject();
            } catch (Exception e) {
                throw new JSONParseError_v0_3(e.getMessage());
            }

            // Extract id field early so error responses can include it
            com.google.gson.JsonElement idElement = node.get("id");
            if (idElement != null && !idElement.isJsonNull() && !idElement.isJsonPrimitive()) {
                throw new InvalidRequestError_v0_3("Invalid JSON-RPC request: 'id' must be a string, number, or null");
            }
            if (idElement != null && !idElement.isJsonNull() && idElement.isJsonPrimitive()) {
                com.google.gson.JsonPrimitive idPrimitive = idElement.getAsJsonPrimitive();
                requestId = idPrimitive.isNumber() ? idPrimitive.getAsLong() : idPrimitive.getAsString();
            }

            // Validate jsonrpc field
            com.google.gson.JsonElement jsonrpcElement = node.get("jsonrpc");
            if (jsonrpcElement == null || !jsonrpcElement.isJsonPrimitive()
                    || !JSONRPCMessage_v0_3.JSONRPC_VERSION.equals(jsonrpcElement.getAsString())) {
                throw new InvalidRequestError_v0_3("Invalid JSON-RPC request: missing or invalid 'jsonrpc' field");
            }

            // Validate method field
            com.google.gson.JsonElement methodElement = node.get("method");
            if (methodElement == null || !methodElement.isJsonPrimitive()) {
                throw new InvalidRequestError_v0_3("Invalid JSON-RPC request: missing or invalid 'method' field");
            }

            String methodName = methodElement.getAsString();
            context.getState().put(org.a2aproject.sdk.compat03.transport.jsonrpc.context.JSONRPCContextKeys_v0_3.METHOD_NAME_KEY, methodName);

            NonStreamingJSONRPCRequest_v0_3<?> request = deserializeNonStreamingRequestV03(node, requestId, methodName);
            nonStreamingResponse = processNonStreamingRequestV03(request, context);
        } catch (JSONRPCError_v0_3 e) {
            error = new JSONRPCErrorResponse_v0_3(requestId, e);
        } catch (JsonSyntaxException e) {
            error = new JSONRPCErrorResponse_v0_3(requestId, new JSONParseError_v0_3(e.getMessage()));
        } catch (Throwable t) {
            error = new JSONRPCErrorResponse_v0_3(requestId, new InternalError_v0_3(t.getMessage()));
        }

        String serialized;
        if (error != null) {
            serialized = Utils_v0_3.toJsonString(error);
        } else {
            serialized = Utils_v0_3.toJsonString(nonStreamingResponse);
        }

        return Response.status(Response.Status.OK)
                .header(HttpHeaders.CONTENT_TYPE, org.a2aproject.sdk.common.MediaType.APPLICATION_JSON)
                .entity(serialized)
                .build();
    }

    private void handleStreamingV03(
            String body,
            HttpServletResponse response,
            HttpServletRequest httpRequest,
            SecurityContext securityContext) throws IOException {

        ServerCallContext context = createV03CallContext(httpRequest, securityContext);
        LOGGER.debug("Handling v0.3 streaming request");

        Flow.Publisher<? extends JSONRPCResponse_v0_3<?>> streamingPublisher = null;
        JSONRPCErrorResponse_v0_3 error = null;
        Object requestId = null;
        try {
            com.google.gson.JsonObject node;
            try {
                node = JsonParser.parseString(body).getAsJsonObject();
            } catch (Exception e) {
                throw new JSONParseError_v0_3(e.getMessage());
            }

            // Extract id field early so error responses can include it
            com.google.gson.JsonElement idElement = node.get("id");
            if (idElement != null && !idElement.isJsonNull() && !idElement.isJsonPrimitive()) {
                throw new InvalidRequestError_v0_3("Invalid JSON-RPC request: 'id' must be a string, number, or null");
            }
            if (idElement != null && !idElement.isJsonNull() && idElement.isJsonPrimitive()) {
                com.google.gson.JsonPrimitive idPrimitive = idElement.getAsJsonPrimitive();
                requestId = idPrimitive.isNumber() ? idPrimitive.getAsLong() : idPrimitive.getAsString();
            }

            // Validate jsonrpc field
            com.google.gson.JsonElement jsonrpcElement = node.get("jsonrpc");
            if (jsonrpcElement == null || !jsonrpcElement.isJsonPrimitive()
                    || !JSONRPCMessage_v0_3.JSONRPC_VERSION.equals(jsonrpcElement.getAsString())) {
                throw new InvalidRequestError_v0_3("Invalid JSON-RPC request: missing or invalid 'jsonrpc' field");
            }

            // Validate method field
            com.google.gson.JsonElement methodElement = node.get("method");
            if (methodElement == null || !methodElement.isJsonPrimitive()) {
                throw new InvalidRequestError_v0_3("Invalid JSON-RPC request: missing or invalid 'method' field");
            }

            String methodName = methodElement.getAsString();
            context.getState().put(org.a2aproject.sdk.compat03.transport.jsonrpc.context.JSONRPCContextKeys_v0_3.METHOD_NAME_KEY, methodName);

            StreamingJSONRPCRequest_v0_3<?> request = deserializeStreamingRequestV03(node, requestId, methodName);
            streamingPublisher = createStreamingPublisherV03(request, context);
        } catch (JSONRPCError_v0_3 e) {
            error = new JSONRPCErrorResponse_v0_3(requestId, e);
        } catch (JsonSyntaxException e) {
            error = new JSONRPCErrorResponse_v0_3(requestId, new JSONParseError_v0_3(e.getMessage()));
        } catch (Throwable t) {
            error = new JSONRPCErrorResponse_v0_3(requestId, new InternalError_v0_3(t.getMessage()));
        }

        if (error != null) {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(org.a2aproject.sdk.common.MediaType.APPLICATION_JSON);
            response.getWriter().write(Utils_v0_3.toJsonString(error));
            response.getWriter().flush();
        } else if (streamingPublisher != null) {
            response.setContentType(MediaType.SERVER_SENT_EVENTS);
            response.setCharacterEncoding("UTF-8");
            response.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
            handleCustomSSEResponseV03(streamingPublisher, response, context);
        }
    }

    private NonStreamingJSONRPCRequest_v0_3<?> deserializeNonStreamingRequestV03(
            com.google.gson.JsonObject node, Object requestId, String methodName) {
        try {
            return switch (methodName) {
                case GetTaskRequest_v0_3.METHOD -> new GetTaskRequest_v0_3(
                        JSONRPCMessage_v0_3.JSONRPC_VERSION, requestId, methodName, deserializeParamsV03(node, TaskQueryParams_v0_3.class));
                case CancelTaskRequest_v0_3.METHOD -> new CancelTaskRequest_v0_3(
                        JSONRPCMessage_v0_3.JSONRPC_VERSION, requestId, methodName, deserializeParamsV03(node, TaskIdParams_v0_3.class));
                case SendMessageRequest_v0_3.METHOD -> new SendMessageRequest_v0_3(
                        JSONRPCMessage_v0_3.JSONRPC_VERSION, requestId, methodName, deserializeParamsV03(node, MessageSendParams_v0_3.class));
                case SetTaskPushNotificationConfigRequest_v0_3.METHOD -> new SetTaskPushNotificationConfigRequest_v0_3(
                        JSONRPCMessage_v0_3.JSONRPC_VERSION, requestId, methodName, deserializeParamsV03(node, TaskPushNotificationConfig_v0_3.class));
                case GetTaskPushNotificationConfigRequest_v0_3.METHOD -> new GetTaskPushNotificationConfigRequest_v0_3(
                        JSONRPCMessage_v0_3.JSONRPC_VERSION, requestId, methodName, deserializeParamsV03(node, GetTaskPushNotificationConfigParams_v0_3.class));
                case ListTaskPushNotificationConfigRequest_v0_3.METHOD -> new ListTaskPushNotificationConfigRequest_v0_3(
                        JSONRPCMessage_v0_3.JSONRPC_VERSION, requestId, methodName, deserializeParamsV03(node, ListTaskPushNotificationConfigParams_v0_3.class));
                case DeleteTaskPushNotificationConfigRequest_v0_3.METHOD -> new DeleteTaskPushNotificationConfigRequest_v0_3(
                        JSONRPCMessage_v0_3.JSONRPC_VERSION, requestId, methodName, deserializeParamsV03(node, DeleteTaskPushNotificationConfigParams_v0_3.class));
                case GetAuthenticatedExtendedCardRequest_v0_3.METHOD -> new GetAuthenticatedExtendedCardRequest_v0_3(
                        JSONRPCMessage_v0_3.JSONRPC_VERSION, requestId, methodName, null);
                default -> throw new MethodNotFoundError_v0_3();
            };
        } catch (JSONRPCError_v0_3 e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidParamsError_v0_3(e.getMessage());
        }
    }

    private StreamingJSONRPCRequest_v0_3<?> deserializeStreamingRequestV03(
            com.google.gson.JsonObject node, Object requestId, String methodName) {
        try {
            return switch (methodName) {
                case SendStreamingMessageRequest_v0_3.METHOD -> new SendStreamingMessageRequest_v0_3(
                        JSONRPCMessage_v0_3.JSONRPC_VERSION, requestId, methodName, deserializeParamsV03(node, MessageSendParams_v0_3.class));
                case TaskResubscriptionRequest_v0_3.METHOD -> new TaskResubscriptionRequest_v0_3(
                        JSONRPCMessage_v0_3.JSONRPC_VERSION, requestId, methodName, deserializeParamsV03(node, TaskIdParams_v0_3.class));
                default -> throw new MethodNotFoundError_v0_3();
            };
        } catch (JSONRPCError_v0_3 e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidParamsError_v0_3(e.getMessage());
        }
    }

    private <T> T deserializeParamsV03(com.google.gson.JsonObject node, Class<T> paramsType) throws JsonProcessingException_v0_3 {
        com.google.gson.JsonElement paramsElement = node.get("params");
        if (paramsElement == null || paramsElement.isJsonNull()) {
            return null;
        }
        return JsonUtil_v0_3.fromJson(paramsElement.toString(), paramsType);
    }

    private JSONRPCResponse_v0_3<?> processNonStreamingRequestV03(
            NonStreamingJSONRPCRequest_v0_3<?> request, ServerCallContext context) {
        if (request instanceof GetTaskRequest_v0_3 req) {
            return jsonRpcHandler_v0_3.onGetTask(req, context);
        } else if (request instanceof CancelTaskRequest_v0_3 req) {
            return jsonRpcHandler_v0_3.onCancelTask(req, context);
        } else if (request instanceof SetTaskPushNotificationConfigRequest_v0_3 req) {
            return jsonRpcHandler_v0_3.setPushNotificationConfig(req, context);
        } else if (request instanceof GetTaskPushNotificationConfigRequest_v0_3 req) {
            return jsonRpcHandler_v0_3.getPushNotificationConfig(req, context);
        } else if (request instanceof SendMessageRequest_v0_3 req) {
            return jsonRpcHandler_v0_3.onMessageSend(req, context);
        } else if (request instanceof ListTaskPushNotificationConfigRequest_v0_3 req) {
            return jsonRpcHandler_v0_3.listPushNotificationConfig(req, context);
        } else if (request instanceof DeleteTaskPushNotificationConfigRequest_v0_3 req) {
            return jsonRpcHandler_v0_3.deletePushNotificationConfig(req, context);
        } else if (request instanceof GetAuthenticatedExtendedCardRequest_v0_3 req) {
            return jsonRpcHandler_v0_3.onGetAuthenticatedExtendedCardRequest(req, context);
        } else {
            return new JSONRPCErrorResponse_v0_3(request.getId(), new UnsupportedOperationError_v0_3());
        }
    }

    private Flow.Publisher<? extends JSONRPCResponse_v0_3<?>> createStreamingPublisherV03(
            StreamingJSONRPCRequest_v0_3<?> request, ServerCallContext context) {
        if (request instanceof SendStreamingMessageRequest_v0_3 req) {
            return jsonRpcHandler_v0_3.onMessageSendStream(req, context);
        } else if (request instanceof TaskResubscriptionRequest_v0_3 req) {
            return jsonRpcHandler_v0_3.onResubscribeToTask(req, context);
        } else {
            return null;
        }
    }

    private void handleCustomSSEResponseV03(Flow.Publisher<? extends JSONRPCResponse_v0_3<?>> publisher,
                                            HttpServletResponse response,
                                            ServerCallContext context) throws IOException {
        PrintWriter writer = response.getWriter();
        AtomicLong eventId = new AtomicLong(0);
        CompletableFuture<Void> streamingComplete = new CompletableFuture<>();

        publisher.subscribe(new Flow.Subscriber<JSONRPCResponse_v0_3<?>>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                LOGGER.debug("v0.3 SSE subscriber onSubscribe called");
                this.subscription = subscription;
                subscription.request(1);

                Runnable runnable = streamingIsSubscribedRunnable;
                if (runnable != null) {
                    runnable.run();
                }
            }

            @Override
            public void onNext(JSONRPCResponse_v0_3<?> item) {
                LOGGER.debug("v0.3 SSE subscriber onNext called with item: {}", item);
                try {
                    long id = eventId.getAndIncrement();
                    String sseEvent = formatSseEventV03(item, id);

                    writer.write(sseEvent);
                    writer.flush();

                    if (writer.checkError()) {
                        LOGGER.info("SSE write failed (likely client disconnect)");
                        handleClientDisconnect();
                        return;
                    }

                    LOGGER.debug("v0.3 SSE event sent successfully with id: {}", id);
                    subscription.request(1);
                } catch (Exception e) {
                    LOGGER.error("Error writing SSE event: {}", e.getMessage(), e);
                    onError(e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                LOGGER.debug("v0.3 SSE subscriber onError called: {}", throwable.getMessage(), throwable);
                handleClientDisconnect();
                streamingComplete.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                LOGGER.debug("v0.3 SSE subscriber onComplete called");
                try {
                    writer.close();
                } catch (Exception e) {
                    LOGGER.error("Error closing writer: {}", e.getMessage(), e);
                }
                streamingComplete.complete(null);
            }

            private void handleClientDisconnect() {
                LOGGER.debug("SSE connection closed, calling EventConsumer.cancel() to stop polling loop");
                if (subscription != null) {
                    subscription.cancel();
                }
                context.invokeEventConsumerCancelCallback();
                try {
                    writer.close();
                } catch (Exception e) {
                    LOGGER.debug("Error closing writer during disconnect: {}", e.getMessage());
                }
            }
        });

        try {
            streamingComplete.get();
        } catch (Exception e) {
            LOGGER.error("Error waiting for streaming completion: {}", e.getMessage(), e);
            throw new IOException("Streaming failed", e);
        }
    }

    private static String formatSseEventV03(Object data, long id) {
        return "data: " + Utils_v0_3.toJsonString(data) + "\nid: " + id + "\n\n";
    }

    // =======================================================================
    // Version resolution
    // =======================================================================

    private static String resolveVersion(HttpServletRequest request) {
        String version = request.getHeader(A2AHeaders.A2A_VERSION);
        if (version == null || version.isBlank()) {
            return VERSION_0_3;
        }
        return version.trim();
    }

    private static boolean isV10(String resolvedVersion) {
        return VERSION_1_0.equals(resolvedVersion);
    }

    private static boolean isV03(String resolvedVersion) {
        return VERSION_0_3.equals(resolvedVersion);
    }

    // =======================================================================
    // Call context creation
    // =======================================================================

    private ServerCallContext createV10CallContext(HttpServletRequest request, SecurityContext securityContext) {
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

        Map<String, String> headers = new HashMap<>();
        for (Enumeration<String> headerNames = request.getHeaderNames(); headerNames.hasMoreElements(); ) {
            String name = headerNames.nextElement();
            headers.put(name, request.getHeader(name));
        }

        state.put(HEADERS_KEY, headers);

        Enumeration<String> en = request.getHeaders(A2AHeaders.A2A_EXTENSIONS);
        List<String> extensionHeaderValues = new ArrayList<>();
        while (en.hasMoreElements()) {
            extensionHeaderValues.add(en.nextElement());
        }
        Set<String> requestedExtensions = A2AExtensions.getRequestedExtensions(extensionHeaderValues);
        state.put(TENANT_KEY, extractTenant(request));
        state.put(TRANSPORT_KEY, TransportProtocol.JSONRPC);

        String requestedVersion = request.getHeader(A2AHeaders.A2A_VERSION);
        return new ServerCallContext(user, state, requestedExtensions, requestedVersion);
    }

    private ServerCallContext createV03CallContext(HttpServletRequest request, SecurityContext securityContext) {
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

        Map<String, String> headers = new HashMap<>();
        for (Enumeration<String> headerNames = request.getHeaderNames(); headerNames.hasMoreElements(); ) {
            String name = headerNames.nextElement();
            headers.put(name, request.getHeader(name));
        }

        state.put(org.a2aproject.sdk.compat03.transport.jsonrpc.context.JSONRPCContextKeys_v0_3.HEADERS_KEY, headers);

        // Extract requested extensions from X-A2A-Extensions header
        Enumeration<String> en = request.getHeaders(A2AHeaders_v0_3.X_A2A_EXTENSIONS);
        List<String> extensionHeaderValues = new ArrayList<>();
        while (en.hasMoreElements()) {
            extensionHeaderValues.add(en.nextElement());
        }
        Set<String> requestedExtensions = A2AExtensions.getRequestedExtensions(extensionHeaderValues);

        return new ServerCallContext(user, state, requestedExtensions, A2AProtocol_v0_3.PROTOCOL_VERSION);
    }

    private static String extractTenant(HttpServletRequest request) {
        String tenantPath = request.getRequestURI();
        if (tenantPath == null || tenantPath.isBlank()) {
            return "";
        }
        if (tenantPath.startsWith("/")) {
            tenantPath = tenantPath.substring(1);
        }
        if (tenantPath.endsWith("/")) {
            tenantPath = tenantPath.substring(0, tenantPath.length() - 1);
        }
        return tenantPath;
    }

    // =======================================================================
    // Testing hook
    // =======================================================================

    public static void setStreamingIsSubscribedRunnable(Runnable streamingIsSubscribedRunnable) {
        MultiVersionA2AServerResource.streamingIsSubscribedRunnable = streamingIsSubscribedRunnable;
    }
}
