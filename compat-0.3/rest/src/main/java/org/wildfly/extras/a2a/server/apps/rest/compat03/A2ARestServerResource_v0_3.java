package org.wildfly.extras.a2a.server.apps.rest.compat03;

import static org.a2aproject.sdk.compat03.transport.rest.context.RestContextKeys_v0_3.HEADERS_KEY;
import static org.a2aproject.sdk.compat03.transport.rest.context.RestContextKeys_v0_3.METHOD_NAME_KEY;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;

import org.a2aproject.sdk.compat03.common.A2AHeaders_v0_3;
import org.a2aproject.sdk.compat03.conversion.A2AProtocol_v0_3;
import org.a2aproject.sdk.compat03.spec.CancelTaskRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.DeleteTaskPushNotificationConfigRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.GetAuthenticatedExtendedCardRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.GetTaskPushNotificationConfigRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.GetTaskRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.InternalError_v0_3;
import org.a2aproject.sdk.compat03.spec.InvalidParamsError_v0_3;
import org.a2aproject.sdk.compat03.spec.JSONRPCError_v0_3;
import org.a2aproject.sdk.compat03.spec.ListTaskPushNotificationConfigRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.SendMessageRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.SendStreamingMessageRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.SetTaskPushNotificationConfigRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.TaskResubscriptionRequest_v0_3;
import org.a2aproject.sdk.compat03.transport.rest.handler.RestHandler_v0_3;
import org.a2aproject.sdk.server.ServerCallContext;
import org.a2aproject.sdk.server.auth.UnauthenticatedUser;
import org.a2aproject.sdk.server.auth.User;
import org.a2aproject.sdk.server.extensions.A2AExtensions;
import org.wildfly.extras.a2a.server.apps.common.SSESubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1")
public class A2ARestServerResource_v0_3 {

    private static final Logger LOGGER = LoggerFactory.getLogger(A2ARestServerResource_v0_3.class);

    @Inject
    RestHandler_v0_3 jsonRestHandler;

    // Hook so testing can wait until the async Subscription is subscribed.
    private static volatile Runnable streamingIsSubscribedRunnable;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("message:send")
    public Response sendMessage(String body, @Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        ServerCallContext context = createCallContext(httpRequest, securityContext, SendMessageRequest_v0_3.METHOD);
        RestHandler_v0_3.HTTPRestResponse response = null;
        try {
            response = jsonRestHandler.sendMessage(body, context);
        } catch (JSONRPCError_v0_3 e) {
            response = jsonRestHandler.createErrorResponse(e);
        } catch (Throwable t) {
            response = jsonRestHandler.createErrorResponse(new InternalError_v0_3(t.getMessage()));
        } finally {
            return Response.status(response.getStatusCode())
                    .header(CONTENT_TYPE, response.getContentType())
                    .entity(response.getBody())
                    .build();
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.SERVER_SENT_EVENTS)
    @Path("message:stream")
    public void sendMessageStreaming(String body, @Context HttpServletRequest httpRequest, @Context HttpServletResponse httpResponse, @Context SecurityContext securityContext) throws IOException {
        ServerCallContext context = createCallContext(httpRequest, securityContext, SendStreamingMessageRequest_v0_3.METHOD);
        RestHandler_v0_3.HTTPRestStreamingResponse streamingResponse = null;
        RestHandler_v0_3.HTTPRestResponse error = null;
        try {
            RestHandler_v0_3.HTTPRestResponse response = jsonRestHandler.sendStreamingMessage(body, context);
            if (response instanceof RestHandler_v0_3.HTTPRestStreamingResponse hTTPRestStreamingResponse) {
                streamingResponse = hTTPRestStreamingResponse;
            } else {
                error = response;
            }
        } finally {
            if (error != null) {
                sendErrorResponse(httpResponse, error);
            } else {
                handleCustomSSEResponse(streamingResponse.getPublisher(), httpResponse, context);
            }
        }
    }

    @GET
    @Path("tasks/{taskId}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getTask(@PathParam("taskId") String taskId,
            @QueryParam("history_length") String historyLengthSnakeStr,
            @QueryParam("historyLength") String historyLengthCamelStr,
            @Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        ServerCallContext context = createCallContext(httpRequest, securityContext, GetTaskRequest_v0_3.METHOD);
        RestHandler_v0_3.HTTPRestResponse response = null;
        try {
            if (taskId == null || taskId.isEmpty()) {
                response = jsonRestHandler.createErrorResponse(new InvalidParamsError_v0_3("bad task id"));
            } else {
                boolean hasHistoryLength = historyLengthSnakeStr != null && !historyLengthSnakeStr.isEmpty();
                boolean hasHistoryLengthCamel = historyLengthCamelStr != null && !historyLengthCamelStr.isEmpty();

                if (hasHistoryLength && hasHistoryLengthCamel) {
                    response = jsonRestHandler.createErrorResponse(
                        new InvalidParamsError_v0_3("Only one of 'history_length' or 'historyLength' may be specified"));
                } else {
                    int historyLength = 0;
                    if (hasHistoryLength) {
                        historyLength = Integer.parseInt(historyLengthSnakeStr);
                    } else if (hasHistoryLengthCamel) {
                        historyLength = Integer.parseInt(historyLengthCamelStr);
                    }
                    response = jsonRestHandler.getTask(taskId, historyLength, context);
                }
            }
        } catch (NumberFormatException e) {
            response = jsonRestHandler.createErrorResponse(new InvalidParamsError_v0_3("bad history_length or historyLength"));
        } catch (JSONRPCError_v0_3 e) {
            response = jsonRestHandler.createErrorResponse(e);
        } catch (Throwable t) {
            response = jsonRestHandler.createErrorResponse(new InternalError_v0_3(t.getMessage()));
        } finally {
            return Response.status(response.getStatusCode())
                    .header(CONTENT_TYPE, response.getContentType())
                    .entity(response.getBody())
                    .build();
        }
    }

    @POST
    @Path("tasks/{taskId}:cancel")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response cancelTask(@PathParam("taskId") String taskId, @Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        ServerCallContext context = createCallContext(httpRequest, securityContext, CancelTaskRequest_v0_3.METHOD);
        RestHandler_v0_3.HTTPRestResponse response = null;
        try {
            if (taskId == null || taskId.isEmpty()) {
                response = jsonRestHandler.createErrorResponse(new InvalidParamsError_v0_3("bad task id"));
            } else {
                response = jsonRestHandler.cancelTask(taskId, context);
            }
        } catch (JSONRPCError_v0_3 e) {
            response = jsonRestHandler.createErrorResponse(e);
        } catch (Throwable t) {
            response = jsonRestHandler.createErrorResponse(new InternalError_v0_3(t.getMessage()));
        } finally {
            return Response.status(response.getStatusCode())
                    .header(CONTENT_TYPE, response.getContentType())
                    .entity(response.getBody())
                    .build();
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.SERVER_SENT_EVENTS)
    @Path("tasks/{taskId}:subscribe")
    public void resubscribeTask(@PathParam("taskId") String taskId, @Context HttpServletRequest httpRequest, @Context HttpServletResponse httpResponse, @Context SecurityContext securityContext) throws IOException {
        ServerCallContext context = createCallContext(httpRequest, securityContext, TaskResubscriptionRequest_v0_3.METHOD);
        RestHandler_v0_3.HTTPRestStreamingResponse streamingResponse = null;
        RestHandler_v0_3.HTTPRestResponse error = null;
        try {
            if (taskId == null || taskId.isEmpty()) {
                error = jsonRestHandler.createErrorResponse(new InvalidParamsError_v0_3("bad task id"));
            } else {
                RestHandler_v0_3.HTTPRestResponse response = jsonRestHandler.resubscribeTask(taskId, context);
                if (response instanceof RestHandler_v0_3.HTTPRestStreamingResponse hTTPRestStreamingResponse) {
                    streamingResponse = hTTPRestStreamingResponse;
                } else {
                    error = response;
                }
            }
        } finally {
            if (error != null) {
                sendErrorResponse(httpResponse, error);
            } else {
                handleCustomSSEResponse(streamingResponse.getPublisher(), httpResponse, context);
            }
        }
    }

    @GET
    @Path("card")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getAuthenticatedExtendedCard(@Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        ServerCallContext context = createCallContext(httpRequest, securityContext, GetAuthenticatedExtendedCardRequest_v0_3.METHOD);
        RestHandler_v0_3.HTTPRestResponse response = jsonRestHandler.getAuthenticatedExtendedCard();
        return Response.status(response.getStatusCode())
                .header(CONTENT_TYPE, response.getContentType())
                .entity(response.getBody())
                .build();
    }

    @POST
    @Path("tasks/{taskId}/pushNotificationConfigs")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response setTaskPushNotificationConfiguration(@PathParam("taskId") String taskId, String body, @Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        ServerCallContext context = createCallContext(httpRequest, securityContext, SetTaskPushNotificationConfigRequest_v0_3.METHOD);
        RestHandler_v0_3.HTTPRestResponse response = null;
        try {
            if (taskId == null || taskId.isEmpty()) {
                response = jsonRestHandler.createErrorResponse(new InvalidParamsError_v0_3("bad task id"));
            } else {
                response = jsonRestHandler.setTaskPushNotificationConfiguration(taskId, body, context);
            }
        } catch (JSONRPCError_v0_3 e) {
            response = jsonRestHandler.createErrorResponse(e);
        } catch (Throwable t) {
            response = jsonRestHandler.createErrorResponse(new InternalError_v0_3(t.getMessage()));
        } finally {
            return Response.status(response.getStatusCode())
                    .header(CONTENT_TYPE, response.getContentType())
                    .entity(response.getBody())
                    .build();
        }
    }

    @GET
    @Path("tasks/{taskId}/pushNotificationConfigs/{configId}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getTaskPushNotificationConfiguration(@PathParam("taskId") String taskId, @PathParam("configId") String configId, @Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        ServerCallContext context = createCallContext(httpRequest, securityContext, GetTaskPushNotificationConfigRequest_v0_3.METHOD);
        RestHandler_v0_3.HTTPRestResponse response = null;
        try {
            if (taskId == null || taskId.isEmpty()) {
                response = jsonRestHandler.createErrorResponse(new InvalidParamsError_v0_3("bad task id"));
            } else {
                response = jsonRestHandler.getTaskPushNotificationConfiguration(taskId, configId, context);
            }
        } catch (JSONRPCError_v0_3 e) {
            response = jsonRestHandler.createErrorResponse(e);
        } catch (Throwable t) {
            response = jsonRestHandler.createErrorResponse(new InternalError_v0_3(t.getMessage()));
        } finally {
            return Response.status(response.getStatusCode())
                    .header(CONTENT_TYPE, response.getContentType())
                    .entity(response.getBody())
                    .build();
        }
    }

    @GET
    @Path("tasks/{taskId}/pushNotificationConfigs")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response listTaskPushNotificationConfigurations(@PathParam("taskId") String taskId, @Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        ServerCallContext context = createCallContext(httpRequest, securityContext, ListTaskPushNotificationConfigRequest_v0_3.METHOD);
        RestHandler_v0_3.HTTPRestResponse response = null;
        try {
            if (taskId == null || taskId.isEmpty()) {
                response = jsonRestHandler.createErrorResponse(new InvalidParamsError_v0_3("bad task id"));
            } else {
                response = jsonRestHandler.listTaskPushNotificationConfigurations(taskId, context);
            }
        } catch (JSONRPCError_v0_3 e) {
            response = jsonRestHandler.createErrorResponse(e);
        } catch (Throwable t) {
            response = jsonRestHandler.createErrorResponse(new InternalError_v0_3(t.getMessage()));
        } finally {
            return Response.status(response.getStatusCode())
                    .header(CONTENT_TYPE, response.getContentType())
                    .entity(response.getBody())
                    .build();
        }
    }

    @DELETE
    @Path("tasks/{taskId}/pushNotificationConfigs/{configId}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response deleteTaskPushNotificationConfiguration(@PathParam("taskId") String taskId, @PathParam("configId") String configId, @Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        ServerCallContext context = createCallContext(httpRequest, securityContext, DeleteTaskPushNotificationConfigRequest_v0_3.METHOD);
        RestHandler_v0_3.HTTPRestResponse response = null;
        try {
            if (taskId == null || taskId.isEmpty()) {
                response = jsonRestHandler.createErrorResponse(new InvalidParamsError_v0_3("bad task id"));
            } else if (configId == null || configId.isEmpty()) {
                response = jsonRestHandler.createErrorResponse(new InvalidParamsError_v0_3("bad config id"));
            } else {
                response = jsonRestHandler.deleteTaskPushNotificationConfiguration(taskId, configId, context);
            }
        } catch (JSONRPCError_v0_3 e) {
            response = jsonRestHandler.createErrorResponse(e);
        } catch (Throwable t) {
            response = jsonRestHandler.createErrorResponse(new InternalError_v0_3(t.getMessage()));
        } finally {
            return Response.status(response.getStatusCode())
                    .header(CONTENT_TYPE, response.getContentType())
                    .entity(response.getBody())
                    .build();
        }
    }

    private void sendErrorResponse(HttpServletResponse httpResponse, RestHandler_v0_3.HTTPRestResponse error) throws IOException {
        httpResponse.setStatus(error.getStatusCode());
        httpResponse.setHeader(CONTENT_TYPE, error.getContentType());
        httpResponse.getWriter().write(error.getBody());
        httpResponse.getWriter().flush();
    }

    /**
     * Handles the streaming response using custom SSE formatting.
     * This approach avoids JAX-RS SSE compatibility issues with async publishers.
     * Implements proper client disconnect detection and EventConsumer cancellation.
     */
    private void handleCustomSSEResponse(Flow.Publisher<String> publisher,
            HttpServletResponse response,
            ServerCallContext context) throws IOException {
        response.setHeader(CONTENT_TYPE, MediaType.SERVER_SENT_EVENTS);
        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("X-Accel-Buffering", "no");

        CompletableFuture<Void> streamingComplete = new CompletableFuture<>();
        try (PrintWriter writer = response.getWriter()) {
            writer.write(": SSE stream started\n\n");
            writer.flush();
            publisher.subscribe(new SSESubscriber(streamingComplete, writer, context));
            streamingComplete.get();
        } catch (Exception e) {
            LOGGER.error("Error waiting for streaming completion: {}", e.getMessage(), e);
            throw new IOException("Streaming failed", e);
        }
    }

    public static void setStreamingIsSubscribedRunnable(Runnable streamingIsSubscribedRunnable) {
        A2ARestServerResource_v0_3.streamingIsSubscribedRunnable = streamingIsSubscribedRunnable;
        SSESubscriber.setStreamingIsSubscribedRunnable(streamingIsSubscribedRunnable);
    }

    private ServerCallContext createCallContext(HttpServletRequest request, SecurityContext securityContext, String jsonRpcMethodName) {
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
        for (Enumeration<String> headerNames = request.getHeaderNames(); headerNames.hasMoreElements();) {
            String name = headerNames.nextElement();
            headers.put(name, request.getHeader(name));
        }

        state.put(HEADERS_KEY, headers);
        state.put(METHOD_NAME_KEY, jsonRpcMethodName);

        // Extract requested extensions from X-A2A-Extensions header (v0.3 header)
        Enumeration<String> en = request.getHeaders(A2AHeaders_v0_3.X_A2A_EXTENSIONS);
        List<String> extensionHeaderValues = new ArrayList<>();
        while (en.hasMoreElements()) {
            extensionHeaderValues.add(en.nextElement());
        }
        Set<String> requestedExtensions = A2AExtensions.getRequestedExtensions(extensionHeaderValues);

        return new ServerCallContext(user, state, requestedExtensions, A2AProtocol_v0_3.PROTOCOL_VERSION);
    }
}
