package org.wildfly.extras.a2a.server.apps.rest.multiversion;

import static org.a2aproject.sdk.compat03.transport.rest.context.RestContextKeys_v0_3.HEADERS_KEY;
import static org.a2aproject.sdk.compat03.transport.rest.context.RestContextKeys_v0_3.METHOD_NAME_KEY;
import static org.a2aproject.sdk.server.ServerCallContext.TRANSPORT_KEY;
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

import org.a2aproject.sdk.common.A2AHeaders;
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
import org.a2aproject.sdk.spec.A2AError;
import org.a2aproject.sdk.spec.A2AErrorCodes;
import org.a2aproject.sdk.spec.InvalidParamsError;
import org.a2aproject.sdk.spec.TransportProtocol;
import org.a2aproject.sdk.spec.VersionNotSupportedError;
import org.a2aproject.sdk.transport.rest.handler.RestHandler;
import org.a2aproject.sdk.transport.rest.context.RestContextKeys;
import org.wildfly.extras.a2a.server.apps.common.SSESubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi-version JAX-RS resource that dispatches requests on {@code /v1/...} paths
 * to either v1.0 or v0.3 handlers based on the {@code A2A-Version} header.
 *
 * <p>The problem this solves: {@code /v1/message:send} is ambiguous -- it could be
 * v1.0 (with tenant="v1") or v0.3 (whose endpoints live under {@code /v1/}).
 * The {@code A2A-Version} header disambiguates:
 * <ul>
 *   <li>{@code A2A-Version: 1.0} -- dispatched to the v1.0 handler with tenant="v1"</li>
 *   <li>No header / {@code A2A-Version: 0.3} -- dispatched to the v0.3 handler</li>
 *   <li>Other -- returns a {@link VersionNotSupportedError}</li>
 * </ul>
 */
@Path("/v1")
public class MultiVersionA2ARestServerResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiVersionA2ARestServerResource.class);

    private static final String VERSION_1_0 = "1.0";
    private static final String VERSION_0_3 = "0.3";

    @Inject
    RestHandler v10Handler;

    @Inject
    RestHandler_v0_3 v03Handler;


    // Hook so testing can wait until the async Subscription is subscribed.
    private static volatile Runnable streamingIsSubscribedRunnable;

    // -------------------------------------------------------------------------
    // POST /v1/message:send
    // -------------------------------------------------------------------------

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("message:send")
    public Response sendMessage(String body, @Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        String version = resolveVersion(httpRequest);
        if (isV10(version)) {
            ServerCallContext context = createCallContextV10(httpRequest, securityContext);
            RestHandler.HTTPRestResponse response = null;
            try {
                response = v10Handler.sendMessage(context, "v1", body);
            } catch (A2AError e) {
                response = v10Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v10Handler.createErrorResponse(new org.a2aproject.sdk.spec.InternalError(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else if (isV03(version)) {
            ServerCallContext context = createCallContextV03(httpRequest, securityContext, SendMessageRequest_v0_3.METHOD);
            RestHandler_v0_3.HTTPRestResponse response = null;
            try {
                response = v03Handler.sendMessage(body, context);
            } catch (JSONRPCError_v0_3 e) {
                response = v03Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v03Handler.createErrorResponse(new InternalError_v0_3(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else {
            return createVersionNotSupportedResponse(version);
        }
    }

    // -------------------------------------------------------------------------
    // POST /v1/message:stream
    // -------------------------------------------------------------------------

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.SERVER_SENT_EVENTS)
    @Path("message:stream")
    public void sendMessageStreaming(String body, @Context HttpServletRequest httpRequest, @Context HttpServletResponse httpResponse, @Context SecurityContext securityContext) throws IOException {
        String version = resolveVersion(httpRequest);
        if (isV10(version)) {
            ServerCallContext context = createCallContextV10(httpRequest, securityContext);
            RestHandler.HTTPRestStreamingResponse streamingResponse = null;
            RestHandler.HTTPRestResponse error = null;
            try {
                RestHandler.HTTPRestResponse response = v10Handler.sendStreamingMessage(context, "v1", body);
                if (response instanceof RestHandler.HTTPRestStreamingResponse hTTPRestStreamingResponse) {
                    streamingResponse = hTTPRestStreamingResponse;
                } else {
                    error = response;
                }
            } finally {
                if (error != null) {
                    sendErrorResponse(httpResponse, error.getStatusCode(), error.getContentType(), error.getBody());
                } else {
                    handleCustomSSEResponse(streamingResponse.getPublisher(), httpResponse, context);
                }
            }
        } else if (isV03(version)) {
            ServerCallContext context = createCallContextV03(httpRequest, securityContext, SendStreamingMessageRequest_v0_3.METHOD);
            RestHandler_v0_3.HTTPRestStreamingResponse streamingResponse = null;
            RestHandler_v0_3.HTTPRestResponse error = null;
            try {
                RestHandler_v0_3.HTTPRestResponse response = v03Handler.sendStreamingMessage(body, context);
                if (response instanceof RestHandler_v0_3.HTTPRestStreamingResponse hTTPRestStreamingResponse) {
                    streamingResponse = hTTPRestStreamingResponse;
                } else {
                    error = response;
                }
            } finally {
                if (error != null) {
                    sendErrorResponse(httpResponse, error.getStatusCode(), error.getContentType(), error.getBody());
                } else {
                    handleCustomSSEResponse(streamingResponse.getPublisher(), httpResponse, context);
                }
            }
        } else {
            Response r = createVersionNotSupportedResponse(version);
            sendErrorResponse(httpResponse, r.getStatus(), "application/json", (String) r.getEntity());
        }
    }

    // -------------------------------------------------------------------------
    // GET /v1/tasks/{taskId}
    // -------------------------------------------------------------------------

    @GET
    @Path("tasks/{taskId}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getTask(@PathParam("taskId") String taskId,
            @QueryParam("historyLength") String historyLengthStr,
            @QueryParam("history_length") String historyLengthSnakeStr,
            @Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        String version = resolveVersion(httpRequest);
        if (isV10(version)) {
            ServerCallContext context = createCallContextV10(httpRequest, securityContext);
            RestHandler.HTTPRestResponse response = null;
            try {
                Integer historyLength = null;
                if (historyLengthStr != null && !historyLengthStr.isEmpty()) {
                    historyLength = Integer.valueOf(historyLengthStr);
                }
                response = v10Handler.getTask(context, "v1", taskId, historyLength);
            } catch (NumberFormatException e) {
                response = v10Handler.createErrorResponse(new org.a2aproject.sdk.spec.InvalidParamsError("bad historyLength"));
            } catch (A2AError e) {
                response = v10Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v10Handler.createErrorResponse(new org.a2aproject.sdk.spec.InternalError(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else if (isV03(version)) {
            ServerCallContext context = createCallContextV03(httpRequest, securityContext, GetTaskRequest_v0_3.METHOD);
            RestHandler_v0_3.HTTPRestResponse response = null;
            try {
                if (taskId == null || taskId.isEmpty()) {
                    response = v03Handler.createErrorResponse(new InvalidParamsError_v0_3("bad task id"));
                } else {
                    boolean hasHistoryLength = historyLengthSnakeStr != null && !historyLengthSnakeStr.isEmpty();
                    boolean hasHistoryLengthCamel = historyLengthStr != null && !historyLengthStr.isEmpty();

                    if (hasHistoryLength && hasHistoryLengthCamel) {
                        response = v03Handler.createErrorResponse(
                            new InvalidParamsError_v0_3("Only one of 'history_length' or 'historyLength' may be specified"));
                    } else {
                        int historyLength = 0;
                        if (hasHistoryLength) {
                            historyLength = Integer.parseInt(historyLengthSnakeStr);
                        } else if (hasHistoryLengthCamel) {
                            historyLength = Integer.parseInt(historyLengthStr);
                        }
                        response = v03Handler.getTask(taskId, historyLength, context);
                    }
                }
            } catch (NumberFormatException e) {
                response = v03Handler.createErrorResponse(new InvalidParamsError_v0_3("bad history_length or historyLength"));
            } catch (JSONRPCError_v0_3 e) {
                response = v03Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v03Handler.createErrorResponse(new InternalError_v0_3(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else {
            return createVersionNotSupportedResponse(version);
        }
    }

    // -------------------------------------------------------------------------
    // POST /v1/tasks/{taskId}:cancel
    // -------------------------------------------------------------------------

    @POST
    @Path("tasks/{taskId}:cancel")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response cancelTask(@PathParam("taskId") String taskId, String body, @Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        String version = resolveVersion(httpRequest);
        if (isV10(version)) {
            ServerCallContext context = createCallContextV10(httpRequest, securityContext);
            RestHandler.HTTPRestResponse response = null;
            try {
                response = v10Handler.cancelTask(context, "v1", body, taskId);
            } catch (A2AError e) {
                response = v10Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v10Handler.createErrorResponse(new org.a2aproject.sdk.spec.InternalError(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else if (isV03(version)) {
            ServerCallContext context = createCallContextV03(httpRequest, securityContext, CancelTaskRequest_v0_3.METHOD);
            RestHandler_v0_3.HTTPRestResponse response = null;
            try {
                if (taskId == null || taskId.isEmpty()) {
                    response = v03Handler.createErrorResponse(new InvalidParamsError_v0_3("bad task id"));
                } else {
                    response = v03Handler.cancelTask(taskId, context);
                }
            } catch (JSONRPCError_v0_3 e) {
                response = v03Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v03Handler.createErrorResponse(new InternalError_v0_3(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else {
            return createVersionNotSupportedResponse(version);
        }
    }

    // -------------------------------------------------------------------------
    // POST /v1/tasks/{taskId}:subscribe
    // -------------------------------------------------------------------------

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.SERVER_SENT_EVENTS)
    @Path("tasks/{taskId}:subscribe")
    public void resubscribeTask(@PathParam("taskId") String taskId, @Context HttpServletRequest httpRequest, @Context HttpServletResponse httpResponse, @Context SecurityContext securityContext) throws IOException {
        String version = resolveVersion(httpRequest);
        if (isV10(version)) {
            ServerCallContext context = createCallContextV10(httpRequest, securityContext);
            RestHandler.HTTPRestStreamingResponse streamingResponse = null;
            RestHandler.HTTPRestResponse error = null;
            try {
                RestHandler.HTTPRestResponse response = v10Handler.subscribeToTask(context, "v1", taskId);
                if (response instanceof RestHandler.HTTPRestStreamingResponse hTTPRestStreamingResponse) {
                    streamingResponse = hTTPRestStreamingResponse;
                } else {
                    error = response;
                }
            } finally {
                if (error != null) {
                    sendErrorResponse(httpResponse, error.getStatusCode(), error.getContentType(), error.getBody());
                } else {
                    handleCustomSSEResponse(streamingResponse.getPublisher(), httpResponse, context);
                }
            }
        } else if (isV03(version)) {
            ServerCallContext context = createCallContextV03(httpRequest, securityContext, TaskResubscriptionRequest_v0_3.METHOD);
            RestHandler_v0_3.HTTPRestStreamingResponse streamingResponse = null;
            RestHandler_v0_3.HTTPRestResponse error = null;
            try {
                if (taskId == null || taskId.isEmpty()) {
                    error = v03Handler.createErrorResponse(new InvalidParamsError_v0_3("bad task id"));
                } else {
                    RestHandler_v0_3.HTTPRestResponse response = v03Handler.resubscribeTask(taskId, context);
                    if (response instanceof RestHandler_v0_3.HTTPRestStreamingResponse hTTPRestStreamingResponse) {
                        streamingResponse = hTTPRestStreamingResponse;
                    } else {
                        error = response;
                    }
                }
            } finally {
                if (error != null) {
                    sendErrorResponse(httpResponse, error.getStatusCode(), error.getContentType(), error.getBody());
                } else {
                    handleCustomSSEResponse(streamingResponse.getPublisher(), httpResponse, context);
                }
            }
        } else {
            Response r = createVersionNotSupportedResponse(version);
            sendErrorResponse(httpResponse, r.getStatus(), "application/json", (String) r.getEntity());
        }
    }

    // -------------------------------------------------------------------------
    // POST /v1/tasks/{taskId}/pushNotificationConfigs
    // -------------------------------------------------------------------------

    @POST
    @Path("tasks/{taskId}/pushNotificationConfigs")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createTaskPushNotificationConfiguration(@PathParam("taskId") String taskId, String body, @Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        String version = resolveVersion(httpRequest);
        if (isV10(version)) {
            ServerCallContext context = createCallContextV10(httpRequest, securityContext);
            RestHandler.HTTPRestResponse response = null;
            try {
                response = v10Handler.createTaskPushNotificationConfiguration(context, "v1", body, taskId);
            } catch (A2AError e) {
                response = v10Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v10Handler.createErrorResponse(new org.a2aproject.sdk.spec.InternalError(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else if (isV03(version)) {
            ServerCallContext context = createCallContextV03(httpRequest, securityContext, SetTaskPushNotificationConfigRequest_v0_3.METHOD);
            RestHandler_v0_3.HTTPRestResponse response = null;
            try {
                if (taskId == null || taskId.isEmpty()) {
                    response = v03Handler.createErrorResponse(new InvalidParamsError_v0_3("bad task id"));
                } else {
                    response = v03Handler.setTaskPushNotificationConfiguration(taskId, body, context);
                }
            } catch (JSONRPCError_v0_3 e) {
                response = v03Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v03Handler.createErrorResponse(new InternalError_v0_3(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else {
            return createVersionNotSupportedResponse(version);
        }
    }

    // -------------------------------------------------------------------------
    // GET /v1/tasks/{taskId}/pushNotificationConfigs/{configId}
    // -------------------------------------------------------------------------

    @GET
    @Path("tasks/{taskId}/pushNotificationConfigs/{configId}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getTaskPushNotificationConfiguration(@PathParam("taskId") String taskId, @PathParam("configId") String configId, @Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        String version = resolveVersion(httpRequest);
        if (isV10(version)) {
            ServerCallContext context = createCallContextV10(httpRequest, securityContext);
            RestHandler.HTTPRestResponse response = null;
            try {
                response = v10Handler.getTaskPushNotificationConfiguration(context, "v1", taskId, configId);
            } catch (A2AError e) {
                response = v10Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v10Handler.createErrorResponse(new org.a2aproject.sdk.spec.InternalError(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else if (isV03(version)) {
            ServerCallContext context = createCallContextV03(httpRequest, securityContext, GetTaskPushNotificationConfigRequest_v0_3.METHOD);
            RestHandler_v0_3.HTTPRestResponse response = null;
            try {
                if (taskId == null || taskId.isEmpty()) {
                    response = v03Handler.createErrorResponse(new InvalidParamsError_v0_3("bad task id"));
                } else {
                    response = v03Handler.getTaskPushNotificationConfiguration(taskId, configId, context);
                }
            } catch (JSONRPCError_v0_3 e) {
                response = v03Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v03Handler.createErrorResponse(new InternalError_v0_3(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else {
            return createVersionNotSupportedResponse(version);
        }
    }

    // -------------------------------------------------------------------------
    // GET /v1/tasks/{taskId}/pushNotificationConfigs
    // -------------------------------------------------------------------------

    @GET
    @Path("tasks/{taskId}/pushNotificationConfigs")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response listTaskPushNotificationConfigurations(@PathParam("taskId") String taskId, @Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        String version = resolveVersion(httpRequest);
        if (isV10(version)) {
            ServerCallContext context = createCallContextV10(httpRequest, securityContext);
            RestHandler.HTTPRestResponse response = null;
            try {
                String pageSizeStr = httpRequest.getParameter("pageSize");
                int pageSize = 0;
                if (pageSizeStr != null && !pageSizeStr.isEmpty()) {
                    pageSize = Integer.parseInt(pageSizeStr);
                }
                String pageToken = "";
                if (httpRequest.getParameter("pageToken") != null) {
                    pageToken = httpRequest.getParameter("pageToken");
                }
                response = v10Handler.listTaskPushNotificationConfigurations(context, "v1", taskId, pageSize, pageToken);
            } catch (NumberFormatException e) {
                response = v10Handler.createErrorResponse(new org.a2aproject.sdk.spec.InvalidParamsError("bad pageSize"));
            } catch (A2AError e) {
                response = v10Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v10Handler.createErrorResponse(new org.a2aproject.sdk.spec.InternalError(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else if (isV03(version)) {
            ServerCallContext context = createCallContextV03(httpRequest, securityContext, ListTaskPushNotificationConfigRequest_v0_3.METHOD);
            RestHandler_v0_3.HTTPRestResponse response = null;
            try {
                if (taskId == null || taskId.isEmpty()) {
                    response = v03Handler.createErrorResponse(new InvalidParamsError_v0_3("bad task id"));
                } else {
                    response = v03Handler.listTaskPushNotificationConfigurations(taskId, context);
                }
            } catch (JSONRPCError_v0_3 e) {
                response = v03Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v03Handler.createErrorResponse(new InternalError_v0_3(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else {
            return createVersionNotSupportedResponse(version);
        }
    }

    // -------------------------------------------------------------------------
    // DELETE /v1/tasks/{taskId}/pushNotificationConfigs/{configId}
    // -------------------------------------------------------------------------

    @DELETE
    @Path("tasks/{taskId}/pushNotificationConfigs/{configId}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response deleteTaskPushNotificationConfiguration(@PathParam("taskId") String taskId, @PathParam("configId") String configId, @Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        String version = resolveVersion(httpRequest);
        if (isV10(version)) {
            ServerCallContext context = createCallContextV10(httpRequest, securityContext);
            RestHandler.HTTPRestResponse response = null;
            try {
                response = v10Handler.deleteTaskPushNotificationConfiguration(context, "v1", taskId, configId);
            } catch (A2AError e) {
                response = v10Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v10Handler.createErrorResponse(new org.a2aproject.sdk.spec.InternalError(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else if (isV03(version)) {
            ServerCallContext context = createCallContextV03(httpRequest, securityContext, DeleteTaskPushNotificationConfigRequest_v0_3.METHOD);
            RestHandler_v0_3.HTTPRestResponse response = null;
            try {
                if (taskId == null || taskId.isEmpty()) {
                    response = v03Handler.createErrorResponse(new InvalidParamsError_v0_3("bad task id"));
                } else if (configId == null || configId.isEmpty()) {
                    response = v03Handler.createErrorResponse(new InvalidParamsError_v0_3("bad config id"));
                } else {
                    response = v03Handler.deleteTaskPushNotificationConfiguration(taskId, configId, context);
                }
            } catch (JSONRPCError_v0_3 e) {
                response = v03Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v03Handler.createErrorResponse(new InternalError_v0_3(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else {
            return createVersionNotSupportedResponse(version);
        }
    }

    // -------------------------------------------------------------------------
    // GET /v1/card -- v0.3 only (v1.0 uses /{tenant}/extendedAgentCard)
    // -------------------------------------------------------------------------

    @GET
    @Path("card")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getAuthenticatedExtendedCard(@Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        String version = resolveVersion(httpRequest);
        if (isV10(version)) {
            // v1.0 does not use /v1/card; it uses /{tenant}/extendedAgentCard
            return createVersionNotSupportedResponse(version);
        } else if (isV03(version)) {
            ServerCallContext context = createCallContextV03(httpRequest, securityContext, GetAuthenticatedExtendedCardRequest_v0_3.METHOD);
            RestHandler_v0_3.HTTPRestResponse response = v03Handler.getAuthenticatedExtendedCard();
            return Response.status(response.getStatusCode())
                    .header(CONTENT_TYPE, response.getContentType())
                    .entity(response.getBody())
                    .build();
        } else {
            return createVersionNotSupportedResponse(version);
        }
    }

    // -------------------------------------------------------------------------
    // GET /v1/extendedAgentCard -- v1.0 only
    // -------------------------------------------------------------------------

    @GET
    @Path("extendedAgentCard")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getExtendedAgentCard(@Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        String version = resolveVersion(httpRequest);
        if (isV10(version)) {
            ServerCallContext context = createCallContextV10(httpRequest, securityContext);
            RestHandler.HTTPRestResponse response = v10Handler.getExtendedAgentCard(context, "v1");
            return Response.status(response.getStatusCode())
                    .header(CONTENT_TYPE, response.getContentType())
                    .entity(response.getBody())
                    .build();
        } else if (isV03(version)) {
            return createVersionNotSupportedResponse(version);
        } else {
            return createVersionNotSupportedResponse(version);
        }
    }

    // -------------------------------------------------------------------------
    // GET /v1/tasks -- v1.0 list tasks
    // -------------------------------------------------------------------------

    @GET
    @Path("tasks")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response listTasks(@Context HttpServletRequest httpRequest, @Context SecurityContext securityContext) {
        String version = resolveVersion(httpRequest);
        if (isV10(version)) {
            ServerCallContext context = createCallContextV10(httpRequest, securityContext);
            RestHandler.HTTPRestResponse response = null;
            try {
                String contextId = httpRequest.getParameter("contextId");
                String statusStr = httpRequest.getParameter("status");
                if (statusStr != null && !statusStr.isEmpty()) {
                    statusStr = statusStr.toUpperCase();
                }
                String pageSizeStr = httpRequest.getParameter("pageSize");
                String pageToken = httpRequest.getParameter("pageToken");
                String historyLengthStr = httpRequest.getParameter("historyLength");
                String statusTimestampAfter = httpRequest.getParameter("statusTimestampAfter");
                String includeArtifactsStr = httpRequest.getParameter("includeArtifacts");

                Integer pageSize = null;
                if (pageSizeStr != null && !pageSizeStr.isEmpty()) {
                    pageSize = Integer.valueOf(pageSizeStr);
                }

                Integer historyLength = null;
                if (historyLengthStr != null && !historyLengthStr.isEmpty()) {
                    historyLength = Integer.valueOf(historyLengthStr);
                }

                Boolean includeArtifacts = null;
                if (includeArtifactsStr != null && !includeArtifactsStr.isEmpty()) {
                    includeArtifacts = Boolean.valueOf(includeArtifactsStr);
                }

                response = v10Handler.listTasks(context, "v1", contextId, statusStr, pageSize,
                        pageToken, historyLength, statusTimestampAfter, includeArtifacts);
            } catch (NumberFormatException e) {
                response = v10Handler.createErrorResponse(new InvalidParamsError("Invalid number format in parameters"));
            } catch (IllegalArgumentException e) {
                response = v10Handler.createErrorResponse(new InvalidParamsError("Invalid parameter value: " + e.getMessage()));
            } catch (A2AError e) {
                response = v10Handler.createErrorResponse(e);
            } catch (Throwable t) {
                response = v10Handler.createErrorResponse(new org.a2aproject.sdk.spec.InternalError(t.getMessage()));
            } finally {
                return Response.status(response.getStatusCode())
                        .header(CONTENT_TYPE, response.getContentType())
                        .entity(response.getBody())
                        .build();
            }
        } else if (isV03(version)) {
            return createVersionNotSupportedResponse(version);
        } else {
            return createVersionNotSupportedResponse(version);
        }
    }

    // -------------------------------------------------------------------------
    // Version resolution helpers
    // -------------------------------------------------------------------------

    private String resolveVersion(HttpServletRequest request) {
        String version = request.getHeader(A2AHeaders.A2A_VERSION);
        if (version == null || version.isBlank()) {
            return VERSION_0_3;
        }
        return version.trim();
    }

    private static boolean isV10(String version) {
        return VERSION_1_0.equals(version);
    }

    private static boolean isV03(String version) {
        return VERSION_0_3.equals(version);
    }

    // -------------------------------------------------------------------------
    // Error response helpers
    // -------------------------------------------------------------------------

    private Response createVersionNotSupportedResponse(String version) {
        VersionNotSupportedError error = new VersionNotSupportedError(
                null,
                "Protocol version '" + version + "' is not supported. Supported versions: [1.0, 0.3]",
                null);
        A2AErrorCodes errorCode = A2AErrorCodes.fromCode(error.getCode());
        int httpStatus = errorCode != null ? errorCode.httpCode() : 400;
        String body = "{\"error\":{\"code\":" + error.getCode() + ",\"message\":\"" + error.getMessage() + "\"}}";
        return Response.status(httpStatus)
                .header(CONTENT_TYPE, "application/json")
                .entity(body)
                .build();
    }

    private void sendErrorResponse(HttpServletResponse httpResponse, int statusCode, String contentType, String body) throws IOException {
        httpResponse.setStatus(statusCode);
        httpResponse.setHeader(CONTENT_TYPE, contentType);
        httpResponse.getWriter().write(body);
        httpResponse.getWriter().flush();
    }

    // -------------------------------------------------------------------------
    // SSE streaming
    // -------------------------------------------------------------------------

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
        MultiVersionA2ARestServerResource.streamingIsSubscribedRunnable = streamingIsSubscribedRunnable;
        SSESubscriber.setStreamingIsSubscribedRunnable(streamingIsSubscribedRunnable);
    }

    // -------------------------------------------------------------------------
    // Call context creation
    // -------------------------------------------------------------------------

    private ServerCallContext createCallContextV10(HttpServletRequest request, SecurityContext securityContext) {
        User user = createUser(request, securityContext);
        Map<String, Object> state = new HashMap<>();

        Map<String, String> headers = extractHeaders(request);
        state.put(RestContextKeys.HEADERS_KEY, headers);
        state.put(RestContextKeys.TENANT_KEY, "v1");
        state.put(TRANSPORT_KEY, TransportProtocol.HTTP_JSON);

        Enumeration<String> en = request.getHeaders(A2AHeaders.A2A_EXTENSIONS);
        List<String> extensionHeaderValues = new ArrayList<>();
        while (en.hasMoreElements()) {
            extensionHeaderValues.add(en.nextElement());
        }
        Set<String> requestedExtensions = A2AExtensions.getRequestedExtensions(extensionHeaderValues);
        String requestedVersion = request.getHeader(A2AHeaders.A2A_VERSION);
        return new ServerCallContext(user, state, requestedExtensions, requestedVersion);
    }

    private ServerCallContext createCallContextV03(HttpServletRequest request, SecurityContext securityContext, String jsonRpcMethodName) {
        User user = createUser(request, securityContext);
        Map<String, Object> state = new HashMap<>();

        Map<String, String> headers = extractHeaders(request);
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

    private User createUser(HttpServletRequest request, SecurityContext securityContext) {
        if (securityContext.getUserPrincipal() == null) {
            return UnauthenticatedUser.INSTANCE;
        }
        return new User() {
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

    private Map<String, String> extractHeaders(HttpServletRequest request) {
        Map<String, String> headers = new HashMap<>();
        for (Enumeration<String> headerNames = request.getHeaderNames(); headerNames.hasMoreElements();) {
            String name = headerNames.nextElement();
            headers.put(name, request.getHeader(name));
        }
        return headers;
    }
}
