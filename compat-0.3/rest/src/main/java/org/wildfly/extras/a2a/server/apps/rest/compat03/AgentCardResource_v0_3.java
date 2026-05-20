package org.wildfly.extras.a2a.server.apps.rest.compat03;

import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.a2aproject.sdk.compat03.transport.rest.handler.RestHandler_v0_3;

@Path("/.well-known/agent-card.json")
public class AgentCardResource_v0_3 {

    @Inject
    RestHandler_v0_3 jsonRestHandler;

    /**
     * Handles incoming GET requests to the v0.3 agent card endpoint.
     * Returns the agent card in JSON format with appropriate caching headers.
     *
     * @return the agent card with caching headers
     */
    @GET
    public Response getAgentCard() {
        RestHandler_v0_3.HTTPRestResponse response = jsonRestHandler.getAgentCard();

        // Generate ETag based on response body content hash
        String etag = "\"" + Integer.toHexString(response.getBody().hashCode()) + "\"";

        // Set Last-Modified to current time in RFC 1123 format
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("GMT"));
        String lastModified = now.format(DateTimeFormatter.RFC_1123_DATE_TIME);

        return Response.status(response.getStatusCode())
                .header(CONTENT_TYPE, response.getContentType())
                .header("Cache-Control", "max-age=3600")
                .header("ETag", etag)
                .header("Last-Modified", lastModified)
                .entity(response.getBody())
                .build();
    }
}
