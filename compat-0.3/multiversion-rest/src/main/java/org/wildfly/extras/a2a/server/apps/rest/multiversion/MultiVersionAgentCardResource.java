package org.wildfly.extras.a2a.server.apps.rest.multiversion;

import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.a2aproject.sdk.transport.rest.handler.RestHandler;

@Path("/")
public class MultiVersionAgentCardResource {

    @Inject
    RestHandler v10Handler;

    @GET
    @Path(".well-known/agent-card.json")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getAgentCard() {
        RestHandler.HTTPRestResponse response = v10Handler.getAgentCard();

        String etag = "\"" + Integer.toHexString(response.getBody().hashCode()) + "\"";

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
