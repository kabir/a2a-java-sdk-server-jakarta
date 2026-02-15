package org.wildfly.extras.a2a.test.server.grpc;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.api.AnnotationsProto;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.protobuf.util.JsonFormat;
import io.a2a.A2A;
import io.a2a.client.ClientBuilder;
import io.a2a.client.http.A2AHttpClient;
import io.a2a.client.transport.grpc.GrpcTransport;
import io.a2a.client.transport.grpc.GrpcTransportConfigBuilder;
import io.a2a.grpc.A2AServiceGrpc;
import io.a2a.grpc.utils.JSONRPCUtils;
import io.a2a.integrations.microprofile.MicroProfileConfigProvider;
import io.a2a.jsonrpc.common.json.JsonUtil;
import io.a2a.server.PublicAgentCard;
import io.a2a.server.apps.common.AbstractA2AServerTest;
import io.a2a.spec.Event;
import io.a2a.spec.TransportProtocol;
import io.a2a.transport.grpc.handler.GrpcHandler;
import io.a2a.util.Assert;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import mutiny.zero.ZeroPublisher;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.RunAsClient;
import org.jboss.arquillian.junit5.container.annotation.ArquillianTest;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterAll;
import org.wildfly.extras.a2a.server.apps.grpc.WildFlyGrpcHandler;

@ArquillianTest
@RunAsClient
public class WildFlyA2AGrpcTestCase extends AbstractA2AServerTest {

    private static ManagedChannel channel;

    public WildFlyA2AGrpcTestCase() {
        super(8080); // HTTP server port for utility endpoints
    }

    @Override
    protected String getTransportProtocol() {
        return TransportProtocol.GRPC.asString();
    }

    @Override
    protected String getTransportUrl() {
        // gRPC port (from WildFly gRPC configuration)
        return "localhost:9555";
    }

    @Override
    protected void configureTransport(ClientBuilder builder) {
        builder.withTransport(GrpcTransport.class, new GrpcTransportConfigBuilder().channelFactory(target -> {
            channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
            return channel;
        }));
    }

    @Deployment
    public static WebArchive createDeployment() throws Exception {
        final JavaArchive[] libraries = List.of(
                // a2a-java-sdk-jakarta-grpc.jar - contains WildFlyGrpcTransportMetadata
                getJarForClass(WildFlyGrpcHandler.class),
                // a2a-java-sdk-client.jar
                getJarForClass(A2A.class),
                // a2a-java-sdk-common.jar
                getJarForClass(Assert.class),
                // a2a-java-sdk-http-client
                getJarForClass(A2AHttpClient.class),
                // a2a-java-sdk-server-common.jar
                getJarForClass(PublicAgentCard.class),
                // a2a-java-sdk-spec.jar
                getJarForClass(Event.class),
                // a2a-java-sdk-spec-grpc.jar (contains JSONRPCUtils)
                getJarForClass(JSONRPCUtils.class),
                //a2a-java-transport-grpc.jar
                getJarForClass(GrpcHandler.class),
                // a2a-java-sdk-jsonrpc-common.jar
                getJarForClass(JsonUtil.class),
                // gson.jar (required by jsonrpc-common)
                getJarForClass(Gson.class),
                // protobuf-java.jar - include correct version to match gencode 4.31.1
                getJarForClass(com.google.protobuf.Message.class),
                // protobuf-java-util.jar (required by spec-grpc JSONRPCUtils)
                getJarForClass(JsonFormat.class),
                // proto-google-common-protos.jar (required by spec-grpc)
                getJarForClass(AnnotationsProto.class),
                // guava.jar (required by a2a-java dependencies)
                getJarForClass(ImmutableSet.class),
                //a2a-java-sdk-microprofile-config.jar (needed to configure a2a-java settings via MP Config)
                getJarForClass(MicroProfileConfigProvider.class),
                // a2a-java-spec-grpc.jar (contains generated gRPC classes)
                getJarForClass(A2AServiceGrpc.class), // Removing to avoid auto-registration by WildFly gRPC subsystem
                // mutiny-zero.jar. This is provided by some WildFly layers, but not always, and not in
                // the server provisioned by Glow when inspecting our war
                getJarForClass(ZeroPublisher.class)).toArray(new JavaArchive[0]);

        return ShrinkWrap.create(WebArchive.class, "ROOT.war")
                .addAsLibraries(libraries)
                // Extra dependencies needed by the tests
                .addPackage(AbstractA2AServerTest.class.getPackage())
                .addPackage(A2ATestResource.class.getPackage())
                .addClass(RestApplication.class)
                .addAsWebInfResource("WEB-INF/web.xml")
                .addAsWebInfResource("META-INF/beans.xml", "beans.xml")
                // Add test properties file for AgentCardProducer
                .addAsResource("a2a-requesthandler-test.properties");
    }

    static JavaArchive getJarForClass(Class<?> clazz) throws Exception {
        File f = new File(clazz.getProtectionDomain().getCodeSource().getLocation().toURI());
        return ShrinkWrap.createFromZipFile(JavaArchive.class, f);
    }

    @AfterAll
    public static void closeChannel() {
        channel.shutdownNow();
        try {
            channel.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
