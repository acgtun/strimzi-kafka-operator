/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.ca.CertificateUtils;
import io.strimzi.operator.common.operator.MockCertIssuer;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CustomListenerCertificateUtilsTest {
    private static final List<CustomListenerCertificates.Listener> LISTENERS = List.of(
            new CustomListenerCertificates.Listener("tls-9093", 9093, "aaaaaaaa"),
            new CustomListenerCertificates.Listener("external-9094", 9094, "bbbbbbbb"));

    @Test
    public void testReloadOps() {
        List<AlterConfigOp> ops = CustomListenerCertificateUtils.reloadOps("my-ns", "my-cluster-brokers-0", LISTENERS);

        assertThat(ops.stream().map(op -> op.opType() + " " + op.configEntry().name() + "=" + op.configEntry().value()).toList(), is(List.of(
                "SET listener.name.tls-9093.ssl.keystore.certificate.chain=${strimzisecrets:my-ns/my-cluster-brokers-0:tls-9093.crt}",
                "SET listener.name.tls-9093.ssl.keystore.key=${strimzisecrets:my-ns/my-cluster-brokers-0:tls-9093.key}",
                "SET listener.name.external-9094.ssl.keystore.certificate.chain=${strimzisecrets:my-ns/my-cluster-brokers-0:external-9094.crt}",
                "SET listener.name.external-9094.ssl.keystore.key=${strimzisecrets:my-ns/my-cluster-brokers-0:external-9094.key}")));
    }

    @Test
    public void testStaleConfigRemovalOps() {
        Config config = new Config(List.of(
                dynamicBrokerConfig("listener.name.tls-9093.ssl.keystore.certificate.chain"),
                dynamicBrokerConfig("listener.name.tls-9093.ssl.keystore.key"),
                dynamicBrokerConfig("listener.name.old-9095.ssl.keystore.certificate.chain"),
                dynamicBrokerConfig("listener.name.old-9095.ssl.keystore.key"),
                dynamicBrokerConfig("listener.name.old-9095.ssl.keystore.type"),
                dynamicBrokerConfig("num.io.threads"),
                new ConfigEntry("listener.name.static-9096.ssl.keystore.key", null, ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG, true, false, List.of(), null, null)));

        List<AlterConfigOp> ops = CustomListenerCertificateUtils.staleConfigRemovalOps(config, LISTENERS);

        assertThat(ops.stream().map(op -> op.opType() + " " + op.configEntry().name()).toList(), containsInAnyOrder(
                "DELETE listener.name.old-9095.ssl.keystore.certificate.chain",
                "DELETE listener.name.old-9095.ssl.keystore.key"));
    }

    @Test
    public void testStaleConfigRemovalOpsWithNothingToRemove() {
        Config config = new Config(List.of(
                dynamicBrokerConfig("listener.name.tls-9093.ssl.keystore.certificate.chain"),
                dynamicBrokerConfig("listener.name.tls-9093.ssl.keystore.key")));

        assertThat(CustomListenerCertificateUtils.staleConfigRemovalOps(config, LISTENERS), is(List.of()));
    }

    @Test
    public void testServedCertificateThumbprint() throws Exception {
        try (TlsServer server = new TlsServer(MockCertIssuer.clusterCaKey(), false)) {
            assertThat(CustomListenerCertificateUtils.servedCertificateThumbprint("localhost", server.port(), 5_000), is(clusterCaCertThumbprint()));
        }
    }

    @Test
    public void testServedCertificateThumbprintWithTlsClientAuthentication() throws Exception {
        try (TlsServer server = new TlsServer(MockCertIssuer.clusterCaKey(), true)) {
            assertThat(CustomListenerCertificateUtils.servedCertificateThumbprint("localhost", server.port(), 5_000), is(clusterCaCertThumbprint()));
        }
    }

    @Test
    public void testServedCertificateThumbprintWithMismatchedKey() throws Exception {
        try (TlsServer server = new TlsServer(MockCertIssuer.clientsCaKey(), false)) {
            assertThrows(SSLHandshakeException.class, () -> CustomListenerCertificateUtils.servedCertificateThumbprint("localhost", server.port(), 5_000));
        }
    }

    @Test
    public void testServedCertificateThumbprintWithUnreachableListener() throws IOException {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }

        assertThrows(IOException.class, () -> CustomListenerCertificateUtils.servedCertificateThumbprint("localhost", port, 1_000));
    }

    private static ConfigEntry dynamicBrokerConfig(String name) {
        return new ConfigEntry(name, null, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG, true, false, List.of(), null, null);
    }

    private static String clusterCaCertThumbprint() throws GeneralSecurityException {
        return CertificateUtils.getCertificateThumbprint(CertificateUtils.x509Certificate(Util.decodeBytesFromBase64(MockCertIssuer.clusterCaCert())))
                .substring(0, Util.HASH_STUB_LENGTH);
    }

    /**
     * TLS server serving the cluster CA certificate with the given private key
     */
    private static class TlsServer implements AutoCloseable {
        private final SSLServerSocket serverSocket;
        private final Thread thread;

        TlsServer(String key, boolean clientAuthentication) throws GeneralSecurityException, IOException {
            Secret secret = new SecretBuilder()
                    .withNewMetadata()
                        .withName("listener-certificate")
                        .withNamespace("my-ns")
                    .endMetadata()
                    .withData(Map.of("tls.crt", MockCertIssuer.clusterCaCert(), "tls.key", key))
                    .build();

            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(PemAuthIdentity.entityOperator(secret, "tls.key", "tls.crt").keyStore(), null);
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), null, null);

            serverSocket = (SSLServerSocket) sslContext.getServerSocketFactory().createServerSocket(0);
            serverSocket.setNeedClientAuth(clientAuthentication);
            thread = new Thread(this::serve);
            thread.start();
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        private void serve() {
            while (!serverSocket.isClosed()) {
                try (SSLSocket socket = (SSLSocket) serverSocket.accept()) {
                    socket.startHandshake();
                    socket.getInputStream().read();
                } catch (IOException e) {
                    // The client closed the connection or the handshake failed
                }
            }
        }

        @Override
        public void close() throws Exception {
            serverSocket.close();
            thread.join();
        }
    }
}
