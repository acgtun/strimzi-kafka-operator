/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

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
}
