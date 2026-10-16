/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.operator.cluster.model.KafkaBrokerConfigurationBuilder;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.ca.CertificateUtils;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Utility methods used by the KafkaRoller to reload custom listener certificates without restarting the brokers
 */
final class CustomListenerCertificateUtils {
    private static final Pattern KEYSTORE_CONFIG_PATTERN = Pattern.compile("listener\\.name\\.[^.]+\\.ssl\\.keystore\\.(certificate\\.chain|key)");

    private CustomListenerCertificateUtils() { }

    /**
     * Builds the configuration updates which make the broker reload its custom listener certificates. The values are
     * the same configuration provider placeholders as in the static broker configuration. Setting them makes the
     * broker resolve them again and load the current certificates from the per-node Secret.
     *
     * @param namespace     Namespace of the pod
     * @param podName       Name of the pod
     * @param listeners     Listeners with custom certificates
     *
     * @return  List of configuration updates
     */
    static List<AlterConfigOp> reloadOps(String namespace, String podName, List<CustomListenerCertificates.Listener> listeners) {
        List<AlterConfigOp> ops = new ArrayList<>();
        for (CustomListenerCertificates.Listener listener : listeners) {
            ops.add(new AlterConfigOp(new ConfigEntry(keystoreConfig(listener.name(), "certificate.chain"),
                    KafkaBrokerConfigurationBuilder.nodeSecretPlaceholder(namespace, podName, listener.name() + ".crt")), AlterConfigOp.OpType.SET));
            ops.add(new AlterConfigOp(new ConfigEntry(keystoreConfig(listener.name(), "key"),
                    KafkaBrokerConfigurationBuilder.nodeSecretPlaceholder(namespace, podName, listener.name() + ".key")), AlterConfigOp.OpType.SET));
        }
        return ops;
    }

    /**
     * Builds the configuration updates which remove the dynamic custom listener certificate configurations of
     * listeners which do not use custom certificates anymore.
     *
     * @param currentConfig     Current configuration of the broker
     * @param listeners         Listeners with custom certificates
     *
     * @return  List of configuration updates. Empty if there is nothing to remove.
     */
    static List<AlterConfigOp> staleConfigRemovalOps(Config currentConfig, List<CustomListenerCertificates.Listener> listeners) {
        Set<String> desired = new HashSet<>();
        for (CustomListenerCertificates.Listener listener : listeners) {
            desired.add(keystoreConfig(listener.name(), "certificate.chain"));
            desired.add(keystoreConfig(listener.name(), "key"));
        }

        return currentConfig.entries().stream()
                .filter(entry -> entry.source() == ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)
                .filter(entry -> KEYSTORE_CONFIG_PATTERN.matcher(entry.name()).matches())
                .filter(entry -> !desired.contains(entry.name()))
                .map(entry -> new AlterConfigOp(new ConfigEntry(entry.name(), null), AlterConfigOp.OpType.DELETE))
                .toList();
    }

    /**
     * Connects to a TLS listener and returns the short thumbprint of the certificate it serves. The certificate is
     * not validated because it is only compared with the desired certificate. The TLS handshake has to complete, which
     * proves that the listener holds the private key matching the served certificate.
     *
     * @param host          Hostname of the broker
     * @param port          Port of the listener
     * @param timeoutMs     Connection and read timeout in milliseconds
     *
     * @return  Short thumbprint of the served certificate
     *
     * @throws IOException                  If the TLS handshake with the listener fails
     * @throws GeneralSecurityException     If the TLS context cannot be initialized or the thumbprint computed
     */
    @SuppressFBWarnings(value = "WEAK_TRUST_MANAGER", justification = "Only the served certificate is read, no data is exchanged")
    static String servedCertificateThumbprint(String host, int port, int timeoutMs) throws IOException, GeneralSecurityException {
        TrustManager trustAllManager = new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                throw new CertificateException("Client certificates are not supported");
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {
                // The served certificate is compared with the desired certificate after the handshake
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, new TrustManager[] {trustAllManager}, null);

        try (SSLSocket socket = (SSLSocket) sslContext.getSocketFactory().createSocket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            socket.setSoTimeout(timeoutMs);
            socket.startHandshake();

            // With TLS 1.3, listeners with TLS client authentication reject the missing client certificate only after the handshake
            return CertificateUtils.getCertificateThumbprint((X509Certificate) socket.getSession().getPeerCertificates()[0]).substring(0, Util.HASH_STUB_LENGTH);
        }
    }

    private static String keystoreConfig(String listenerName, String option) {
        return "listener.name." + listenerName + ".ssl.keystore." + option;
    }
}
