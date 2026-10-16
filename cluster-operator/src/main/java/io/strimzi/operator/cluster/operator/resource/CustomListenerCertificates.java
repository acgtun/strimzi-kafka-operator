/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.util.List;

/**
 * Desired state of the custom listener certificates used by the KafkaRoller to reload them without restarting the
 * Kafka brokers.
 *
 * @param reloadEnabled     Indicates whether the certificates can be reloaded dynamically
 * @param listeners         Listeners using custom certificates
 * @param thumbprints       Desired value of the custom listener certificate thumbprints annotation
 */
public record CustomListenerCertificates(boolean reloadEnabled, List<Listener> listeners, String thumbprints) {
    /**
     * Listener using a custom certificate
     *
     * @param name          Name of the listener as used in the Kafka configuration (e.g. external-9094)
     * @param port          Port of the listener
     * @param thumbprint    Short thumbprint of the desired certificate
     */
    public record Listener(String name, int port, String thumbprint) { }
}
