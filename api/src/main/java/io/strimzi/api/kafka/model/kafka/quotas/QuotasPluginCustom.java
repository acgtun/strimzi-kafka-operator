/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.quotas;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Map;

@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "quotaCallbackClass", "kafkaAdminClientConfigPrefix", "config"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class QuotasPluginCustom extends QuotasPlugin {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_CUSTOM = "custom";

    private String quotaCallbackClass;
    private String kafkaAdminClientConfigPrefix;
    private Map<String, Object> config;

    @Description("Must be `" + TYPE_CUSTOM + "`")
    @Override
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getType() {
        return TYPE_CUSTOM;
    }

    @SuppressWarnings("checkstyle:NoFullyQualifiedClassNames") // False positive, fully qualified class name used in a string
    @Description("The fully qualified name of a class implementing the `org.apache.kafka.server.quota.ClientQuotaCallback` interface, " +
        "which must be available on the Kafka classpath. " +
        "It is set as the value of the `client.quota.callback.class` broker option.")
    public String getQuotaCallbackClass() {
        return quotaCallbackClass;
    }

    public void setQuotaCallbackClass(String quotaCallbackClass) {
        this.quotaCallbackClass = quotaCallbackClass;
    }

    @Description("When set, the operator generates the configuration of a TLS Kafka Admin client connecting to the Kafka cluster " +
        "(bootstrap address of the replication listener, security protocol, per-node PEM keystore, and the cluster CA truststore) " +
        "under options prefixed with this value, in the same way it does for the `strimzi` quotas plugin type. " +
        "For example, with prefix `com.example.quotas.kafka.admin`, the operator sets `com.example.quotas.kafka.admin.bootstrap.servers` and the related security options.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getKafkaAdminClientConfigPrefix() {
        return kafkaAdminClientConfigPrefix;
    }

    public void setKafkaAdminClientConfigPrefix(String kafkaAdminClientConfigPrefix) {
        this.kafkaAdminClientConfigPrefix = kafkaAdminClientConfigPrefix;
    }

    @Description("Configuration options of the custom quotas plugin, added verbatim to the Kafka node configuration. " +
        "Options managed by Strimzi itself and the `client.quota.callback.class` option cannot be set here.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }
}
