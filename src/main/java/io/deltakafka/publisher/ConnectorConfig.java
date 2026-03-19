package io.deltakafka.publisher;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Runtime configuration for the publisher, sourced from environment variables.
 *
 * <p>Required variables: {@code DELTA_TABLE_PATH}, {@code KAFKA_BOOTSTRAP_SERVERS}, and
 * {@code KAFKA_TOPIC}. Optional variables control row filtering, record limits, and Azure ABFS
 * OAuth settings.
 */
public final class ConnectorConfig {

    static final String DELTA_TABLE_PATH_ENV = "DELTA_TABLE_PATH";
    static final String KAFKA_BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVERS";
    static final String KAFKA_TOPIC_ENV = "KAFKA_TOPIC";
    static final String DELTA_FILTER_COLUMN_ENV = "DELTA_FILTER_COLUMN";
    static final String DELTA_FILTER_VALUE_ENV = "DELTA_FILTER_VALUE";
    static final String MAX_RECORDS_ENV = "MAX_RECORDS";
    static final String AZURE_STORAGE_ACCOUNT_ENV = "AZURE_STORAGE_ACCOUNT";
    static final String AZURE_CLIENT_ID_ENV = "AZURE_CLIENT_ID";
    static final String AZURE_CLIENT_SECRET_ENV = "AZURE_CLIENT_SECRET";
    static final String AZURE_TENANT_ID_ENV = "AZURE_TENANT_ID";

    private final String deltaTablePath;
    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private final String filterColumn;
    private final String filterValue;
    private final int maxRecords;
    private final String azureStorageAccount;
    private final String azureClientId;
    private final String azureClientSecret;
    private final String azureTenantId;

    private ConnectorConfig(
            String deltaTablePath,
            String kafkaBootstrapServers,
            String kafkaTopic,
            String filterColumn,
            String filterValue,
            int maxRecords,
            String azureStorageAccount,
            String azureClientId,
            String azureClientSecret,
            String azureTenantId) {
        this.deltaTablePath = Objects.requireNonNull(deltaTablePath, "deltaTablePath");
        this.kafkaBootstrapServers = Objects.requireNonNull(kafkaBootstrapServers, "kafkaBootstrapServers");
        this.kafkaTopic = Objects.requireNonNull(kafkaTopic, "kafkaTopic");
        this.filterColumn = filterColumn;
        this.filterValue = filterValue;
        this.maxRecords = maxRecords;
        this.azureStorageAccount = azureStorageAccount;
        this.azureClientId = azureClientId;
        this.azureClientSecret = azureClientSecret;
        this.azureTenantId = azureTenantId;
    }

    /**
     * Builds a configuration object from the current process environment.
     *
     * @return the validated runtime configuration
     */
    public static ConnectorConfig fromEnvironment() {
        return fromMap(System.getenv());
    }

    static ConnectorConfig fromMap(Map<String, String> values) {
        String deltaTablePath = required(values, DELTA_TABLE_PATH_ENV);
        String kafkaBootstrapServers = required(values, KAFKA_BOOTSTRAP_SERVERS_ENV);
        String kafkaTopic = required(values, KAFKA_TOPIC_ENV);
        String filterColumn = optional(values, DELTA_FILTER_COLUMN_ENV).orElse(null);
        String filterValue = optional(values, DELTA_FILTER_VALUE_ENV).orElse(null);
        int maxRecords = parsePositiveInt(optional(values, MAX_RECORDS_ENV).orElse("0"), MAX_RECORDS_ENV);
        String azureStorageAccount = optional(values, AZURE_STORAGE_ACCOUNT_ENV).orElse(null);
        String azureClientId = optional(values, AZURE_CLIENT_ID_ENV).orElse(null);
        String azureClientSecret = optional(values, AZURE_CLIENT_SECRET_ENV).orElse(null);
        String azureTenantId = optional(values, AZURE_TENANT_ID_ENV).orElse(null);

        if ((filterColumn == null) != (filterValue == null)) {
            throw new IllegalArgumentException(
                    DELTA_FILTER_COLUMN_ENV + " and " + DELTA_FILTER_VALUE_ENV + " must be provided together");
        }

        return new ConnectorConfig(
                deltaTablePath,
                kafkaBootstrapServers,
                kafkaTopic,
                filterColumn,
                filterValue,
                maxRecords,
                azureStorageAccount,
                azureClientId,
                azureClientSecret,
                azureTenantId);
    }

    public String getDeltaTablePath() {
        return deltaTablePath;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public Optional<String> getFilterColumn() {
        return Optional.ofNullable(filterColumn);
    }

    public Optional<String> getFilterValue() {
        return Optional.ofNullable(filterValue);
    }

    public int getMaxRecords() {
        return maxRecords;
    }

    public boolean hasFilter() {
        return filterColumn != null;
    }

    /**
     * Builds the Kafka producer properties used by this process.
     *
     * @return Kafka producer properties for string key/value publishing
     */
    public Properties buildKafkaProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    /**
     * Builds the Hadoop configuration used by Delta Kernel.
     *
     * <p>If Azure OAuth variables are present, all of them must be present and are applied as ABFS
     * client credential settings. If none are present, the default Hadoop configuration is used as-is.
     *
     * @return Hadoop configuration for reading the configured Delta table
     */
    public Configuration buildHadoopConfiguration() {
        Configuration configuration = new Configuration();

        boolean anyAzureSettingProvided =
                azureStorageAccount != null || azureClientId != null || azureClientSecret != null || azureTenantId != null;
        boolean allAzureSettingsProvided =
                azureStorageAccount != null && azureClientId != null && azureClientSecret != null && azureTenantId != null;

        if (!anyAzureSettingProvided) {
            return configuration;
        }

        if (!allAzureSettingsProvided) {
            throw new IllegalArgumentException(
                    "If one Azure OAuth setting is provided, all of "
                            + AZURE_STORAGE_ACCOUNT_ENV + ", "
                            + AZURE_CLIENT_ID_ENV + ", "
                            + AZURE_CLIENT_SECRET_ENV + ", and "
                            + AZURE_TENANT_ID_ENV + " must be set");
        }

        String endpointHost = azureStorageAccount + ".dfs.core.windows.net";
        configuration.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");
        configuration.set("fs.AbstractFileSystem.abfss.impl", "org.apache.hadoop.fs.azurebfs.Abfs");
        configuration.set("fs.azure.account.auth.type." + endpointHost, "OAuth");
        configuration.set(
                "fs.azure.account.oauth.provider.type." + endpointHost,
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
        configuration.set("fs.azure.account.oauth2.client.id." + endpointHost, azureClientId);
        configuration.set("fs.azure.account.oauth2.client.secret." + endpointHost, azureClientSecret);
        configuration.set(
                "fs.azure.account.oauth2.client.endpoint." + endpointHost,
                "https://login.microsoftonline.com/" + azureTenantId + "/oauth2/token");
        return configuration;
    }

    private static String required(Map<String, String> values, String key) {
        return optional(values, key)
                .orElseThrow(() -> new IllegalArgumentException("Missing required environment variable: " + key));
    }

    private static Optional<String> optional(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null) {
            return Optional.empty();
        }

        String trimmed = value.trim();
        return trimmed.isEmpty() ? Optional.empty() : Optional.of(trimmed);
    }

    private static int parsePositiveInt(String rawValue, String key) {
        try {
            int value = Integer.parseInt(rawValue);
            if (value < 0) {
                throw new IllegalArgumentException(key + " must be greater than or equal to 0");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer", e);
        }
    }
}
