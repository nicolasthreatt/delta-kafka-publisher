package io.deltakafka.publisher;

/**
 * CLI entrypoint for the Delta-to-Kafka publisher.
 */
public final class Main {

    private Main() {
    }

    /**
     * Loads runtime configuration from environment variables and starts the publisher.
     *
     * @param args ignored command-line arguments
     * @throws Exception if configuration loading, Delta scanning, or Kafka publishing fails
     */
    public static void main(String[] args) throws Exception {
        ConnectorConfig config = ConnectorConfig.fromEnvironment();
        new DeltaTableKafkaPublisher(config).run();
    }
}
