package io.deltakafka.publisher;

import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scans the latest Delta Lake snapshot and publishes each materialized row to Kafka as JSON.
 */
public final class DeltaTableKafkaPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaTableKafkaPublisher.class);

    private final ConnectorConfig config;
    private int recordsPublished;

    /**
     * Creates a publisher for a validated runtime configuration.
     *
     * @param config runtime configuration for Delta access and Kafka publishing
     */
    public DeltaTableKafkaPublisher(ConnectorConfig config) {
        this.config = config;
    }

    /**
     * Executes a single publish run against the configured Delta table.
     *
     * @throws Exception if Delta scanning or Kafka publishing fails
     */
    public void run() throws Exception {
        Properties kafkaProperties = config.buildKafkaProperties();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties)) {
            Engine engine = DefaultEngine.create(config.buildHadoopConfiguration());
            Table table = Table.forPath(engine, config.getDeltaTablePath());
            Snapshot snapshot = table.getLatestSnapshot(engine);
            Scan scan = buildScan(engine, snapshot);
            Row scanState = scan.getScanState(engine);
            StructType readSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState);
            Optional<Predicate> remainingFilter = scan.getRemainingFilter();

            LOG.info("Publishing Delta records from {}", config.getDeltaTablePath());
            config.getFilterColumn().ifPresent(
                    column -> LOG.info("Applying equality filter {}={}", column, config.getFilterValue().orElse("")));
            remainingFilter.ifPresent(filter -> LOG.info("Applying remaining Delta filter after scan planning"));
            processScan(engine, scan, scanState, readSchema, remainingFilter, producer);
            producer.flush();
            LOG.info("Published {} records to topic {}", recordsPublished, config.getKafkaTopic());
        }
    }

    private Scan buildScan(Engine engine, Snapshot snapshot) {
        ScanBuilder scanBuilder = snapshot.getScanBuilder(engine);

        if (config.hasFilter()) {
            Predicate predicate = new Predicate(
                    "=",
                    new Column(config.getFilterColumn().orElseThrow()),
                    Literal.ofString(config.getFilterValue().orElseThrow()));
            scanBuilder = scanBuilder.withFilter(engine, predicate);
        }

        return scanBuilder.build();
    }

    private void processScan(
            Engine engine,
            Scan scan,
            Row scanState,
            StructType readSchema,
            Optional<Predicate> remainingFilter,
            KafkaProducer<String, String> producer) {
        try (CloseableIterator<FilteredColumnarBatch> scanFileIterator = scan.getScanFiles(engine)) {
            while (scanFileIterator.hasNext() && !limitReached()) {
                FilteredColumnarBatch scanFilesBatch = scanFileIterator.next();
                processScanFilesBatch(engine, scanState, readSchema, remainingFilter, scanFilesBatch, producer);
            }
        } catch (IOException e) {
            throw new RuntimeException("I/O error while scanning Delta files", e);
        }
    }

    private void processScanFilesBatch(
            Engine engine,
            Row scanState,
            StructType readSchema,
            Optional<Predicate> remainingFilter,
            FilteredColumnarBatch scanFilesBatch,
            KafkaProducer<String, String> producer)
            throws IOException {
        try (CloseableIterator<Row> scanFileRows = scanFilesBatch.getRows()) {
            while (scanFileRows.hasNext() && !limitReached()) {
                Row scanFileRow = scanFileRows.next();
                processScanFile(engine, scanState, readSchema, remainingFilter, scanFileRow, producer);
            }
        }
    }

    private void processScanFile(
            Engine engine,
            Row scanState,
            StructType readSchema,
            Optional<Predicate> remainingFilter,
            Row scanFileRow,
            KafkaProducer<String, String> producer)
            throws IOException {
        FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);

        try (CloseableIterator<ColumnarBatch> parquetData = engine.getParquetHandler()
                        .readParquetFiles(singletonCloseable(fileStatus), readSchema, Optional.empty());
                CloseableIterator<FilteredColumnarBatch> transformedData =
                        Scan.transformPhysicalData(engine, scanState, scanFileRow, parquetData)) {
            processTransformedData(engine, transformedData, remainingFilter, producer);
        }
    }

    private void processTransformedData(
            Engine engine,
            CloseableIterator<FilteredColumnarBatch> transformedData,
            Optional<Predicate> remainingFilter,
            KafkaProducer<String, String> producer)
            throws IOException {
        PredicateEvaluator predicateEvaluator = null;

        while (transformedData.hasNext() && !limitReached()) {
            FilteredColumnarBatch filteredData = transformedData.next();
            if (remainingFilter.isPresent() && predicateEvaluator == null) {
                predicateEvaluator = engine.getExpressionHandler()
                        .getPredicateEvaluator(filteredData.getData().getSchema(), remainingFilter.orElseThrow());
            }
            processRows(applyRemainingFilter(predicateEvaluator, filteredData), producer);
        }
    }

    private static FilteredColumnarBatch applyRemainingFilter(
            PredicateEvaluator predicateEvaluator,
            FilteredColumnarBatch filteredData) {
        if (predicateEvaluator == null) {
            return filteredData;
        }

        return new FilteredColumnarBatch(
                filteredData.getData(),
                Optional.of(predicateEvaluator.eval(filteredData.getData(), filteredData.getSelectionVector())));
    }

    private void processRows(FilteredColumnarBatch filteredData, KafkaProducer<String, String> producer)
            throws IOException {
        try (CloseableIterator<Row> rowIterator = filteredData.getRows()) {
            while (rowIterator.hasNext() && !limitReached()) {
                Row row = rowIterator.next();
                String json = DeltaRowJsonSerializer.toJson(row);
                producer.send(new ProducerRecord<>(config.getKafkaTopic(), json));
                recordsPublished++;

                if (recordsPublished % 1000 == 0) {
                    LOG.info("Published {} records", recordsPublished);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize or publish a Delta row", e);
        }
    }

    private boolean limitReached() {
        return config.getMaxRecords() > 0 && recordsPublished >= config.getMaxRecords();
    }

    private static <T> CloseableIterator<T> singletonCloseable(T item) {
        Iterator<T> iterator = Collections.singleton(item).iterator();
        return new CloseableIterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return iterator.next();
            }

            @Override
            public void close() {
            }
        };
    }
}
