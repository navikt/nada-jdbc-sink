package no.nav.kafkaconnect.sink;

import no.nav.kafkaconnect.dialect.DatabaseDialect;
import no.nav.kafkaconnect.dialect.DatabaseDialects;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

public class NadaJdbcSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(NadaJdbcSinkTask.class);

    DatabaseDialect dialect;
    JdbcSinkConfig config;
    JdbcDbWriter writer;
    int remainingRetries;

    @Override
    public void start(final Map<String, String> props) {
        log.info("Starting JDBC Sink task");
        config = new JdbcSinkConfig(props);
        initWriter();
        remainingRetries = config.maxRetries;
    }

    void initWriter() {
        if (config.dialectName != null && !config.dialectName.trim().isEmpty()) {
            dialect = DatabaseDialects.create(config.dialectName, config);
        } else {
            dialect = DatabaseDialects.findBestFor(config.connectionUrl, config);
        }
        final DbStructure dbStructure = new DbStructure(dialect);
        log.info("Initializing writer using SQL dialect: {}", dialect.getClass().getSimpleName());
        writer = new JdbcDbWriter(config, dialect, dbStructure);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        log.debug(
                "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
                        + "database...",
                recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
        );
        try {
            writer.write(records);
        } catch (SQLException sqle) {
            log.warn(
                    "Write of {} records failed, remainingRetries={}",
                    records.size(),
                    remainingRetries,
                    sqle
            );
            String sqleAllMessages = "";
            for (Throwable e : sqle) {
                sqleAllMessages += e + System.lineSeparator();
            }
            if (remainingRetries == 0) {
                throw new ConnectException(new SQLException(sqleAllMessages));
            } else {
                writer.closeQuietly();
                initWriter();
                remainingRetries--;
                context.timeout(config.retryBackoffMs);
                throw new RetriableException(new SQLException(sqleAllMessages));
            }
        }
        remainingRetries = config.maxRetries;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // Not necessary
    }

    public void stop() {
        log.info("Stopping task");
        try {
            writer.closeQuietly();
        } finally {
            try {
                if (dialect != null) {
                    dialect.close();
                }
            } catch (Throwable t) {
                log.warn("Error while closing the {} dialect: ", dialect.name(), t);
            } finally {
                dialect = null;
            }
        }
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

}
