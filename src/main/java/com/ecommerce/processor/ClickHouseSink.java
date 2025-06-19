package com.ecommerce.processor;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ClickHouseSink<T> implements Sink<T>, Serializable {
    private final Properties config;
    private final ClickHouseRowConverter<T> rowConverter;
    private final int batchSize;
    private final long batchIntervalMs;
    private final int maxRetries;

    private ClickHouseSink(Builder<T> builder) {
        this.config = builder.config;
        this.rowConverter = builder.rowConverter;
        this.batchSize = builder.batchSize;
        this.batchIntervalMs = builder.batchIntervalMs;
        this.maxRetries = builder.maxRetries;
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) throws IOException {
        return new ClickHouseWriter(config, rowConverter, batchSize, batchIntervalMs, maxRetries);
    }

    // Builder 模式完整实现
    public static class Builder<T> {
        private final Properties config = new Properties();
        private ClickHouseRowConverter<T> rowConverter;
        private int batchSize = 1000;
        private long batchIntervalMs = 5000;
        private int maxRetries = 3;

        public Builder<T> setURL(String url) {
            config.setProperty("url", url);
            return this;
        }

        public Builder<T> setUsername(String username) {
            config.setProperty("username", username);
            return this;
        }

        public Builder<T> setPassword(String password) {
            config.setProperty("password", password);
            return this;
        }

        public Builder<T> setTableName(String tableName) {
            config.setProperty("tableName", tableName);
            return this;
        }

        public Builder<T> setBatchSize(int batchSize) {
            Preconditions.checkArgument(batchSize > 0, "Batch size must be positive");
            this.batchSize = batchSize;
            return this;
        }

        public Builder<T> setBatchIntervalMs(long batchIntervalMs) {
            Preconditions.checkArgument(batchIntervalMs > 0, "Batch interval must be positive");
            this.batchIntervalMs = batchIntervalMs;
            return this;
        }

        public Builder<T> setMaxRetries(int maxRetries) {
            Preconditions.checkArgument(maxRetries >= 0, "Max retries cannot be negative");
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder<T> setRowConverter(ClickHouseRowConverter<T> rowConverter) {
            this.rowConverter = Preconditions.checkNotNull(rowConverter, "Row converter cannot be null");
            return this;
        }

        public ClickHouseSink<T> build() {
            Preconditions.checkNotNull(config.getProperty("url"), "URL must be set");
            Preconditions.checkNotNull(config.getProperty("tableName"), "Table name must be set");
            Preconditions.checkNotNull(rowConverter, "Row converter must be set");
            return new ClickHouseSink<>(this);
        }
    }

    // SinkWriter 完整实现
    private class ClickHouseWriter implements SinkWriter<T> {
        private final Properties config;
        private final ClickHouseRowConverter<T> rowConverter;
        private final int batchSize;
        private final long batchIntervalMs;
        private final int maxRetries;
        private final List<Object[]> batch;
        private long lastFlushTime;

        private Connection connection;
        private PreparedStatement preparedStatement;

        public ClickHouseWriter(Properties config, ClickHouseRowConverter<T> rowConverter, int batchSize, long batchIntervalMs, int maxRetries) {
            this.config = config;
            this.rowConverter = rowConverter;
            this.batchSize = batchSize;
            this.batchIntervalMs = batchIntervalMs;
            this.maxRetries = maxRetries;
            this.batch = new ArrayList<>(batchSize);
            this.lastFlushTime = System.currentTimeMillis();

            initializeConnection();
        }

        private void initializeConnection() {
            try {
                Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
                this.connection = DriverManager.getConnection(config.getProperty("url"),
                        config.getProperty("username", ""),
                        config.getProperty("password", ""));
                this.connection.setAutoCommit(false);

                String tableName = config.getProperty("tableName");
                String placeholders = String.join(",", Collections.nCopies(rowConverter.getFieldCount(), "?"));
                String sql = String.format("INSERT INTO %s VALUES (%s)", tableName, placeholders);
                this.preparedStatement = connection.prepareStatement(sql);
            } catch (ClassNotFoundException | SQLException e) {
                throw new RuntimeException("Failed to initialize JDBC resources", e);
            }
        }

        @Override
        public void write(T element, Context context) throws IOException {
            try {
                Object[] row = rowConverter.convert(element, context);
                synchronized (batch) {
                    batch.add(row);
                    checkFlush();
                }
            } catch (Exception e) {
                throw new IOException("Failed to process element", e);
            }
        }

        @Override
        public void flush(boolean b) throws IOException {
            if (b) {
                flush();
            }

        }

        @Override
        public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
            SinkWriter.super.writeWatermark(watermark);
        }

        private void checkFlush() throws IOException {
            boolean sizeTrigger = batch.size() >= batchSize;
            boolean timeTrigger = System.currentTimeMillis() - lastFlushTime > batchIntervalMs;

            if (sizeTrigger || timeTrigger) {
                flush();
                lastFlushTime = System.currentTimeMillis();
            }
        }

        private void flush() throws IOException {
            if (batch.isEmpty()) return;

            int currentRetry = 0;
            while (currentRetry <= maxRetries) {
                try {
                    for (Object[] row : batch) {
                        for (int i = 0; i < row.length; i++) {
                            preparedStatement.setObject(i + 1, row[i]);
                        }
                        preparedStatement.addBatch();
                    }

                    preparedStatement.executeBatch();
                    connection.commit();
                    batch.clear();
                    return;
                } catch (SQLException e) {
                    try {
                        connection.rollback();
                    } catch (SQLException ex) {
                        throw new IOException("Rollback failed", ex);
                    }

                    if (currentRetry++ >= maxRetries) {
                        throw new IOException("Failed to flush after " + maxRetries + " retries", e);
                    }

                    try {
                        Thread.sleep(1000 * currentRetry);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Flush interrupted", ex);
                    }
                } finally {
                    try {
                        preparedStatement.clearBatch();
                    } catch (SQLException ex) {
                        // Ignore clear batch failure
                    }
                }
            }
        }

        @Override
        public void close() throws Exception {
            try {
                flush(); // 确保关闭前刷新剩余数据
            } finally {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        }
    }

    // 行转换器接口
    public interface ClickHouseRowConverter<T> {
        Object[] convert(T element, SinkWriter.Context context);

        int getFieldCount();
    }
}
