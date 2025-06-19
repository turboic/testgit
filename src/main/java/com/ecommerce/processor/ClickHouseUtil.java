package com.ecommerce.processor;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;

public class ClickHouseUtil implements Serializable {
    // 定义写入语义（根据 ClickHouse 支持情况简化）
    public enum WriteSemantic {
        AT_LEAST_ONCE,
        BEST_EFFORT
        // ClickHouse 不原生支持 Exactly Once，需结合 Flink 检查点实现
    }

    // 工具方法：构建带重试的 JDBC 连接
    public static Connection createRetryableConnection(String url, String username, String password, int maxRetries) {
        for (int i = 0; i < maxRetries; i++) {
            try {
                return DriverManager.getConnection(url, username, password);
            } catch (Exception e) {
                if (i == maxRetries - 1) {
                    throw new RuntimeException("Failed to connect after " + maxRetries + " retries", e);
                }
                try {
                    Thread.sleep(1000 * (i + 1)); // 指数退避
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        throw new IllegalStateException("Unreachable code");
    }
}
