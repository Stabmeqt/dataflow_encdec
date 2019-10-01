package com.epam.dataflow.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;

public class PoolProvider {

    private DataSource dataSource;
    private static final Object lock = new Object();
    private static PoolProvider instance = null;

    public static PoolProvider of(String driver, String connectionString, String user, String password, int minSize,
                                  int maxSize) {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = new PoolProvider(create(driver, connectionString, user, password, minSize, maxSize));
                }
            }
        }
        return instance;
    }

    private PoolProvider(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    private static DataSource create(String driver, String connectionString, String user, String password,
                                     int minSize, int maxSize) {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        try {
            dataSource.setDriverClass(driver);
        } catch (PropertyVetoException e) {
            throw new RuntimeException("Failed to set driver", e);
        }

        dataSource.setJdbcUrl(connectionString);
        dataSource.setUser(user);
        dataSource.setPassword(password);
        dataSource.setMaxPoolSize(maxSize);
        dataSource.setMinPoolSize(minSize);
        dataSource.setInitialPoolSize(minSize);

        return dataSource;
    }
}
