CREATE DATABASE restaurant_monitoring;

USE restaurant_monitoring;

CREATE TABLE store_status (
    id INT AUTO_INCREMENT PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL,
    timestamp_utc DATETIME NOT NULL,
    status VARCHAR(10) NOT NULL
);

CREATE TABLE business_hours (
    id INT AUTO_INCREMENT PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL,
    day_of_week INT NOT NULL,
    start_time_local TIME NOT NULL,
    end_time_local TIME NOT NULL
);

CREATE TABLE store_timezone (
    id INT AUTO_INCREMENT PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL,
    timezone_str VARCHAR(50) NOT NULL
);

CREATE TABLE reports (
    id INT AUTO_INCREMENT PRIMARY KEY,
    report_id VARCHAR(100) UNIQUE NOT NULL,
    status VARCHAR(10) NOT NULL DEFAULT 'Running',
    report_data LONGBLOB,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
