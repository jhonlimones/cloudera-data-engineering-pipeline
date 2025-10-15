-- ================================================
-- DDL para Hive/Impala - Referencia
-- Generado automáticamente
-- ================================================


    -- DDL para Hive/Impala (referencia)
    CREATE EXTERNAL TABLE IF NOT EXISTS transactions_processed (
        transaction_id INT,
        customer_id INT,
        account_type STRING,
        country STRING,
        amount DOUBLE,
        currency STRING,
        category STRING,
        channel STRING,
        timestamp STRING,
        status STRING,
        merchant_id STRING,
        card_last4 STRING,
        timestamp_dt TIMESTAMP,
        hour INT,
        day_of_week INT,
        time_slot STRING,
        is_weekend BOOLEAN,
        amount_category STRING,
        minutes_since_last_transaction DOUBLE,
        customer_transaction_count BIGINT,
        transaction_velocity STRING,
        is_suspicious_night BOOLEAN,
        is_suspicious_velocity BOOLEAN,
        risk_score INT
    )
    PARTITIONED BY (year INT, month INT, day INT)
    STORED AS PARQUET
    LOCATION '/home/jhon/Escritorio/proyectos/mini-proyecto-cloudera/data/processed';
    
    -- Recuperar particiones
    MSCK REPAIR TABLE transactions_processed;
    


    -- DDL para tabla de alertas
    CREATE EXTERNAL TABLE IF NOT EXISTS fraud_alerts (
        transaction_id INT,
        customer_id INT,
        account_type STRING,
        country STRING,
        amount DOUBLE,
        currency STRING,
        category STRING,
        channel STRING,
        timestamp STRING,
        status STRING,
        alert_priority STRING,
        risk_score INT,
        fraud_reasons STRING,
        fraud_rule_status BOOLEAN,
        fraud_rule_high_declined BOOLEAN,
        fraud_rule_night BOOLEAN,
        fraud_rule_velocity BOOLEAN,
        is_fraud_detected BOOLEAN
    )
    STORED AS PARQUET
    LOCATION '/home/jhon/Escritorio/proyectos/mini-proyecto-cloudera/data/results/fraud_alerts/fraud_alerts.parquet';
    