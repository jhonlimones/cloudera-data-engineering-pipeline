"""
FASE 4: CREACIÓN DE TABLAS HIVE/IMPALA
Simula la creación de tablas externas en Hive apuntando a Parquet
"""
import logging
import sys
from datetime import datetime

sys.path.append('..')
from utils import create_spark_session, stop_spark_session
from utils.constants import PROCESSED_DATA_PATH, FRAUD_ALERTS_PATH, LOGS_DIR

# Configurar logging
log_file = LOGS_DIR / f"04_hive_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def create_transactions_table(spark, data_path):
    """Crea tabla externa de transacciones procesadas"""
    logging.info("Creando tabla: transactions_processed")
    
    # Leer parquet para obtener el esquema
    df = spark.read.parquet(str(data_path))
    
    # Registrar como tabla temporal (simula tabla Hive)
    df.createOrReplaceTempView("transactions_processed")
    
    # Mostrar DDL que se usaría en Hive real
    ddl = f"""
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
    LOCATION '{data_path}';
    
    -- Recuperar particiones
    MSCK REPAIR TABLE transactions_processed;
    """
    
    logging.info("📝 DDL de referencia generado")
    logging.info(f"✅ Tabla transactions_processed creada con {df.count()} registros")
    
    return ddl

def create_fraud_alerts_table(spark, alerts_path):
    """Crea tabla de alertas de fraude"""
    logging.info("Creando tabla: fraud_alerts")
    
    alerts_file = alerts_path / "fraud_alerts.parquet"
    
    if not alerts_file.exists():
        logging.warning("⚠️ No se encontraron alertas de fraude. Tabla no creada.")
        return None
    
    df_alerts = spark.read.parquet(str(alerts_file))
    df_alerts.createOrReplaceTempView("fraud_alerts")
    
    ddl = f"""
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
    LOCATION '{alerts_file}';
    """
    
    logging.info(f"✅ Tabla fraud_alerts creada con {df_alerts.count()} registros")
    
    return ddl

def verify_tables(spark):
    """Verifica que las tablas estén accesibles"""
    logging.info("\n" + "=" * 70)
    logging.info("VERIFICANDO TABLAS CREADAS")
    logging.info("=" * 70)
    
    # Listar tablas
    tables = spark.catalog.listTables()
    logging.info(f"\nTablas disponibles: {len(tables)}")
    for table in tables:
        logging.info(f"  - {table.name} ({table.tableType})")
    
    # Verificar transactions_processed
    if spark.catalog.tableExists("transactions_processed"):
        count = spark.sql("SELECT COUNT(*) as total FROM transactions_processed").collect()[0]['total']
        logging.info(f"\n✅ transactions_processed: {count} registros")
        
        # Mostrar esquema
        logging.info("\nEsquema de transactions_processed:")
        spark.sql("DESCRIBE transactions_processed").show(30, truncate=False)
    
    # Verificar fraud_alerts
    if spark.catalog.tableExists("fraud_alerts"):
        count = spark.sql("SELECT COUNT(*) as total FROM fraud_alerts").collect()[0]['total']
        logging.info(f"\n✅ fraud_alerts: {count} registros")
    
    logging.info("=" * 70)

def main():
    """Función principal de creación de tablas"""
    logging.info("=" * 70)
    logging.info("INICIANDO FASE 4: CREACIÓN DE TABLAS HIVE/IMPALA")
    logging.info("=" * 70)
    
    spark = None
    
    try:
        spark = create_spark_session("04_CreateHiveTables")
        
        # Crear tabla de transacciones
        ddl_transactions = create_transactions_table(spark, PROCESSED_DATA_PATH)
        
        # Crear tabla de alertas
        ddl_alerts = create_fraud_alerts_table(spark, FRAUD_ALERTS_PATH)
        
        # Verificar tablas
        verify_tables(spark)
        
        # Guardar DDLs en archivo SQL
        sql_output = LOGS_DIR.parent / "sql" / "create_tables.sql"
        sql_output.parent.mkdir(parents=True, exist_ok=True)
        
        with open(sql_output, 'w') as f:
            f.write("-- ================================================\n")
            f.write("-- DDL para Hive/Impala - Referencia\n")
            f.write("-- Generado automáticamente\n")
            f.write("-- ================================================\n\n")
            f.write(ddl_transactions)
            if ddl_alerts:
                f.write("\n\n")
                f.write(ddl_alerts)
        
        logging.info(f"\n💾 DDL guardado en: {sql_output}")
        
        logging.info("\n" + "=" * 70)
        logging.info("RESUMEN")
        logging.info("=" * 70)
        logging.info("Tablas creadas en Spark SQL (simula Hive):")
        logging.info("  ✅ transactions_processed")
        if ddl_alerts:
            logging.info("  ✅ fraud_alerts")
        logging.info(f"\nDDL de referencia: {sql_output}")
        logging.info("=" * 70)
        
        logging.info("✅ FASE 4 COMPLETADA EXITOSAMENTE")
        
        return 0
        
    except Exception as e:
        logging.error(f"❌ Error creando tablas: {str(e)}", exc_info=True)
        return 1
        
    finally:
        if spark:
            stop_spark_session(spark)

if __name__ == "__main__":
    sys.exit(main())