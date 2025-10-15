"""
FASE 1: INGESTA Y VALIDACIÓN DE DATOS
Simula la lectura desde HDFS y validación inicial
"""
import logging
import sys
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, trim

# Configurar paths
sys.path.append('..')
from utils import create_spark_session, stop_spark_session, generate_validation_report
from utils.constants import RAW_TRANSACTIONS_FILE, VALIDATED_DATA_PATH, LOGS_DIR

# Configurar logging
log_file = LOGS_DIR / f"01_ingest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def define_schema():
    """Define el esquema explícito para el CSV"""
    return StructType([
        StructField("transaction_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("account_type", StringType(), False),
        StructField("country", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("category", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("status", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("card_last4", StringType(), False)
    ])

def clean_data(df):
    """Aplica limpieza básica a los datos"""
    logging.info("Aplicando limpieza de datos...")
    
    # Trim espacios en strings
    string_columns = [field.name for field in df.schema.fields 
                     if isinstance(field.dataType, StringType)]
    
    for column in string_columns:
        df = df.withColumn(column, trim(col(column)))
    
    # Eliminar duplicados por transaction_id
    initial_count = df.count()
    df = df.dropDuplicates(['transaction_id'])
    final_count = df.count()
    
    if initial_count != final_count:
        logging.warning(f"⚠️ Se eliminaron {initial_count - final_count} duplicados")
    
    # Eliminar registros con valores nulos en campos críticos
    critical_fields = ['transaction_id', 'customer_id', 'amount', 'timestamp', 'status']
    df = df.na.drop(subset=critical_fields)
    
    logging.info(f"✅ Limpieza completada. Registros finales: {df.count()}")
    
    return df

def main():
    """Función principal de ingesta"""
    logging.info("=" * 70)
    logging.info("INICIANDO FASE 1: INGESTA Y VALIDACIÓN DE DATOS")
    logging.info("=" * 70)
    
    spark = None
    
    try:
        # Crear SparkSession
        spark = create_spark_session("01_IngestData")
        
        # Verificar archivo de entrada
        if not RAW_TRANSACTIONS_FILE.exists():
            raise FileNotFoundError(f"❌ Archivo no encontrado: {RAW_TRANSACTIONS_FILE}")
        
        logging.info(f"📂 Leyendo datos desde: {RAW_TRANSACTIONS_FILE}")
        
        # Leer CSV con esquema explícito
        schema = define_schema()
        df = spark.read.csv(
            str(RAW_TRANSACTIONS_FILE),
            header=True,
            schema=schema
        )
        
        logging.info(f"✅ Datos cargados. Total registros: {df.count()}")
        
        # Mostrar muestra de datos
        logging.info("\n📊 Muestra de datos (primeras 5 filas):")
        df.show(5, truncate=False)
        
        # Validar datos
        validation_report = generate_validation_report(df)
        
        # Limpiar datos
        df_clean = clean_data(df)
        
        # Guardar en formato Parquet (simulando escritura a HDFS)
        output_path = VALIDATED_DATA_PATH / "transactions_validated.parquet"
        logging.info(f"💾 Guardando datos validados en: {output_path}")
        
        df_clean.write.mode("overwrite").parquet(str(output_path))
        
        # Verificar escritura
        df_verify = spark.read.parquet(str(output_path))
        logging.info(f"✅ Verificación: {df_verify.count()} registros guardados correctamente")
        
        # Resumen final
        logging.info("\n" + "=" * 70)
        logging.info("RESUMEN DE INGESTA")
        logging.info("=" * 70)
        logging.info(f"Registros originales: {validation_report['total_records']}")
        logging.info(f"Registros validados: {df_clean.count()}")
        logging.info(f"Duplicados eliminados: {validation_report['duplicates']}")
        logging.info(f"Output: {output_path}")
        logging.info("=" * 70)
        
        logging.info("✅ FASE 1 COMPLETADA EXITOSAMENTE")
        
        return 0
        
    except Exception as e:
        logging.error(f"❌ Error en la ingesta: {str(e)}", exc_info=True)
        return 1
        
    finally:
        if spark:
            stop_spark_session(spark)

if __name__ == "__main__":
    sys.exit(main())