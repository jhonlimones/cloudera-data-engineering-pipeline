"""
FASE 2: TRANSFORMACIÓN Y ENRIQUECIMIENTO DE DATOS
Feature engineering y preparación para análisis
"""
import logging
import sys
from datetime import datetime
from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth, hour, dayofweek,
    when, lag, unix_timestamp, count as spark_count
)
from pyspark.sql.window import Window

sys.path.append('..')
from utils import create_spark_session, stop_spark_session
from utils.constants import VALIDATED_DATA_PATH, PROCESSED_DATA_PATH, LOGS_DIR

# Configurar logging
log_file = LOGS_DIR / f"02_transform_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def extract_temporal_features(df):
    """Extrae características temporales del timestamp"""
    logging.info("Extrayendo características temporales...")
    
    # Convertir timestamp a datetime
    df = df.withColumn("timestamp_dt", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    
    # Extraer componentes temporales
    df = df.withColumn("year", year(col("timestamp_dt"))) \
           .withColumn("month", month(col("timestamp_dt"))) \
           .withColumn("day", dayofmonth(col("timestamp_dt"))) \
           .withColumn("hour", hour(col("timestamp_dt"))) \
           .withColumn("day_of_week", dayofweek(col("timestamp_dt")))
    
    # Clasificar franja horaria
    df = df.withColumn("time_slot",
        when((col("hour") >= 6) & (col("hour") < 12), "Mañana")
        .when((col("hour") >= 12) & (col("hour") < 18), "Tarde")
        .when((col("hour") >= 18) & (col("hour") < 23), "Noche")
        .otherwise("Madrugada")
    )
    
    # Clasificar día de semana vs fin de semana
    df = df.withColumn("is_weekend",
        when(col("day_of_week").isin([1, 7]), True).otherwise(False)
    )
    
    logging.info("✅ Características temporales extraídas")
    return df

def create_amount_categories(df):
    """Crea categorías de monto de transacción"""
    logging.info("Creando categorías de monto...")
    
    df = df.withColumn("amount_category",
        when(col("amount") < 50, "Bajo")
        .when((col("amount") >= 50) & (col("amount") < 200), "Medio")
        .when((col("amount") >= 200) & (col("amount") < 1000), "Alto")
        .otherwise("Muy Alto")
    )
    
    logging.info("✅ Categorías de monto creadas")
    return df

def calculate_velocity_features(df):
    """Calcula características de velocidad transaccional por cliente"""
    logging.info("Calculando features de velocidad transaccional...")
    
    # Ventana por cliente ordenada por timestamp
    window_spec = Window.partitionBy("customer_id").orderBy("timestamp_dt")
    
    # Calcular tiempo entre transacciones (en minutos)
    df = df.withColumn("prev_timestamp", lag("timestamp_dt").over(window_spec))
    
    df = df.withColumn("minutes_since_last_transaction",
        when(col("prev_timestamp").isNotNull(),
             (unix_timestamp("timestamp_dt") - unix_timestamp("prev_timestamp")) / 60
        ).otherwise(None)
    )
    
    # Contar transacciones por cliente en el dataset
    window_spec_unbounded = Window.partitionBy("customer_id")
    df = df.withColumn("customer_transaction_count",
        spark_count("*").over(window_spec_unbounded)
    )
    
    # Clasificar velocidad
    df = df.withColumn("transaction_velocity",
        when(col("minutes_since_last_transaction") < 5, "Muy Rápida")
        .when((col("minutes_since_last_transaction") >= 5) & 
              (col("minutes_since_last_transaction") < 30), "Rápida")
        .when((col("minutes_since_last_transaction") >= 30) & 
              (col("minutes_since_last_transaction") < 120), "Normal")
        .when(col("minutes_since_last_transaction") >= 120, "Lenta")
        .otherwise("Primera Transacción")
    )
    
    # Eliminar columna temporal
    df = df.drop("prev_timestamp")
    
    logging.info("✅ Features de velocidad calculadas")
    return df

def add_risk_indicators(df):
    """Añade indicadores de riesgo básicos"""
    logging.info("Añadiendo indicadores de riesgo...")
    
    # Transacciones nocturnas de alto monto
    df = df.withColumn("is_suspicious_night",
        when(
            (col("time_slot") == "Madrugada") & (col("amount") > 1000),
            True
        ).otherwise(False)
    )
    
    # Transacciones muy rápidas con monto alto
    df = df.withColumn("is_suspicious_velocity",
        when(
            (col("transaction_velocity") == "Muy Rápida") & (col("amount") > 500),
            True
        ).otherwise(False)
    )
    
    # Score de riesgo simple (0-10)
    df = df.withColumn("risk_score",
        (when(col("status") == "DECLINED", 2).otherwise(0)) +
        (when(col("is_suspicious_night"), 3).otherwise(0)) +
        (when(col("is_suspicious_velocity"), 3).otherwise(0)) +
        (when(col("amount") > 2000, 2).otherwise(0))
    )
    
    logging.info("✅ Indicadores de riesgo añadidos")
    return df

def main():
    """Función principal de transformación"""
    logging.info("=" * 70)
    logging.info("INICIANDO FASE 2: TRANSFORMACIÓN Y ENRIQUECIMIENTO")
    logging.info("=" * 70)
    
    spark = None
    
    try:
        spark = create_spark_session("02_TransformData")
        
        # Leer datos validados
        input_path = VALIDATED_DATA_PATH / "transactions_validated.parquet"
        logging.info(f"📂 Leyendo datos desde: {input_path}")
        
        df = spark.read.parquet(str(input_path))
        initial_count = df.count()
        logging.info(f"✅ Datos cargados: {initial_count} registros")
        
        # Aplicar transformaciones
        df = extract_temporal_features(df)
        df = create_amount_categories(df)
        df = calculate_velocity_features(df)
        df = add_risk_indicators(df)
        
        # Mostrar muestra transformada
        logging.info("\n📊 Muestra de datos transformados:")
        df.select(
            "transaction_id", "customer_id", "amount", "timestamp",
            "time_slot", "amount_category", "transaction_velocity", "risk_score"
        ).show(5, truncate=False)
        
        # Guardar particionado por año/mes/día
        output_path = PROCESSED_DATA_PATH
        logging.info(f"💾 Guardando datos procesados en: {output_path}")
        logging.info("   Particionando por: year, month, day")
        
        df.write.mode("overwrite") \
          .partitionBy("year", "month", "day") \
          .parquet(str(output_path))
        
        # Verificar escritura
        df_verify = spark.read.parquet(str(output_path))
        final_count = df_verify.count()
        
        logging.info("\n" + "=" * 70)
        logging.info("RESUMEN DE TRANSFORMACIÓN")
        logging.info("=" * 70)
        logging.info(f"Registros procesados: {final_count}")
        logging.info(f"Columnas originales: {len(df.columns) - 11}")
        logging.info(f"Columnas finales: {len(df.columns)}")
        logging.info(f"Features nuevas: 11")
        logging.info(f"Output: {output_path}")
        logging.info("=" * 70)
        
        logging.info("✅ FASE 2 COMPLETADA EXITOSAMENTE")
        
        return 0
        
    except Exception as e:
        logging.error(f"❌ Error en transformación: {str(e)}", exc_info=True)
        return 1
        
    finally:
        if spark:
            stop_spark_session(spark)

if __name__ == "__main__":
    sys.exit(main())