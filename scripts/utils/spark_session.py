"""
Configuración y creación de SparkSession
"""
from pyspark.sql import SparkSession
from utils.constants import SPARK_APP_NAME, SPARK_MASTER
import logging

def create_spark_session(app_name=None):
    """
    Crea y configura una SparkSession
    
    Args:
        app_name: Nombre de la aplicación Spark (opcional)
    
    Returns:
        SparkSession configurada
    """
    if app_name is None:
        app_name = SPARK_APP_NAME
    
    logging.info(f"Creando SparkSession: {app_name}")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(SPARK_MASTER) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    # Configurar nivel de log
    spark.sparkContext.setLogLevel("WARN")
    
    logging.info(f"✅ SparkSession creada - Version: {spark.version}")
    
    return spark

def stop_spark_session(spark):
    """
    Detiene la SparkSession de forma segura
    
    Args:
        spark: SparkSession a detener
    """
    if spark:
        logging.info("Deteniendo SparkSession...")
        spark.stop()
        logging.info("✅ SparkSession detenida correctamente")