"""
Funciones de validación de datos
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan
import logging
from utils.constants import (
    VALID_ACCOUNT_TYPES, VALID_COUNTRIES, VALID_CURRENCIES,
    VALID_STATUSES, VALID_CHANNELS, VALID_CATEGORIES
)

def validate_schema(df: DataFrame, expected_columns: list) -> bool:
    """
    Valida que el DataFrame tenga las columnas esperadas
    """
    df_columns = set(df.columns)
    expected_columns_set = set(expected_columns)
    
    missing = expected_columns_set - df_columns
    extra = df_columns - expected_columns_set
    
    if missing:
        logging.error(f"❌ Columnas faltantes: {missing}")
        return False
    
    if extra:
        logging.warning(f"⚠️ Columnas extra: {extra}")
    
    logging.info("✅ Esquema validado correctamente")
    return True

def check_nulls(df: DataFrame) -> dict:
    """
    Cuenta valores nulos por columna
    """
    null_counts = {}
    
    for column in df.columns:
        null_count = df.filter(
            col(column).isNull() | isnan(col(column)) | (col(column) == "")
        ).count()
        
        if null_count > 0:
            null_counts[column] = null_count
            logging.warning(f"⚠️ Columna '{column}' tiene {null_count} valores nulos")
    
    if not null_counts:
        logging.info("✅ No se encontraron valores nulos")
    
    return null_counts

def check_duplicates(df: DataFrame, key_column: str) -> int:
    """
    Cuenta registros duplicados basados en una columna clave
    """
    total = df.count()
    distinct = df.select(key_column).distinct().count()
    duplicates = total - distinct
    
    if duplicates > 0:
        logging.warning(f"⚠️ Se encontraron {duplicates} duplicados en '{key_column}'")
    else:
        logging.info(f"✅ No hay duplicados en '{key_column}'")
    
    return duplicates

def validate_categorical_values(df: DataFrame) -> dict:
    """
    Valida que los valores categóricos estén dentro de los esperados
    """
    validations = {
        'account_type': VALID_ACCOUNT_TYPES,
        'country': VALID_COUNTRIES,
        'currency': VALID_CURRENCIES,
        'status': VALID_STATUSES,
        'channel': VALID_CHANNELS,
        'category': VALID_CATEGORIES
    }
    
    invalid_records = {}
    
    for column, valid_values in validations.items():
        invalid_count = df.filter(~col(column).isin(valid_values)).count()
        
        if invalid_count > 0:
            invalid_records[column] = invalid_count
            logging.warning(
                f"⚠️ Columna '{column}': {invalid_count} valores inválidos"
            )
    
    if not invalid_records:
        logging.info("✅ Todos los valores categóricos son válidos")
    
    return invalid_records

def validate_numeric_ranges(df: DataFrame) -> dict:
    """
    Valida rangos numéricos razonables
    """
    issues = {}
    
    # Validar montos
    negative_amounts = df.filter(col('amount') < 0).count()
    if negative_amounts > 0:
        issues['negative_amounts'] = negative_amounts
        logging.warning(f"⚠️ {negative_amounts} transacciones con monto negativo")
    
    zero_amounts = df.filter(col('amount') == 0).count()
    if zero_amounts > 0:
        issues['zero_amounts'] = zero_amounts
        logging.warning(f"⚠️ {zero_amounts} transacciones con monto cero")
    
    # Validar customer_id y transaction_id
    invalid_customer_ids = df.filter(col('customer_id') < 0).count()
    if invalid_customer_ids > 0:
        issues['invalid_customer_ids'] = invalid_customer_ids
        logging.warning(f"⚠️ {invalid_customer_ids} customer_id inválidos")
    
    if not issues:
        logging.info("✅ Rangos numéricos válidos")
    
    return issues

def generate_validation_report(df: DataFrame) -> dict:
    """
    Genera un reporte completo de validación
    """
    logging.info("=" * 60)
    logging.info("INICIANDO VALIDACIÓN DE DATOS")
    logging.info("=" * 60)
    
    report = {
        'total_records': df.count(),
        'schema_valid': validate_schema(
            df, 
            ['transaction_id', 'customer_id', 'account_type', 'country', 
             'amount', 'currency', 'category', 'channel', 'timestamp', 
             'status', 'merchant_id', 'card_last4']
        ),
        'null_counts': check_nulls(df),
        'duplicates': check_duplicates(df, 'transaction_id'),
        'invalid_categorical': validate_categorical_values(df),
        'numeric_issues': validate_numeric_ranges(df)
    }
    
    logging.info("=" * 60)
    logging.info("VALIDACIÓN COMPLETADA")
    logging.info("=" * 60)
    
    return report