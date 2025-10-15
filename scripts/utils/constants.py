"""
Constantes y configuraciones del proyecto
"""
import os
from pathlib import Path

# Rutas base del proyecto
BASE_DIR = Path(__file__).parent.parent.parent.absolute()
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
SQL_DIR = BASE_DIR / "sql"

# Rutas de datos
RAW_DATA_PATH = DATA_DIR / "raw"
VALIDATED_DATA_PATH = DATA_DIR / "validated"
PROCESSED_DATA_PATH = DATA_DIR / "processed"
RESULTS_PATH = DATA_DIR / "results"

# Rutas específicas de resultados
FRAUD_ALERTS_PATH = RESULTS_PATH / "fraud_alerts"
ANALYTICS_PATH = RESULTS_PATH / "analytics"
REPORTS_PATH = RESULTS_PATH / "reports"

# Archivos
RAW_TRANSACTIONS_FILE = RAW_DATA_PATH / "transactions.csv"

# Configuración Spark
SPARK_APP_NAME = "BankTransactionsAnalysis"
SPARK_MASTER = "local[*]"  # Usar todos los cores disponibles

# Esquema esperado del CSV
EXPECTED_SCHEMA = {
    'transaction_id': 'int',
    'customer_id': 'int',
    'account_type': 'string',
    'country': 'string',
    'amount': 'double',
    'currency': 'string',
    'category': 'string',
    'channel': 'string',
    'timestamp': 'string',
    'status': 'string',
    'merchant_id': 'string',
    'card_last4': 'string'
}

# Valores válidos para validación
VALID_ACCOUNT_TYPES = ['Corriente', 'Ahorro', 'Empresarial', 'Premium']
VALID_COUNTRIES = ['ES', 'FR', 'DE', 'UK', 'US', 'MX']
VALID_CURRENCIES = ['EUR', 'USD', 'GBP', 'MXN']
VALID_STATUSES = ['APPROVED', 'DECLINED', 'PENDING', 'FRAUD']
VALID_CHANNELS = ['Online', 'Movil', 'Presencial', 'ATM']
VALID_CATEGORIES = [
    'Supermercado', 'Restaurante', 'Gasolinera', 'Hotel',
    'E-commerce', 'Transferencia', 'Cajero', 'Farmacia',
    'Entretenimiento', 'Transporte'
]

# Umbrales para detección de fraude
FRAUD_THRESHOLDS = {
    'high_amount': 2000.0,
    'night_hours': (23, 5),
    'velocity_minutes': 5,
    'velocity_amount': 500.0,
    'suspicious_night_amount': 1000.0
}

# Configuración de particionado
PARTITION_COLUMNS = ['year', 'month', 'day']

# Crear directorios si no existen
for path in [DATA_DIR, LOGS_DIR, RAW_DATA_PATH, VALIDATED_DATA_PATH, 
             PROCESSED_DATA_PATH, RESULTS_PATH, FRAUD_ALERTS_PATH, 
             ANALYTICS_PATH, REPORTS_PATH]:
    path.mkdir(parents=True, exist_ok=True)

print(f"✅ Constantes cargadas. Base dir: {BASE_DIR}")