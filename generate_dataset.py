"""
Script para generar el dataset de transacciones bancarias
Ejecutar con: python generate_dataset.py

El archivo transactions.csv será generado con 2000 registros
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Configurar semilla
np.random.seed(42)
random.seed(42)

NUM_RECORDS = 2000
START_DATE = datetime(2025, 10, 1)
END_DATE = datetime(2025, 10, 14)

ACCOUNT_TYPES = ['Corriente', 'Ahorro', 'Empresarial', 'Premium']
ACCOUNT_WEIGHTS = [0.40, 0.30, 0.20, 0.10]

COUNTRIES = ['ES', 'FR', 'DE', 'UK', 'US', 'MX']
COUNTRY_WEIGHTS = [0.30, 0.20, 0.15, 0.15, 0.10, 0.10]

CURRENCIES = {'ES': 'EUR', 'FR': 'EUR', 'DE': 'EUR', 'UK': 'GBP', 'US': 'USD', 'MX': 'MXN'}

CATEGORIES = ['Supermercado', 'Restaurante', 'Gasolinera', 'Hotel', 'E-commerce', 
              'Transferencia', 'Cajero', 'Farmacia', 'Entretenimiento', 'Transporte']

CHANNELS = ['Online', 'Movil', 'Presencial', 'ATM']
CHANNEL_WEIGHTS = [0.45, 0.30, 0.20, 0.05]

def generate_timestamp():
    days = (END_DATE - START_DATE).days
    random_day = START_DATE + timedelta(days=random.randint(0, days))
    hour_weights = [0.01, 0.01, 0.01, 0.01, 0.01, 0.02, 0.03, 0.05, 0.08, 0.09, 0.08, 0.07,
                    0.09, 0.08, 0.07, 0.06, 0.05, 0.06, 0.07, 0.06, 0.04, 0.03, 0.02, 0.01]
    hour = random.choices(range(24), weights=hour_weights)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return random_day.replace(hour=hour, minute=minute, second=second)

def generate_amount(account_type, category):
    base_amounts = {
        'Supermercado': (10, 150), 'Restaurante': (15, 200), 'Gasolinera': (30, 100),
        'Hotel': (80, 500), 'E-commerce': (20, 300), 'Transferencia': (50, 2000),
        'Cajero': (20, 500), 'Farmacia': (10, 100), 'Entretenimiento': (15, 150),
        'Transporte': (5, 100)
    }
    min_amount, max_amount = base_amounts[category]
    multipliers = {'Corriente': 1.0, 'Ahorro': 0.8, 'Empresarial': 2.5, 'Premium': 3.0}
    amount = random.uniform(min_amount, max_amount) * multipliers[account_type]
    return round(amount, 2)

def determine_status(amount, timestamp, customer_id, transactions_so_far):
    hour = timestamp.hour
    if random.random() < 0.01:
        return 'FRAUD'
    if hour >= 23 or hour <= 5:
        if amount > 1000:
            return 'DECLINED' if random.random() < 0.7 else 'APPROVED'
    recent = [t for t in transactions_so_far if t['customer_id'] == customer_id]
    if len(recent) > 0:
        last_transaction = recent[-1]
        time_diff = (timestamp - last_transaction['timestamp']).total_seconds() / 60
        if time_diff < 5 and amount > 500:
            return 'DECLINED' if random.random() < 0.6 else 'APPROVED'
    if amount > 2000:
        return 'DECLINED' if random.random() < 0.3 else 'APPROVED'
    if random.random() < 0.02:
        return 'PENDING'
    if random.random() < 0.12:
        return 'DECLINED'
    return 'APPROVED'

# Generar datos
data = []
customer_ids = list(range(100, 700))

print("Generando dataset...")
for i in range(NUM_RECORDS):
    if (i + 1) % 500 == 0:
        print(f"Progreso: {i + 1}/{NUM_RECORDS}")
    
    transaction_id = 1000 + i
    customer_id = random.choice(customer_ids)
    account_type = random.choices(ACCOUNT_TYPES, weights=ACCOUNT_WEIGHTS)[0]
    country = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
    currency = CURRENCIES[country]
    category = random.choice(CATEGORIES)
    channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]
    timestamp = generate_timestamp()
    amount = generate_amount(account_type, category)
    status = determine_status(amount, timestamp, customer_id, data)
    merchant_id = f"M{random.randint(10000, 99999)}"
    card_last4 = f"{random.randint(1000, 9999)}"
    
    data.append({
        'transaction_id': transaction_id,
        'customer_id': customer_id,
        'account_type': account_type,
        'country': country,
        'amount': amount,
        'currency': currency,
        'category': category,
        'channel': channel,
        'timestamp': timestamp,
        'status': status,
        'merchant_id': merchant_id,
        'card_last4': card_last4
    })

df = pd.DataFrame(data)
df = df.sort_values('timestamp').reset_index(drop=True)
df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
df.to_csv('transactions.csv', index=False)

print(f"\n✅ Dataset generado: transactions.csv")
print(f"📊 Total: {len(df)} registros")
print(f"\nEstados: {df['status'].value_counts().to_dict()}")
print(f"Países: {df['country'].value_counts().to_dict()}")
print(f"Monto total: €{df['amount'].sum():,.2f}")
print(f"\nPrimeras 5 filas:\n{df.head()}")