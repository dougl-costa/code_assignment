from json import loads
from kafka import KafkaConsumer
import sqlite3

# Configuração do banco de dados
conn = sqlite3.connect('iot_sensors.db')
cursor = conn.cursor()

# Criar tabela se não existir
cursor.execute('''
CREATE TABLE IF NOT EXISTS leituras_sensores (
    id TEXT PRIMARY KEY,
    tipo TEXT,
    valor REAL,
    timestamp TEXT,
    latitude REAL,
    longitude REAL
)
''')
conn.commit()

# Configuração do consumidor Kafka
consumidor = KafkaConsumer(
    'iot_sensor_data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

def processar_dados(dados):
    cursor.execute('''
    INSERT INTO leituras_sensores 
    (id, tipo, valor, timestamp, latitude, longitude)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        dados['sensor_id'],
        dados['tipo'],
        dados['valor'],
        dados['timestamp'],
        dados['localizacao']['latitude'],
        dados['localizacao']['longitude']
    ))
    conn.commit()
    print(f"Dados armazenados: {dados['sensor_id']}")

if __name__ == '__main__':
    try:
        for mensagem in consumidor:
            dados = mensagem.value
            processar_dados(dados)
    except KeyboardInterrupt:
        print("Parando o consumidor...")
    finally:
        conn.close()