from time import sleep
from json import dumps
from kafka import KafkaProducer
from faker import Faker
import random
from datetime import datetime

fake = Faker()

# Configurações do Kafka
produtor = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# Tipos de sensores possíveis
tipos_sensores = ['temperatura', 'umidade', 'pressão', 'luminosidade', 'vibração']

def gerar_dados_sensor():
    return {
        'sensor_id': fake.uuid4(),
        'tipo': random.choice(tipos_sensores),
        'valor': random.uniform(0, 100),
        'timestamp': datetime.now().isoformat(),
        'localizacao': {
            'latitude': fake.latitude(),
            'longitude': fake.longitude()
        }
    }

if __name__ == '__main__':
    while True:
        try:
            dados = gerar_dados_sensor()
            produtor.send('iot_sensor_data', value=dados)
            print(f"Dados enviados: {dados}")
            sleep(random.uniform(0.5, 2.5))  # Intervalo aleatório entre envios
        except KeyboardInterrupt:
            print("Parando o produtor...")
            break