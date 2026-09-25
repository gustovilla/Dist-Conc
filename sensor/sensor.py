"""Simulador de sensores de uma fábrica inteligente (produtor Kafka).

Cada container deste serviço representa um conjunto de máquinas de um setor
da fábrica (produção, refrigeração, empacotamento...). Periodicamente cada
máquina publica, no tópico Kafka configurado, uma leitura em JSON com
temperatura, vibração e consumo de energia.

A chave de cada mensagem é o identificador da máquina. O Kafka calcula a
partição a partir do hash da chave, então:

* todas as leituras de uma mesma máquina vão para a mesma partição, o que
  preserva a ordem das leituras daquela máquina;
* máquinas diferentes se espalham pelas partições, o que permite dividir a
  carga entre vários consumidores.

Toda a configuração vem de variáveis de ambiente (ver ``.env`` e
``docker-compose.yaml``); não há constantes fixas no código.
"""

import json
import logging
import os
import random
import signal
import socket
import time
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import KafkaError, Message, Producer


def ler_env(nome: str) -> str:
    """Lê uma variável de ambiente obrigatória.

    Args:
        nome: Nome da variável.

    Returns:
        O valor da variável.

    Raises:
        RuntimeError: Se a variável não estiver definida ou estiver vazia.
    """
    valor = os.environ.get(nome, "")
    if valor == "":
        raise RuntimeError(f"Variável de ambiente obrigatória não definida: {nome}")
    return valor


class Maquina:
    """Uma máquina da fábrica equipada com três sensores.

    Os valores lidos oscilam em torno de um valor base (ruído gaussiano).
    Com uma certa probabilidade, uma das métricas recebe um acréscimo grande,
    simulando uma anomalia (superaquecimento, vibração excessiva, pico de
    consumo) que os processadores devem detectar.

    Attributes:
        maquina_id: Identificador único da máquina (chave das mensagens).
        setor: Setor da fábrica onde a máquina está instalada.
        seq: Número de sequência da última leitura gerada.
    """

    def __init__(self, maquina_id: str, setor: str, bases: dict, anomalias: dict,
                 ruido_relativo: float, prob_anomalia: float):
        """Cria uma máquina simulada.

        Args:
            maquina_id: Identificador único da máquina.
            setor: Setor da fábrica.
            bases: Valor base de cada métrica, ex.: ``{"temperatura_c": 65}``.
            anomalias: Acréscimo médio de cada métrica em caso de anomalia.
            ruido_relativo: Desvio padrão do ruído, como fração do valor base.
            prob_anomalia: Probabilidade (0 a 1) de uma leitura ser anômala.
        """
        self.maquina_id = maquina_id
        self.setor = setor
        self.bases = bases
        self.anomalias = anomalias
        self.ruido_relativo = ruido_relativo
        self.prob_anomalia = prob_anomalia
        self.seq = 0

    def ler(self) -> dict:
        """Gera uma nova leitura dos sensores da máquina.

        Returns:
            Dicionário com os dados da leitura, pronto para ser serializado
            em JSON. O campo ``anomalia_injetada`` informa qual métrica foi
            alterada propositalmente (ou ``None``), permitindo comparar o que
            foi simulado com o que os processadores detectaram.
        """
        self.seq += 1
        valores = {
            metrica: max(0.0, random.gauss(base, base * self.ruido_relativo))
            for metrica, base in self.bases.items()
        }

        anomalia = None
        if random.random() < self.prob_anomalia:
            anomalia = random.choice(list(self.anomalias))
            # acréscimo entre 75% e 125% do valor médio configurado
            valores[anomalia] += self.anomalias[anomalia] * random.uniform(0.75, 1.25)

        return {
            "maquina_id": self.maquina_id,
            "setor": self.setor,
            "seq": self.seq,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **{metrica: round(valor, 2) for metrica, valor in valores.items()},
            "anomalia_injetada": anomalia,
        }


class Sensor:
    """Container de sensores: publica periodicamente as leituras das máquinas.

    Attributes:
        rodando: Enquanto ``True`` o laço principal continua; é colocado em
            ``False`` ao receber SIGTERM/SIGINT (ex.: ``docker stop``).
    """

    def __init__(self):
        """Lê a configuração do ambiente e cria o produtor Kafka e as máquinas."""
        self.log = logging.getLogger("sensor")
        self.hostname = socket.gethostname()
        self.topico = ler_env("TOPICO")
        self.intervalo = float(ler_env("SENSOR_INTERVALO_S"))
        self.intervalo_estatisticas = float(ler_env("INTERVALO_ESTATISTICAS_S"))
        setor = ler_env("SENSOR_SETOR")

        bases = {
            "temperatura_c": float(ler_env("SENSOR_TEMPERATURA_BASE_C")),
            "vibracao_mm_s": float(ler_env("SENSOR_VIBRACAO_BASE_MM_S")),
            "energia_kw": float(ler_env("SENSOR_ENERGIA_BASE_KW")),
        }
        anomalias = {
            "temperatura_c": float(ler_env("SENSOR_ANOMALIA_TEMPERATURA_C")),
            "vibracao_mm_s": float(ler_env("SENSOR_ANOMALIA_VIBRACAO_MM_S")),
            "energia_kw": float(ler_env("SENSOR_ANOMALIA_ENERGIA_KW")),
        }
        ruido = float(ler_env("SENSOR_RUIDO_RELATIVO"))
        prob = float(ler_env("SENSOR_PROB_ANOMALIA"))

        # O hostname do container (ID curto do Docker) torna o ID da máquina
        # único mesmo quando o serviço é escalado para vários containers.
        self.maquinas = [
            Maquina(f"{setor}-{self.hostname[:6]}-m{i}", setor, bases, anomalias, ruido, prob)
            for i in range(1, int(ler_env("SENSOR_MAQUINAS")) + 1)
        ]

        self.producer = Producer({
            "bootstrap.servers": ler_env("KAFKA_BOOTSTRAP_SERVERS"),
            "client.id": self.hostname,
            # acks=all: o líder só confirma depois que as réplicas em
            # sincronia (no mínimo min.insync.replicas) gravaram a mensagem.
            "acks": "all",
            # Evita mensagens duplicadas quando o produtor reenvia após falhas.
            "enable.idempotence": True,
            # Hash da chave -> partição (ver SENSOR_PARTICIONADOR no .env)
            "partitioner": ler_env("SENSOR_PARTICIONADOR"),
        })

        self.rodando = True
        self.entregues = Counter()      # entregas por partição no período
        self.falhas = 0                 # falhas de entrega no período
        self.particao_da_maquina = {}   # para registrar maquina -> partição

    def ao_entregar(self, erro: KafkaError, msg: Message) -> None:
        """Callback chamado pelo produtor quando o broker confirma (ou não) uma mensagem.

        É executado dentro de ``producer.poll()``/``flush()``, na mesma thread
        do laço principal, por isso os contadores não precisam de lock.

        Args:
            erro: Erro de entrega, ou ``None`` em caso de sucesso.
            msg: A mensagem enviada (com partição e offset atribuídos).
        """
        maquina = msg.key().decode()
        if erro is not None:
            self.falhas += 1
            self.log.error("falha ao entregar leitura de %s: %s", maquina, erro)
            return
        self.entregues[msg.partition()] += 1
        if self.particao_da_maquina.get(maquina) != msg.partition():
            self.particao_da_maquina[maquina] = msg.partition()
            self.log.info("máquina %s -> partição %d", maquina, msg.partition())
        self.log.debug("entregue %s partição=%d offset=%d", maquina, msg.partition(), msg.offset())

    def registrar_estatisticas(self, duracao: float) -> None:
        """Escreve no log quantas leituras foram entregues no último período.

        Args:
            duracao: Duração do período, em segundos.
        """
        total = sum(self.entregues.values())
        self.log.info(
            "ESTATISTICA sensor=%s entregues=%d (%.1f msg/s) falhas=%d por_particao=%s",
            self.hostname, total, total / duracao, self.falhas, dict(sorted(self.entregues.items())),
        )
        self.entregues.clear()
        self.falhas = 0

    def parar(self, signum, _frame) -> None:
        """Tratador de sinal: pede o encerramento do laço principal.

        Args:
            signum: Número do sinal recebido.
            _frame: Quadro de execução (não utilizado).
        """
        self.log.info("sinal %s recebido, encerrando", signal.Signals(signum).name)
        self.rodando = False

    def executar(self) -> None:
        """Laço principal: gera e publica leituras até receber um sinal de parada."""
        self.log.info(
            "sensor %s iniciado: %d máquinas, tópico=%s, intervalo=%.1fs",
            self.hostname, len(self.maquinas), self.topico, self.intervalo,
        )
        inicio_periodo = time.monotonic()
        while self.rodando:
            inicio_ciclo = time.monotonic()
            for maquina in self.maquinas:
                leitura = maquina.ler()
                try:
                    self.producer.produce(
                        self.topico,
                        key=maquina.maquina_id,
                        value=json.dumps(leitura),
                        on_delivery=self.ao_entregar,
                    )
                except BufferError:
                    # Fila local cheia (ex.: cluster indisponível): descarta a leitura
                    self.falhas += 1
                    self.log.warning("fila local do produtor cheia; leitura descartada")
                if leitura["anomalia_injetada"]:
                    self.log.info("anomalia simulada em %s: %s", maquina.maquina_id,
                                  leitura["anomalia_injetada"])

            # Processa callbacks de entrega pendentes sem bloquear
            self.producer.poll(0)

            agora = time.monotonic()
            if agora - inicio_periodo >= self.intervalo_estatisticas:
                self.registrar_estatisticas(agora - inicio_periodo)
                inicio_periodo = agora

            # Espera o que falta para completar o intervalo, atendendo callbacks.
            # poll() retorna assim que atende um evento, por isso o laço.
            fim_ciclo = inicio_ciclo + self.intervalo
            while self.rodando and (restante := fim_ciclo - time.monotonic()) > 0:
                self.producer.poll(restante)

        pendentes = self.producer.flush(self.intervalo * 10)
        self.log.info("sensor encerrado (%d mensagens não entregues)", pendentes)


def main() -> None:
    """Configura o log, instala os tratadores de sinal e executa o sensor."""
    logging.basicConfig(
        level=ler_env("LOG_LEVEL"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    sensor = Sensor()
    signal.signal(signal.SIGTERM, sensor.parar)
    signal.signal(signal.SIGINT, sensor.parar)
    sensor.executar()


if __name__ == "__main__":
    main()
