"""Processador de dados dos sensores (consumidor Kafka).

Várias réplicas deste serviço rodam ao mesmo tempo, todas no mesmo grupo de
consumo (``GRUPO_CONSUMO``). O coordenador do grupo, que é um dos brokers,
divide as partições do tópico entre os membros vivos. Cada partição é lida
por um único membro, e é isso que balanceia a carga.

Quando um processador entra no grupo (escala) ou sai dele (falha ou parada),
acontece um **rebalanço**: as partições são redistribuídas. Os callbacks
``ao_atribuir``, ``ao_revogar`` e ``ao_perder`` registram cada mudança no log
e no banco, e é isso que permite demonstrar o rebalanço.

Para cada leitura o processador:

1. verifica se temperatura, vibração ou energia ultrapassam os limites;
2. grava a leitura (e os alertas, se houver) no SQLite;
3. marca o offset como processado, para ser confirmado ao Kafka.

O offset só é marcado **depois** da gravação (entrega "pelo menos uma vez").
Se o processador cair entre os passos 2 e 3, outro membro relê a mensagem, e
o banco descarta a duplicata (ver ``armazenamento.py``).
"""

import json
import logging
import os
import signal
import socket
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from armazenamento import Armazenamento


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


def detectar_anomalias(leitura: dict, limites: dict) -> list:
    """Compara cada métrica da leitura com o seu limite.

    Args:
        leitura: Leitura decodificada do JSON.
        limites: Limite máximo de cada métrica, ex.: ``{"temperatura_c": 85.0}``.

    Returns:
        Lista de tuplas ``(metrica, valor, limite)`` com as métricas que
        ultrapassaram o limite (lista vazia se a leitura é normal).
    """
    return [
        (metrica, leitura[metrica], limite)
        for metrica, limite in limites.items()
        if leitura.get(metrica) is not None and leitura[metrica] > limite
    ]


class Processador:
    """Membro do grupo de consumo que processa as leituras dos sensores.

    Attributes:
        rodando: Enquanto ``True`` o laço principal continua; é colocado em
            ``False`` ao receber SIGTERM/SIGINT (ex.: ``docker stop``).
        particoes: Partições atualmente atribuídas a este processador.
    """

    def __init__(self):
        """Lê a configuração, abre o banco e cria o consumidor Kafka."""
        self.log = logging.getLogger("processador")
        self.id = socket.gethostname()
        self.topico = ler_env("TOPICO")
        self.poll_timeout = float(ler_env("POLL_TIMEOUT_S"))
        self.intervalo_estatisticas = float(ler_env("INTERVALO_ESTATISTICAS_S"))
        self.limites = {
            "temperatura_c": float(ler_env("LIMITE_TEMPERATURA_C")),
            "vibracao_mm_s": float(ler_env("LIMITE_VIBRACAO_MM_S")),
            "energia_kw": float(ler_env("LIMITE_ENERGIA_KW")),
        }
        self.db = Armazenamento(ler_env("DB_CAMINHO"), float(ler_env("DB_TIMEOUT_S")))

        self.consumer = Consumer({
            "bootstrap.servers": ler_env("KAFKA_BOOTSTRAP_SERVERS"),
            "group.id": ler_env("GRUPO_CONSUMO"),
            # client.id aparece em kafka-consumer-groups.sh --describe,
            # ligando cada partição ao container que a consome.
            "client.id": self.id,
            "partition.assignment.strategy": ler_env("ESTRATEGIA_ATRIBUICAO"),
            "session.timeout.ms": int(ler_env("SESSION_TIMEOUT_MS")),
            "heartbeat.interval.ms": int(ler_env("HEARTBEAT_MS")),
            # Primeira execução do grupo: lê o tópico desde o início
            "auto.offset.reset": "earliest",
            # Offsets confirmados periodicamente, mas só os marcados
            # explicitamente com store_offsets() (após gravar no banco).
            "enable.auto.commit": True,
            "enable.auto.offset.store": False,
        })

        self.rodando = True
        self.particoes = set()
        self.processadas = Counter()   # mensagens por partição no período
        self.alertas = 0
        self.duplicadas = 0

    # ------------------------------------------------------------------
    # Callbacks de rebalanço (executados dentro de consumer.poll())
    # ------------------------------------------------------------------
    def _registrar_rebalanco(self, evento: str, particoes: list) -> None:
        """Atualiza o conjunto de partições e registra o evento no log e no banco.

        Args:
            evento: ``"atribuidas"``, ``"revogadas"`` ou ``"perdidas"``.
            particoes: Partições (TopicPartition) envolvidas.
        """
        numeros = sorted(p.partition for p in particoes)
        if evento == "atribuidas":
            self.particoes.update(numeros)
        else:
            self.particoes.difference_update(numeros)
        atuais = sorted(self.particoes)
        nivel = logging.WARNING if evento == "perdidas" else logging.INFO
        self.log.log(nivel, "REBALANCO processador=%s partições %s: %s -> responsável por %s",
                     self.id, evento, numeros, atuais)
        self.db.registrar_evento(self.id, evento, numeros, atuais)

    def ao_atribuir(self, _consumer: Consumer, particoes: list) -> None:
        """Chamado quando o coordenador entrega partições a este processador.

        Com a estratégia ``cooperative-sticky`` a lista contém apenas as
        partições novas; a biblioteca aplica a atribuição automaticamente
        quando o callback termina.

        Args:
            _consumer: O consumidor (não utilizado).
            particoes: Partições recebidas.
        """
        self._registrar_rebalanco("atribuidas", particoes)

    def ao_revogar(self, consumer: Consumer, particoes: list) -> None:
        """Chamado quando partições são retiradas deste processador.

        Antes de perdê-las, confirma os offsets já processados para que o
        novo dono continue exatamente de onde este parou.

        Args:
            consumer: O consumidor.
            particoes: Partições revogadas.
        """
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as erro:
            # _NO_OFFSET: não havia nada novo para confirmar
            if erro.args[0].code() != KafkaError._NO_OFFSET:
                self.log.warning("falha ao confirmar offsets na revogação: %s", erro)
        self._registrar_rebalanco("revogadas", particoes)

    def ao_perder(self, _consumer: Consumer, particoes: list) -> None:
        """Chamado quando as partições foram perdidas sem revogação ordenada.

        Acontece, por exemplo, quando o processador fica tempo demais sem
        falar com o coordenador; outro membro já pode ter assumido as partições.

        Args:
            _consumer: O consumidor (não utilizado).
            particoes: Partições perdidas.
        """
        self._registrar_rebalanco("perdidas", particoes)

    # ------------------------------------------------------------------
    # Processamento
    # ------------------------------------------------------------------
    def processar(self, msg: Message) -> None:
        """Detecta anomalias em uma mensagem, grava no banco e marca o offset.

        Args:
            msg: Mensagem recebida do Kafka.
        """
        try:
            leitura = json.loads(msg.value())
        except (json.JSONDecodeError, TypeError):
            self.log.error("mensagem inválida descartada (partição=%d offset=%d)",
                           msg.partition(), msg.offset())
            self.consumer.store_offsets(msg)
            return

        alertas = detectar_anomalias(leitura, self.limites)
        nova = self.db.salvar_leitura(msg.topic(), msg.partition(), msg.offset(),
                                      leitura, alertas, self.id)
        if not nova:
            self.duplicadas += 1
            self.log.info("leitura já gravada ignorada (partição=%d offset=%d)",
                          msg.partition(), msg.offset())
        else:
            for metrica, valor, limite in alertas:
                self.alertas += 1
                self.log.warning(
                    "ALERTA maquina=%s setor=%s %s=%.2f > limite %.2f (partição=%d offset=%d)",
                    leitura["maquina_id"], leitura["setor"], metrica, valor, limite,
                    msg.partition(), msg.offset(),
                )
        self.processadas[msg.partition()] += 1
        # Só agora o offset pode ser confirmado ao Kafka
        self.consumer.store_offsets(msg)

    def registrar_estatisticas(self, duracao: float) -> None:
        """Escreve no log a carga tratada por este processador no último período.

        Args:
            duracao: Duração do período, em segundos.
        """
        total = sum(self.processadas.values())
        self.log.info(
            "ESTATISTICA processador=%s partições=%s processadas=%d (%.1f msg/s) "
            "alertas=%d duplicadas=%d por_particao=%s",
            self.id, sorted(self.particoes), total, total / duracao, self.alertas,
            self.duplicadas, dict(sorted(self.processadas.items())),
        )
        self.processadas.clear()
        self.alertas = 0
        self.duplicadas = 0

    def parar(self, signum, _frame) -> None:
        """Tratador de sinal: pede o encerramento do laço principal.

        Args:
            signum: Número do sinal recebido.
            _frame: Quadro de execução (não utilizado).
        """
        self.log.info("sinal %s recebido, saindo do grupo", signal.Signals(signum).name)
        self.rodando = False

    def executar(self) -> None:
        """Assina o tópico e processa mensagens até receber um sinal de parada."""
        self.consumer.subscribe(
            [self.topico],
            on_assign=self.ao_atribuir,
            on_revoke=self.ao_revogar,
            on_lost=self.ao_perder,
        )
        self.log.info("processador %s entrou no grupo; limites=%s", self.id, self.limites)

        inicio_periodo = time.monotonic()
        try:
            while self.rodando:
                msg = self.consumer.poll(self.poll_timeout)
                if msg is not None:
                    if msg.error():
                        self.log.error("erro do consumidor: %s", msg.error())
                    else:
                        self.processar(msg)

                agora = time.monotonic()
                if agora - inicio_periodo >= self.intervalo_estatisticas:
                    self.registrar_estatisticas(agora - inicio_periodo)
                    inicio_periodo = agora
        finally:
            # close() confirma os offsets pendentes e sai do grupo de forma
            # ordenada, provocando um rebalanço imediato nos demais membros.
            self.consumer.close()
            self.db.fechar()
            self.log.info("processador %s encerrado", self.id)


def main() -> None:
    """Configura o log, instala os tratadores de sinal e executa o processador."""
    logging.basicConfig(
        level=ler_env("LOG_LEVEL"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    processador = Processador()
    signal.signal(signal.SIGTERM, processador.parar)
    signal.signal(signal.SIGINT, processador.parar)
    processador.executar()


if __name__ == "__main__":
    main()
