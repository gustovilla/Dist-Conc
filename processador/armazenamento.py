"""Persistência das leituras, alertas e eventos de rebalanço em SQLite.

Todos os processadores gravam no mesmo arquivo SQLite, que fica em um volume
Docker compartilhado. Como são processos diferentes escrevendo no mesmo
banco, o arquivo usa o modo WAL (leitores não bloqueiam o escritor) e um
``timeout`` de espera pelo lock de escrita.

Cada leitura é identificada pela sua posição no Kafka
(tópico, partição, offset). Se um processador cair depois de gravar mas antes
de confirmar o offset, a mesma mensagem será entregue de novo a outro
processador após o rebalanço; a chave primária faz o ``INSERT OR IGNORE``
descartar a duplicata. Assim a entrega "pelo menos uma vez" do Kafka resulta
em um registro "exatamente uma vez" no banco.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone

ESQUEMA = """
CREATE TABLE IF NOT EXISTS leituras (
    topico            TEXT    NOT NULL,
    particao          INTEGER NOT NULL,
    offset_kafka      INTEGER NOT NULL,
    maquina_id        TEXT    NOT NULL,
    setor             TEXT    NOT NULL,
    seq               INTEGER,
    timestamp_leitura TEXT    NOT NULL,
    temperatura_c     REAL,
    vibracao_mm_s     REAL,
    energia_kw        REAL,
    anomalia_injetada TEXT,
    processador       TEXT    NOT NULL,
    processado_em     TEXT    NOT NULL,
    PRIMARY KEY (topico, particao, offset_kafka)
);

CREATE TABLE IF NOT EXISTS alertas (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    topico            TEXT    NOT NULL,
    particao          INTEGER NOT NULL,
    offset_kafka      INTEGER NOT NULL,
    maquina_id        TEXT    NOT NULL,
    setor             TEXT    NOT NULL,
    metrica           TEXT    NOT NULL,
    valor             REAL    NOT NULL,
    limite            REAL    NOT NULL,
    timestamp_leitura TEXT    NOT NULL,
    processador       TEXT    NOT NULL,
    detectado_em      TEXT    NOT NULL,
    UNIQUE (topico, particao, offset_kafka, metrica)
);

CREATE TABLE IF NOT EXISTS eventos_grupo (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    processador TEXT NOT NULL,
    evento      TEXT NOT NULL,
    particoes   TEXT NOT NULL,
    atuais      TEXT NOT NULL,
    momento     TEXT NOT NULL
);
"""


def agora_iso() -> str:
    """Retorna o instante atual (UTC) em formato ISO 8601."""
    return datetime.now(timezone.utc).isoformat()


class Armazenamento:
    """Acesso ao banco SQLite compartilhado pelos processadores."""

    def __init__(self, caminho: str, timeout_s: float):
        """Abre (ou cria) o banco e garante que as tabelas existam.

        Args:
            caminho: Caminho do arquivo SQLite.
            timeout_s: Tempo máximo de espera pelo lock de escrita, em segundos.
        """
        os.makedirs(os.path.dirname(caminho), exist_ok=True)
        self.con = sqlite3.connect(caminho, timeout=timeout_s)
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute("PRAGMA synchronous=NORMAL")
        with self.con:
            self.con.executescript(ESQUEMA)

    def salvar_leitura(self, topico: str, particao: int, offset: int, leitura: dict,
                       alertas: list, processador: str) -> bool:
        """Grava uma leitura e seus alertas em uma única transação.

        Args:
            topico: Tópico de origem da mensagem.
            particao: Partição de origem.
            offset: Offset da mensagem na partição.
            leitura: Conteúdo JSON da leitura, já decodificado.
            alertas: Lista de tuplas ``(metrica, valor, limite)`` violadas.
            processador: Identificador do processador que tratou a mensagem.

        Returns:
            ``True`` se a leitura era nova; ``False`` se já havia sido gravada
            (mensagem reentregue após um rebalanço).
        """
        momento = agora_iso()
        with self.con:
            cursor = self.con.execute(
                """INSERT OR IGNORE INTO leituras VALUES
                   (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (topico, particao, offset, leitura["maquina_id"], leitura["setor"],
                 leitura.get("seq"), leitura["timestamp"], leitura.get("temperatura_c"),
                 leitura.get("vibracao_mm_s"), leitura.get("energia_kw"),
                 leitura.get("anomalia_injetada"), processador, momento),
            )
            if cursor.rowcount == 0:
                return False
            self.con.executemany(
                """INSERT OR IGNORE INTO alertas
                   (topico, particao, offset_kafka, maquina_id, setor, metrica, valor,
                    limite, timestamp_leitura, processador, detectado_em)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [(topico, particao, offset, leitura["maquina_id"], leitura["setor"],
                  metrica, valor, limite, leitura["timestamp"], processador, momento)
                 for metrica, valor, limite in alertas],
            )
        return True

    def registrar_evento(self, processador: str, evento: str, particoes: list,
                         atuais: list) -> None:
        """Registra uma mudança de atribuição de partições (rebalanço).

        Args:
            processador: Identificador do processador.
            evento: ``"atribuidas"``, ``"revogadas"`` ou ``"perdidas"``.
            particoes: Partições envolvidas neste evento.
            atuais: Partições pelas quais o processador fica responsável depois do evento.
        """
        with self.con:
            self.con.execute(
                "INSERT INTO eventos_grupo (processador, evento, particoes, atuais, momento)"
                " VALUES (?, ?, ?, ?, ?)",
                (processador, evento, json.dumps(particoes), json.dumps(atuais), agora_iso()),
            )

    def fechar(self) -> None:
        """Fecha a conexão com o banco."""
        self.con.close()
