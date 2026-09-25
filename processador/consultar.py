"""Relatório do banco SQLite: evidência de balanceamento, rebalanço e alertas.

Executado em um container avulso que monta o mesmo volume dos processadores::

    docker compose run --rm --no-deps processador python consultar.py

Mostra:

* quantas leituras cada processador tratou (balanceamento de carga);
* quais partições cada processador tratou (divisão do trabalho);
* o histórico de rebalanços registrado pelos callbacks do grupo;
* os alertas mais recentes e a comparação entre anomalias simuladas pelos
  sensores e anomalias detectadas pelos processadores.
"""

import os
import sqlite3
import sys


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


def imprimir_tabela(titulo: str, cursor: sqlite3.Cursor) -> None:
    """Imprime o resultado de uma consulta como tabela de texto alinhada.

    Args:
        titulo: Título exibido acima da tabela.
        cursor: Cursor com a consulta já executada.
    """
    colunas = [c[0] for c in cursor.description]
    linhas = [[("" if v is None else str(v)) for v in linha] for linha in cursor.fetchall()]
    larguras = [max([len(c)] + [len(l[i]) for l in linhas]) for i, c in enumerate(colunas)]
    print(f"\n== {titulo} ==")
    print("  ".join(c.ljust(w) for c, w in zip(colunas, larguras)))
    print("  ".join("-" * w for w in larguras))
    for linha in linhas:
        print("  ".join(v.ljust(w) for v, w in zip(linha, larguras)))
    if not linhas:
        print("(nenhum registro)")


def main() -> None:
    """Executa as consultas e imprime o relatório.

    Um argumento opcional na linha de comando define quantos eventos de
    rebalanço e alertas recentes são mostrados.
    """
    limite = int(sys.argv[1]) if len(sys.argv) > 1 else int(ler_env("CONSULTA_LIMITE"))
    con = sqlite3.connect(f"file:{ler_env('DB_CAMINHO')}?mode=ro", uri=True)

    imprimir_tabela("Leituras por processador (balanceamento de carga)", con.execute(
        """SELECT processador, COUNT(*) AS leituras,
                  GROUP_CONCAT(DISTINCT particao) AS particoes,
                  MIN(processado_em) AS primeira, MAX(processado_em) AS ultima
           FROM leituras GROUP BY processador ORDER BY primeira"""))

    imprimir_tabela("Leituras por partição", con.execute(
        """SELECT particao, COUNT(*) AS leituras, COUNT(DISTINCT maquina_id) AS maquinas,
                  COUNT(DISTINCT processador) AS processadores_que_trataram
           FROM leituras GROUP BY particao ORDER BY particao"""))

    imprimir_tabela(f"Últimos {limite} eventos de rebalanço", con.execute(
        """SELECT momento, processador, evento, particoes, atuais AS responsavel_por
           FROM (SELECT * FROM eventos_grupo ORDER BY id DESC LIMIT ?) ORDER BY id""",
        (limite,)))

    imprimir_tabela(f"Últimos {limite} alertas", con.execute(
        """SELECT timestamp_leitura, maquina_id, metrica, ROUND(valor, 2) AS valor, limite,
                  particao, processador
           FROM alertas ORDER BY id DESC LIMIT ?""", (limite,)))

    imprimir_tabela("Anomalias simuladas x detectadas", con.execute(
        """SELECT
             (SELECT COUNT(*) FROM leituras) AS leituras,
             (SELECT COUNT(*) FROM leituras WHERE anomalia_injetada IS NOT NULL) AS simuladas,
             (SELECT COUNT(DISTINCT topico || particao || '-' || offset_kafka) FROM alertas)
                AS leituras_com_alerta,
             (SELECT COUNT(*) FROM alertas) AS alertas"""))
    con.close()


if __name__ == "__main__":
    main()
