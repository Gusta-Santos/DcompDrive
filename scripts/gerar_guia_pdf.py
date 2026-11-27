"""Gera docs/Guia-DComp-Drive.pdf — guia explicativo da Parte 1."""
import sys
from datetime import date

from reportlab.graphics.shapes import Drawing, Line, Polygon, Rect, String
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (KeepTogether, PageBreak, Paragraph, Preformatted, SimpleDocTemplate, Spacer,
                                Table, TableStyle)
from reportlab.platypus.tableofcontents import TableOfContents

OUT = sys.argv[1]
F = "C:/Windows/Fonts/"
pdfmetrics.registerFont(TTFont("Segoe", F + "segoeui.ttf"))
pdfmetrics.registerFont(TTFont("Segoe-B", F + "segoeuib.ttf"))
pdfmetrics.registerFont(TTFont("Segoe-I", F + "segoeuii.ttf"))
pdfmetrics.registerFont(TTFont("Mono", F + "consola.ttf"))
pdfmetrics.registerFont(TTFont("Mono-B", F + "consolab.ttf"))
from reportlab.pdfbase.pdfmetrics import registerFontFamily

registerFontFamily("Segoe", normal="Segoe", bold="Segoe-B", italic="Segoe-I", boldItalic="Segoe-B")
registerFontFamily("Mono", normal="Mono", bold="Mono-B", italic="Mono", boldItalic="Mono-B")

INK = colors.HexColor("#1d2330")
MUTED = colors.HexColor("#5b6472")
ACCENT = colors.HexColor("#2f5bd3")
LINE = colors.HexColor("#d9dde4")
SOFT = colors.HexColor("#f2f5fb")
CODEBG = colors.HexColor("#f4f5f7")
OKC = colors.HexColor("#1f8a4c")

ss = {
    "body": ParagraphStyle("body", fontName="Segoe", fontSize=10, leading=14.5, textColor=INK, spaceAfter=6),
    "h1": ParagraphStyle("h1", fontName="Segoe-B", fontSize=18, leading=23, textColor=INK, spaceBefore=4,
                         spaceAfter=10, keepWithNext=1),
    "h2": ParagraphStyle("h2", fontName="Segoe-B", fontSize=13, leading=17, textColor=ACCENT, spaceBefore=12,
                         spaceAfter=6, keepWithNext=1),
    "h3": ParagraphStyle("h3", fontName="Segoe-B", fontSize=10.5, leading=14, textColor=INK, spaceBefore=6,
                         spaceAfter=3, keepWithNext=1),
    "small": ParagraphStyle("small", fontName="Segoe", fontSize=8.5, leading=11.5, textColor=MUTED),
    "cell": ParagraphStyle("cell", fontName="Segoe", fontSize=8.8, leading=11.8, textColor=INK),
    "cellb": ParagraphStyle("cellb", fontName="Segoe-B", fontSize=8.8, leading=11.8, textColor=INK),
    "bullet": ParagraphStyle("bullet", fontName="Segoe", fontSize=10, leading=14.5, textColor=INK, leftIndent=14,
                             bulletIndent=3, spaceAfter=2),
    "code": ParagraphStyle("code", fontName="Mono", fontSize=8.6, leading=11.5, textColor=INK),
    "title": ParagraphStyle("title", fontName="Segoe-B", fontSize=28, leading=34, textColor=INK, alignment=TA_CENTER),
    "subtitle": ParagraphStyle("subtitle", fontName="Segoe", fontSize=13, leading=18, textColor=MUTED,
                               alignment=TA_CENTER),
}

story = []


def P(t, s="body"):
    story.append(Paragraph(t, ss[s]))


def H1(t):
    p = Paragraph(t, ss["h1"])
    p._toc = (0, t)
    story.append(p)


def H2(t):
    p = Paragraph(t, ss["h2"])
    p._toc = (1, t)
    story.append(p)


def H3(t):
    P(t, "h3")


def bullets(items):
    for it in items:
        story.append(Paragraph(it, ss["bullet"], bulletText="•"))
    story.append(Spacer(1, 4))


def code(t):
    pre = Preformatted(t.strip("\n"), ss["code"])
    tb = Table([[pre]], colWidths=[16.4 * cm])
    tb.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), CODEBG), ("BOX", (0, 0), (-1, -1), 0.5, LINE),
                            ("LEFTPADDING", (0, 0), (-1, -1), 8), ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                            ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6)]))
    story.append(tb)
    story.append(Spacer(1, 8))


def table(rows, widths, header=True):
    data = [[Paragraph(str(c), ss["cellb" if (header and i == 0) else "cell"]) for c in r] for i, r in enumerate(rows)]
    tb = Table(data, colWidths=[w * cm for w in widths], repeatRows=1 if header else 0)
    st = [("GRID", (0, 0), (-1, -1), 0.4, LINE), ("VALIGN", (0, 0), (-1, -1), "TOP"),
          ("LEFTPADDING", (0, 0), (-1, -1), 5), ("RIGHTPADDING", (0, 0), (-1, -1), 5),
          ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 4)]
    if header:
        st.append(("BACKGROUND", (0, 0), (-1, 0), SOFT))
    tb.setStyle(TableStyle(st))
    story.append(tb)
    story.append(Spacer(1, 10))


def callout(label, text, color=ACCENT):
    inner = [Paragraph(f"<font color='#{color.hexval()[2:]}'><b>{label}</b></font>", ss["cell"]),
             Paragraph(text, ss["cell"])]
    tb = Table([[inner]], colWidths=[16.4 * cm])
    tb.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), SOFT), ("LINEBEFORE", (0, 0), (0, -1), 3, color),
                            ("LEFTPADDING", (0, 0), (-1, -1), 9), ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                            ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6)]))
    story.append(tb)
    story.append(Spacer(1, 8))


def rf(code_, title, pede, fizemos, porque, verificar):
    block = [Paragraph(f"{code_} — {title}", ss["h2"])]
    block[0]._toc = (1, f"{code_} — {title}")
    story.extend(block)
    callout("O que o enunciado pede", pede)
    H3("Como foi implementado")
    bullets(fizemos)
    H3("Por que foi feito assim")
    bullets(porque)
    if verificar:
        H3("Como verificar")
        bullets(verificar)


def arrow(d, x1, y1, x2, y2, label=None, color=INK):
    d.add(Line(x1, y1, x2, y2, strokeColor=color, strokeWidth=1.2))
    import math
    a = math.atan2(y2 - y1, x2 - x1)
    s = 6
    d.add(Polygon([x2, y2, x2 - s * math.cos(a - 0.4), y2 - s * math.sin(a - 0.4),
                   x2 - s * math.cos(a + 0.4), y2 - s * math.sin(a + 0.4)], fillColor=color, strokeColor=color))
    if label:
        d.add(String((x1 + x2) / 2 + 4, (y1 + y2) / 2 + 3, label, fontName="Segoe", fontSize=7.5, fillColor=MUTED))


def box(d, x, y, w, h, title, sub=None, fill=SOFT):
    d.add(Rect(x, y, w, h, rx=6, ry=6, fillColor=fill, strokeColor=ACCENT, strokeWidth=1))
    d.add(String(x + w / 2, y + h - 15, title, fontName="Segoe-B", fontSize=9.5, fillColor=INK, textAnchor="middle"))
    if sub:
        for i, line in enumerate(sub):
            d.add(String(x + w / 2, y + h - 28 - i * 10, line, fontName="Segoe", fontSize=7.5, fillColor=MUTED,
                         textAnchor="middle"))


def architecture():
    d = Drawing(16.4 * cm, 8.6 * cm)
    W = 16.4 * cm
    box(d, 10, 160, 120, 70, "Cliente", ["CLI Python", "ou navegador (Vue)", "divide, hash, cifra"])
    box(d, 190, 160, 230, 70, "Controlador + Metadados", ["SQLite (WAL) · heartbeats", "posicionamento · leases",
                                                          "re-replicação · GC"], fill=colors.HexColor("#e8eefc"))
    arrow(d, 130, 205, 190, 205, None, ACCENT)
    arrow(d, 190, 185, 130, 185, None, ACCENT)
    d.add(String(160, 212, "metadados", fontName="Segoe", fontSize=7.5, fillColor=ACCENT, textAnchor="middle"))
    d.add(String(160, 175, "(JSON)", fontName="Segoe", fontSize=7.5, fillColor=ACCENT, textAnchor="middle"))
    xs = [20, 135, 250, 365]
    names = ["node1 (zona-a)", "node2 (zona-b)", "node3 (zona-c)", "node4 ... node5"]
    for x, n in zip(xs, names):
        box(d, x, 20, 95, 62, n, ["disk1 · disk2"])
    for i in range(3):
        arrow(d, xs[i] + 95, 50, xs[i + 1], 50, None, OKC)
    d.add(String(W / 2 - 40, 6, "pipeline de réplicas entre nós (plano de dados)", fontName="Segoe", fontSize=7.5,
                 fillColor=OKC, textAnchor="middle"))
    arrow(d, 60, 160, 60, 82, None, OKC)
    d.add(String(66, 120, "blocos (direto)", fontName="Segoe", fontSize=7.5, fillColor=OKC))
    for x in xs:
        arrow(d, x + 60, 82, 300, 160, None, colors.HexColor("#9aa6bd"))
    d.add(String(372, 136, "registro, heartbeat,", fontName="Segoe", fontSize=7.5, fillColor=MUTED))
    d.add(String(372, 126, "block report", fontName="Segoe", fontSize=7.5, fillColor=MUTED))
    story.append(d)
    P("Figura 1 — Blocos trafegam só entre cliente e nós (verde). O controlador troca apenas metadados (azul/cinza).",
      "small")
    story.append(Spacer(1, 8))


# ============================================================================ capa
story.append(Spacer(1, 5 * cm))
P("DComp Drive", "title")
story.append(Spacer(1, 10))
P("Sistema Distribuído de Armazenamento de Arquivos — Parte 1", "subtitle")
story.append(Spacer(1, 6))
P("Guia das especificações, de como executar e de por que cada escolha foi feita", "subtitle")
story.append(Spacer(1, 3 * cm))
P("Sistemas Distribuídos (COMP0470) · UFS", "subtitle")
P(f"Gerado em {date.today().strftime('%d/%m/%Y')}", "subtitle")
story.append(PageBreak())

# ============================================================================ sumário
P("Sumário", "h1")
toc = TableOfContents()
toc.levelStyles = [
    ParagraphStyle("t0", fontName="Segoe-B", fontSize=10, leading=15, leftIndent=0),
    ParagraphStyle("t1", fontName="Segoe", fontSize=9, leading=12.5, leftIndent=14, textColor=MUTED),
]
story.append(toc)
story.append(PageBreak())

# ============================================================================ 1
H1("1. Como ler este guia")
P("O enunciado pede um sistema distribuído de armazenamento de arquivos com replicação, verificação de integridade "
  "por hash e recuperação automática de falhas, inspirado em HDFS, GFS, Ceph e MinIO. Este guia percorre o "
  "enunciado na mesma ordem: arquitetura e papéis, como executar, cada requisito funcional (RF1 a RF12), os "
  "requisitos não funcionais, as configurações, as tecnologias e os entregáveis.")
P("Cada requisito é apresentado em quatro partes:")
bullets(["<b>O que o enunciado pede</b> — resumo do requisito, em caixa destacada;",
         "<b>Como foi implementado</b> — o que o código faz e em qual arquivo;",
         "<b>Por que foi feito assim</b> — a justificativa, ligada às explicações do enunciado "
         "e às alternativas descartadas;",
         "<b>Como verificar</b> — o comando ou teste que demonstra o requisito funcionando."])
callout("Objetivos de aprendizagem do enunciado",
        "Replicação e consistência, detecção de falhas (heartbeats e timeouts), descoberta de serviços, controle de "
        "concorrência, separação entre plano de controle (metadados) e plano de dados, e projeto tolerante a falhas "
        "e escalável. Cada um aparece explicitamente nas seções a seguir.")

# ============================================================================ 2
H1("2. Arquitetura e papéis")
P("O enunciado aceita arquitetura cliente/servidor (estilo HDFS/GFS) ou P2P/híbrida, desde que os papéis de "
  "<b>Controlador</b>, <b>Nó de metadados</b>, <b>Nó de armazenamento</b> e <b>Cliente</b> existam e estejam "
  "separados. Ele também permite que um processo acumule papéis, desde que isso seja declarado.")
architecture()
H2("Decisão: cliente/servidor no estilo HDFS/GFS")
table([
    ["Papel do enunciado", "Processo", "Onde está no código"],
    ["Controlador", "dfs.controller", "controller/cluster.py (nós, heartbeats, posicionamento) e "
                                      "controller/replication.py (re-replicação, drain, GC, reconciliação)"],
    ["Nó de metadados", "dfs.controller (mesmo processo)", "controller/store.py (árvore, versões, blocos, réplicas, "
                                                           "SQLite)"],
    ["Nó de armazenamento", "dfs.storage (um por máquina)", "storage/chunkstore.py (discos) e storage/app.py (API, "
                                                            "heartbeat, scrubbing, pipeline)"],
    ["Cliente", "dfs.client (CLI) e o navegador", "client/lib.py, client/cli.py e frontend/src/dfsClient.js"],
], [3.4, 4.2, 8.8])
H3("Por que um controlador central")
bullets([
    "Um ponto único de decisão simplifica posicionamento, contagem de réplicas e leases: não é preciso consenso "
    "distribuído (Raft/ZooKeeper), que o enunciado lista como <i>opcional</i>.",
    "É o modelo descrito no próprio enunciado (\"o cliente pede ao nó metadados/controlador a alocação...\") e nos "
    "artigos de referência (GFS e HDFS: NameNode/DataNode).",
])
H3("Por que controlador e metadados no mesmo processo")
bullets([
    "O controlador consulta o mapa bloco → réplicas a cada decisão (re-replicação, trim, drain). Separá-los criaria "
    "uma chamada de rede em todo ciclo, sem ganho na escala do trabalho.",
    "Mesmo assim os papéis ficam em <b>módulos separados</b> (store.py vs cluster.py/replication.py), como o "
    "enunciado exige, e o documento declara que o processo acumula os dois papéis.",
])
H3("Separação plano de controle × plano de dados")
P("O requisito de escalabilidade diz que o controlador não pode ser gargalo do tráfego de dados. Por isso o "
  "conteúdo dos arquivos <b>nunca</b> passa pelo controlador: ele só responde \"em quais nós gravar/ler\" e o "
  "cliente transfere os blocos direto com os nós. Isso vale também para a interface web: o navegador é um cliente "
  "completo (divide em blocos, calcula SHA-256, cifra com WebCrypto e envia direto aos nós).")

# ============================================================================ 3
H1("3. Como executar")
H2("3.1 Com Docker (recomendado)")
P("O enunciado exige que o sistema rode em várias máquinas ou contêineres \"com poucos comandos\" e com pelo menos "
  "1 controlador e 5 nós. O docker-compose.yml sobe exatamente isso: 1 controlador, 5 nós com 2 discos cada "
  "(volumes Docker distintos), distribuídos em 3 zonas, e a interface web.")
code("""
docker compose up -d --build                 # sobe tudo
WEB_PORT=8088 docker compose up -d           # se a porta 8080 estiver ocupada
docker compose ps                            # confere os contêineres
docker compose down                          # para (mantém os dados)
docker compose down -v                       # para e apaga os dados
""")
table([
    ["Endereço", "O que é"],
    ["http://localhost:8080", "Interface web (abas Arquivos e Cluster)"],
    ["http://localhost:8000/docs", "API do controlador com documentação automática"],
    ["http://localhost:9001 ... 9005", "Nós de armazenamento (/metrics, /health)"],
    ["./logs/*.jsonl", "Logs estruturados de todos os componentes"],
], [5.5, 10.9])
H2("3.2 Sem Docker")
code("""
python -m venv backend/.venv
backend/.venv/Scripts/pip install -r backend/requirements.txt     # Linux/macOS: .venv/bin/pip
backend/.venv/Scripts/python scripts/run_local.py --clean          # controlador + 5 nós
npm --prefix frontend install
npm --prefix frontend run dev                                      # web em http://localhost:5173
""")
H2("3.3 Usando a CLI")
P("A CLI fala com o controlador em http://localhost:8000 (ou na URL da variável DFS_CONTROLLER). "
  "No Git Bash do Windows, prefixe com MSYS_NO_PATHCONV=1 para que /pasta não seja convertida em caminho do "
  "Windows. Dentro do Docker, use <font name='Mono'>docker compose run --rm cli &lt;comando&gt;</font> (arquivos na "
  "pasta ./files).")
code("""
python -m dfs.client mkdir /fotos
python -m dfs.client put foto.jpg /fotos/foto.jpg        # -r 2 muda N; -f sobrescreve
python -m dfs.client ls /fotos
python -m dfs.client stat /fotos/foto.jpg                # hash, estado, réplicas por bloco
python -m dfs.client get /fotos/foto.jpg copia.jpg       # baixa e verifica o hash
python -m dfs.client mv /fotos /imagens
python -m dfs.client rm /imagens/foto.jpg
python -m dfs.client rmdir /imagens -R
python -m dfs.client setrep /imagens 2
python -m dfs.client status                              # painel do cluster (RF11)
python -m dfs.client events -n 50                        # eventos recentes
python -m dfs.client drain node3                         # esvazia um nó (RF7)
python -m dfs.client metrics                             # métricas JSON
""")
P("(Os comandos python -m dfs.client devem rodar de dentro de backend/ ou com PYTHONPATH=backend.)", "small")
H2("3.4 Roteiro da demonstração exigida")
P("O entregável 5 pede um vídeo mostrando gravação com replicação, leitura, detecção da falha de um nó com "
  "recuperação automática e detecção de arquivo corrompido. Sequência sugerida:")
code("""
# 1. gravação com replicação e leitura
python -m dfs.client put video.mp4 /demo/video.mp4
python -m dfs.client stat /demo/video.mp4          # 3 réplicas em nós/zonas distintos
python -m dfs.client get /demo/video.mp4 copia.mp4

# 2. falha de nó: detecção e recuperação (acompanhe a aba Cluster)
docker compose stop storage3                       # ~12 s depois: node3 = dead
python -m dfs.client status                        # episódio de recuperação concluído
python -m dfs.client get /demo/video.mp4 copia2.mp4  # leitura continua funcionando
docker compose start storage3                      # volta; excesso de réplicas é removido

# 3. arquivo corrompido (use um chunk_id mostrado no stat)
curl -X POST http://localhost:9001/admin/chunks/<chunk_id>/corrupt
python -m dfs.client get /demo/video.mp4 copia3.mp4  # devolve o conteúdo correto
python -m dfs.client events                          # replica_corrupt + rereplication
""")
H2("3.5 Várias máquinas físicas")
P("Para a Parte 2 em máquinas reais, o controlador roda em uma máquina e cada outra máquina sobe um nó com "
  "docker-compose.node.yml, informando apenas o endereço do controlador:")
code("""
CONTROLLER_URL=http://IP_DO_CONTROLADOR:8000 NODE_ID=node7 PUBLIC_HOST=IP_DESTA_MAQUINA \\
  docker compose -f docker-compose.node.yml up -d --build
""")
P("Clientes de outras máquinas precisam alcançar os nós: suba o compose principal com "
  "PUBLIC_HOST=&lt;ip-da-máquina&gt; para que os nós anunciem um endereço acessível.")

# ============================================================================ 4 RFs
story.append(PageBreak())
H1("4. Requisitos funcionais")

rf("RF1", "Operações sobre arquivos e diretórios",
   "No mínimo: put, get, remover, renomear/mover, listar diretório, criar e remover diretório, consultar "
   "metadados (tamanho, data, hash, número e localização das réplicas) e obter o estado do arquivo (íntegro, "
   "sub-replicado, em recuperação).",
   ["Todos os comandos existem na CLI, na API REST e na interface web: put, get, rm, mv (arquivo e diretório "
    "inteiro), ls, mkdir (com -p), rmdir (com -R), stat e setrep.",
    "stat devolve tamanho, datas de criação e modificação, hash e algoritmo, fator N, estado e, por bloco, o hash, "
    "as réplicas vivas e a localização (nó, disco, zona).",
    "Estados calculados pelo controlador: <b>íntegro</b>, <b>sub-replicado</b>, <b>em recuperação</b> (há cópia "
    "em andamento para o arquivo), <b>indisponível</b> (algum bloco sem réplica viva) e <b>gravando</b>."],
   ["O estado é <i>calculado</i> a partir das réplicas vivas, e não guardado. Assim ele nunca fica "
    "desatualizado após uma falha.",
    "Os estados extras (indisponível e gravando) cobrem casos reais que os três estados pedidos não "
    "distinguem."],
   ["python -m dfs.client stat /arquivo e a coluna ESTADO de ls; na web, botão Info."])

rf("RF2", "Interface de uso",
   "Uma CLI ou API (REST/gRPC); uma interface web simples é bônus.",
   ["CLI completa (dfs.client), API REST documentada em /docs e interface web em Vue 3."],
   ["Ter as três formas atende ao requisito e ao bônus. A API REST também permite usar locust, k6 ou wrk "
    "diretamente na Parte 2, como o enunciado sugere.",
    "A interface é propositalmente simples: o foco do trabalho é o sistema distribuído."],
   None)

rf("RF3", "Replicação",
   "Cada arquivo em N nós (N configurável), nunca em dois discos ou máquinas iguais. Sem nós suficientes, rejeitar "
   "ou aceitar como sub-replicado conforme a política, avisando o usuário.",
   ["Os arquivos são divididos em blocos e <b>cada bloco</b> tem N réplicas (padrão 3).",
    "Posicionamento (cluster.py, choose_targets) em passadas: (1) máquina e zona ainda não usadas; (2) máquina não "
    "usada; (3) só se permitido, disco ainda não usado numa máquina já usada. Dois blocos no mesmo disco "
    "nunca são permitidos.",
    "under_replication_policy: <b>accept</b> grava com as réplicas possíveis, avisa e completa depois; "
    "<b>reject</b> recusa com HTTP 507 e mensagem explicando quantos destinos havia.",
    "N configurável no cluster (YAML), por diretório (mkdir -r) e por arquivo (put -r, setrep)."],
   ["O enunciado manda documentar o que acontece com menos máquinas que N. A decisão foi usar discos distintos da "
    "mesma máquina como última opção, com aviso ao usuário: isso ainda protege contra falha de disco. Quem "
    "quiser a regra estrita desliga allow_same_machine_fallback.",
    "A preferência por zonas atende à extensão sugerida de \"domínios de falha (rack, zona)\".",
    "Os candidatos são ordenados por sorteio ponderado pelo espaço livre (Efraimidis-Spirakis) em vez de "
    "\"sempre o mais livre\". Escolher sempre o mais livre mandaria todos os blocos de um arquivo para o mesmo "
    "disco."],
   ["stat mostra as réplicas de cada bloco em nós e zonas diferentes; put -r 12 com 5 nós mostra o aviso de "
    "sub-replicação."])

rf("RF4", "Integridade por hash",
   "Hash (ex.: SHA-256) calculado no cliente ou no primeiro nó, registrado e verificado (a) ao final da gravação em "
   "cada réplica, (b) em toda leitura, (c) por scrubbing periódico. Réplica corrompida descartada e substituída. "
   "Sugestão: blocos de 4 a 64 MB, cada um com seu hash, além do hash do arquivo.",
   ["Hash calculado <b>no cliente</b>: SHA-256 de cada bloco e do arquivo inteiro (SHA-512 é opcional).",
    "(a) Cada nó confere o hash recebido, grava em tmp/, faz fsync, <b>relê do disco</b> e confere de novo antes "
    "de confirmar; só então renomeia atomicamente.",
    "(b) Na leitura, o nó verifica antes de enviar (se estiver corrompido: HTTP 409, apaga e avisa o controlador). "
    "O cliente verifica de novo cada bloco recebido e, no fim, o hash do arquivo inteiro.",
    "(c) Cada nó relê todos os blocos a cada scrub_interval_s, com pausas para não saturar o disco.",
    "Corrupção encontrada: a réplica é apagada, sai dos metadados e a re-replicação cria uma cópia nova a partir de "
    "uma réplica íntegra. O destino confere o hash do que copiou.",
    "Blocos de 4 MB (configurável), seguindo a sugestão do enunciado."],
   ["Calcular no cliente protege também o trecho de rede até o primeiro nó (o hash nasce antes de o dado sair).",
    "Reler após o fsync detecta erro de escrita no disco, não só erro de transmissão.",
    "Verificar no nó <i>e</i> no cliente: o nó evita enviar lixo; o cliente pega corrupção na rede.",
    "Relato de corrupção vindo de cliente não apaga nada direto: o controlador pede ao nó para reverificar. Um "
    "cliente com bug não consegue destruir réplicas boas."],
   ["curl -X POST http://localhost:900X/admin/chunks/&lt;id&gt;/corrupt seguido de get (ou aguardar o "
    "scrubbing) e events."])

rf("RF5", "Detecção de falhas e re-replicação",
   "Detectar falha de discos e máquinas por heartbeat e timeout; restabelecer a replicação de todos os arquivos "
   "afetados com novos destinos válidos; prioridade para quem tem menos réplicas vivas; limite de banda e "
   "concorrência.",
   ["Heartbeat a cada 2 s. Sem heartbeat por 6 s o nó fica <b>suspeito</b> (as réplicas ainda contam); após 12 s "
    "fica <b>morto</b> (as réplicas deixam de contar e a re-replicação começa).",
    "Falha de disco: a cada heartbeat o nó testa cada disco (a pasta existe e aceita gravação?) e informa o status.",
    "Uma consulta SQL lista os blocos com réplicas vivas abaixo de N, ordenados por <b>número de réplicas vivas "
    "crescente</b>.",
    "Para cada um: origem = réplica viva menos ocupada; destino = mesmo algoritmo do RF3. O controlador manda o "
    "<b>nó destino puxar</b> o bloco do nó origem.",
    "Limites: max_concurrent (cluster), max_per_node (por nó) e bandwidth_mbps (por cópia, controlado pelo nó "
    "destino)."],
   ["O estado intermediário <i>suspeito</i> evita re-replicar em massa por uma pausa curta (GC, rede lenta). "
    "Esse é o custo de um falso positivo.",
    "A cópia é nó → nó para manter o controlador fora do plano de dados, também na recuperação.",
    "Os limites de banda e concorrência atendem ao pedido de \"não saturar a rede\" e serão variados no "
    "experimento E7."],
   ["docker compose stop storage3 e acompanhar a aba Cluster ou status. No teste em Docker: detecção em 12,6 s e "
    "4 blocos recuperados em 0,13 s."])

rf("RF6", "Descoberta automática",
   "Novos nós descobertos ou registrados automaticamente, sem reiniciar nem editar configuração central. Um nó que "
   "volta é reconhecido e tem seus blocos reconciliados (aproveitados se íntegros, descartados se obsoletos).",
   ["<b>Registro ativo</b>: o nó só conhece CONTROLLER_URL. Ao subir, envia register com seus discos e passa a "
    "receber blocos imediatamente.",
    "Identidade estável (NODE_ID, ou id gerado e salvo no 1º disco): ao voltar, o nó é reconhecido como o mesmo.",
    "Block report (ao registrar, ao voltar de morto, ao recuperar um disco e a cada 30 s). Em cada bloco: íntegro e "
    "válido → mantido; hash errado → apagado; de arquivo apagado ou sobrescrito → obsoleto, apagado; desconhecido "
    "(gravação abortada) → apagado após carência.",
    "Se o controlador reinicia e não conhece o nó, ele responde 404 ao heartbeat e o nó se registra de novo "
    "sozinho."],
   ["Das três sugestões do enunciado (registro ativo, multicast/mDNS, Consul/etcd), o registro ativo é o que "
    "funciona em qualquer rede roteável. Multicast costuma falhar entre redes Docker, máquinas virtuais e redes de "
    "laboratório, e Consul/etcd seriam mais uma dependência.",
    "Reaproveitar os blocos de um nó que volta economiza banda: o que sobrar além de N é removido (trim)."],
   ["docker compose --profile extra up -d storage6 (nó novo) e docker compose start storage3 (nó que volta)."])

rf("RF7", "Gerenciamento de espaço",
   "O controlador conhece o espaço total, usado e livre de cada disco e o usa na escolha de destinos; discos acima "
   "de um limite (ex.: 90%) não recebem réplicas; comando de drain para esvaziar um nó antes de retirá-lo.",
   ["Cada heartbeat informa total, usado, livre e status de cada disco. O controlador ainda soma as "
    "<b>reservas</b> das gravações em andamento.",
    "Um disco que passaria de disk_usage_limit (0,90) com o bloco é descartado como destino.",
    "Drain (CLI, API ou botão na web): o nó para de receber réplicas, as dele deixam de contar e a re-replicação "
    "copia tudo para outros nós. Quando nada depende dele, vira <b>drained</b> e pode ser desligado; forget "
    "remove o nó da lista.",
    "DISK_CAPACITY_MB simula a capacidade de cada disco."],
   ["Sem as reservas, duas gravações simultâneas poderiam ver o mesmo espaço livre e estourar um disco.",
    "Durante o drain o próprio nó serve de origem das cópias: é mais rápido e seguro do que esperar ele cair.",
    "No Docker todos os volumes estão no mesmo disco físico e informariam o mesmo espaço; a capacidade simulada "
    "torna o limite de 90% testável."],
   ["python -m dfs.client drain node5 e status (estado draining → drained)."])

rf("RF8", "Concorrência",
   "Tratar acessos simultâneos de vários clientes aos mesmos arquivos e diretórios; definir e documentar a "
   "semântica (last-writer-wins, lease de escrita ou versionamento); uma leitura nunca devolve arquivo parcialmente "
   "escrito.",
   ["<b>Semântica: lease de escrita exclusivo por caminho + versionamento.</b>",
    "Só um escritor por caminho; o segundo recebe HTTP 409 com mensagem clara. O lease (60 s) é renovado pelo "
    "cliente durante a transferência. Se expirar, a gravação é desfeita.",
    "Cada gravação cria uma <b>versão imutável</b>, publicada numa única transação. Leitores veem a versão antiga "
    "inteira até o commit e a nova inteira depois.",
    "Os blocos de uma versão substituída só são apagados após uma carência. Se uma leitura longa perder esse "
    "prazo, o cliente percebe que a versão mudou e recomeça com a nova.",
    "rename, rm e rmdir sobre caminhos com gravação em andamento são recusados; o commit falha se o diretório pai "
    "tiver sido removido."],
   ["Com last-writer-wins, uma escrita sumiria em silêncio. Com o lease, o conflito fica visível para o "
    "usuário.",
    "O versionamento é o que garante a exigência central do RF8: como a versão só existe após o commit atômico, "
    "não há como ler metade de um arquivo.",
    "Todos os handlers do controlador rodam num único event loop, sem await dentro de transação. Isso equivale "
    "ao lock global do NameNode do HDFS: simples e livre de condições de corrida."],
   ["Teste e2e: escritores concorrentes e leitura durante sobrescrita (o leitor só vê o hash da v1 ou da v2)."])

rf("RF9", "Criptografia (opcional por configuração)",
   "Dados cifrados nos nós (ex.: AES-256-GCM); decidir e justificar onde cifrar (cliente ou servidor), onde ficam as "
   "chaves e se o hash é sobre o texto claro ou o cifrado.",
   ["AES-256-GCM via biblioteca <i>cryptography</i> (Python) e WebCrypto (navegador). Nenhum algoritmo próprio, "
    "como o enunciado recomenda.",
    "Liga e desliga com encryption.enabled. A escolha é registrada por versão, então arquivos antigos continuam "
    "legíveis.",
    "<b>Onde cifra: no cliente.</b>",
    "<b>Onde ficam as chaves: envelope encryption.</b> O controlador tem uma chave-mestra (KEK) em arquivo separado "
    "do banco. Cada versão recebe uma chave própria (DEK) aleatória, guardada nos metadados cifrada pela KEK. O "
    "cliente recebe a DEK e cifra localmente.",
    "Formato do bloco: nonce (12 bytes) + texto cifrado + tag (16 bytes), com o chunk_id como dado associado."],
   ["Cifrar no cliente: os nós (muitos, em máquinas possivelmente menos confiáveis) só guardam texto cifrado, e o "
    "dado já trafega cifrado.",
    "Envelope encryption: nós nunca veem chave nem texto claro; o controlador nunca vê o conteúdo. Comprometer o "
    "disco de um nó não revela nada.",
    "<b>Hash do bloco sobre o conteúdo cifrado</b>: nós e scrubbing verificam integridade sem a chave. <b>Hash do "
    "arquivo sobre o texto claro</b>: o usuário compara com sha256sum do original.",
    "O chunk_id como dado associado impede trocar blocos cifrados de lugar sem detecção."],
   ["Suba com DFS__ENCRYPTION__ENABLED=true; o arquivo baixado bate com o original, mas o conteúdo no disco do nó "
    "é ilegível. Teste e2e: o texto SEGREDO não aparece no bloco gravado."])

rf("RF10", "Persistência e recuperação dos metadados",
   "Os metadados (árvore, mapa arquivo → blocos → réplicas) não podem se perder ao reiniciar: usar log de operações "
   "com snapshots ou banco de dados; sem recriação manual do estado.",
   ["SQLite em modo <b>WAL</b> com synchronous=FULL; cada operação é uma transação ACID.",
    "Ao reiniciar, o banco é reaberto. Os nós conhecidos voltam como <i>suspeitos</i> com prazo renovado; "
    "heartbeats e block reports restabelecem o estado real."],
   ["O WAL do SQLite é exatamente \"log de operações com snapshots\" (o checkpoint é o snapshot), já testado e "
    "confiável. Escrever um log próprio seria mais código e mais risco justamente na parte que não pode falhar.",
    "Voltar os nós como suspeitos (e não mortos) evita disparar re-replicação em massa só porque o "
    "controlador reiniciou."],
   ["docker compose restart controller e depois get de qualquer arquivo; teste e2e RF10."])

rf("RF11", "Visibilidade do estado",
   "Comando ou painel com nós (vivos, suspeitos, mortos), espaço de cada disco, quantidade de arquivos por estado e "
   "re-replicações em andamento.",
   ["CLI <b>status</b> (com -w para atualizar sozinho) e aba <b>Cluster</b> da web, atualizada a cada 2 s.",
    "Também mostra: blocos sub e super-replicados, episódios de recuperação (causa, duração, bytes) e eventos "
    "recentes."],
   ["Os episódios de recuperação já medem o que a Parte 2 pede (tempo de recuperação e volume copiado)."],
   None)

rf("RF12", "Registro de eventos (logs)",
   "Logs estruturados em todos os componentes (data/hora, componente, evento, id do arquivo ou bloco).",
   ["Uma linha JSON por evento: ts (ISO-8601 UTC, ms), level, component, instance, event e campos como path, "
    "chunk_id, node_id, disk_id, bytes, duration_ms.",
    "No Docker, todos os logs vão para ./logs (controller.jsonl, node1.jsonl, ...).",
    "Eventos-chave: node_dead, recovery_started, rereplication_completed, recovery_completed, chunk_corrupt_detected, "
    "replica_corrupt, file_committed, node_registered."],
   ["JSON por linha é fácil de filtrar com jq ou pandas. Por exemplo, o tempo de detecção da Parte 2 é a "
    "diferença entre o horário da falha injetada e o evento node_dead."],
   None)

# ============================================================================ 5 RNF
H1("5. Requisitos não funcionais")
H2("Tolerância a falhas")
callout("O que o enunciado pede", "Continuar atendendo leituras (e escritas, se houver nós) com até N−1 nós fora; "
        "falhas de rede, nós lentos e quedas no meio da gravação não podem deixar estado inconsistente; o "
        "controlador deve ao menos reiniciar e reconstruir o estado.")
table([
    ["Falha", "Comportamento"],
    ["Até N−1 nós caídos", "Leituras continuam (o cliente tenta a próxima réplica); escritas continuam se houver "
                           "pelo menos W destinos."],
    ["Nó cai no meio do pipeline", "O nó anterior pula para o seguinte; se ficar abaixo de W, o cliente pede "
                                   "novos destinos (/allocate) excluindo os que falharam."],
    ["Cliente cai no meio da gravação", "O lease expira, a sessão é apagada e os blocos órfãos são removidos "
                                        "pelo block report."],
    ["Nó cai enquanto grava um bloco", "Só sobra lixo em tmp/, apagado ao reiniciar; o nome final só é criado por "
                                       "rename atômico após conferir o hash."],
    ["Nó lento", "Timeouts em todas as chamadas; o estado suspeito evita falsos positivos."],
    ["Controlador cai", "Indisponível até reiniciar; ao reiniciar, recupera tudo do SQLite (RF10) sem "
                        "intervenção."],
], [5, 11.4])
H2("Escalabilidade")
bullets([
    "Adicionar nós aumenta capacidade e vazão sem mudar código nem parar o sistema (registro ativo, RF6). O "
    "posicionamento por espaço livre passa a preferir os nós novos.",
    "O controlador nunca transporta dados (seção 2), só JSON pequeno.",
    "<b>Escala esperada:</b> dezenas de nós e centenas de milhares de arquivos (cerca de 1 KB de metadados por "
    "bloco).",
    "<b>Limites conhecidos:</b> o laço de re-replicação recalcula a contagem de todos os blocos a cada ciclo; o "
    "rename de diretório é proporcional ao tamanho da subárvore; o controlador processa metadados em um único "
    "thread, então muitos arquivos minúsculos saturam o controlador antes da rede (é o que o experimento E3 deve "
    "mostrar); a web carrega o arquivo inteiro na memória para calcular o hash.",
])
H2("Consistência e teorema CAP")
callout("O que o enunciado pede", "Declarar o modelo (forte ou eventual), o quórum de escrita W e justificar em "
        "relação ao teorema CAP.")
bullets([
    "<b>Modelo: consistência forte.</b> Os metadados passam por um único controlador que serializa as mutações. O "
    "conteúdo só fica visível após o commit, e os blocos são imutáveis e verificados por hash. Por isso qualquer "
    "réplica lida devolve exatamente a versão confirmada, mesmo com W &lt; N.",
    "<b>Quórum padrão: N = 3, W = 2.</b> A escrita é confirmada após 2 réplicas gravadas e verificadas; a 3ª é "
    "completada em segundo plano pela re-replicação (o exemplo dado no próprio enunciado). W = 2 tolera perder um "
    "nó durante a gravação sem perder dado já confirmado.",
    "<b>CAP: o sistema é CP.</b> Numa partição, o lado sem o controlador não aceita escritas nem resolve nomes, para "
    "não divergir. Num sistema de arquivos, ler uma versão misturada ou perder uma escrita confirmada é pior que "
    "ficar temporariamente indisponível. No lado com o controlador, leituras sobrevivem a N−1 falhas.",
])
H2("Observabilidade")
bullets([
    "GET /metrics no controlador: latência por rota (média, p50, p95, máximo), arquivos por estado, blocos por "
    "situação, re-replicações, episódios de recuperação e tempos de detecção.",
    "GET /metrics em cada nó: vazão e latência de put_chunk, get_chunk e replicate_in, uso dos discos e corrupções "
    "detectadas.",
    "A CLI informa a vazão (MB/s) de cada put/get; os logs JSON completam o histórico (RF12).",
])

# ============================================================================ 6 config
H1("6. Configurações")
P("O enunciado pede configurações em arquivo (YAML, JSON ou TOML) e/ou variáveis de ambiente e linha de comando, "
  "sem recompilar. Tudo fica em <b>backend/config/config.yaml</b>, lido pelo controlador, que é a <b>fonte única "
  "de verdade</b>: os nós recebem os intervalos no registro e os clientes recebem tamanho de bloco, hash e cifra "
  "ao abrir uma gravação.")
code("""
# Precedência: padrão < YAML < variável de ambiente < linha de comando
DFS__CLUSTER__REPLICATION_FACTOR=2 docker compose up -d
python -m dfs.controller --set encryption.enabled=true --set cluster.write_quorum=1
""")
table([
    ["Parâmetro do enunciado", "Exigência", "Chave no YAML", "Padrão"],
    ["Criptografia", "Obrigatório", "encryption.enabled / algorithm", "false / AES-256-GCM"],
    ["Nível de replicação (N)", "Obrigatório", "cluster.replication_factor (+ por dir/arquivo)", "3"],
    ["Tamanho do bloco", "Opcional", "cluster.chunk_size_mb", "4"],
    ["Intervalo e timeout de heartbeat", "Recomendado", "heartbeat.interval_s / suspect_after_s / dead_after_s",
     "2 / 6 / 12 s"],
    ["Quórum de escrita (W)", "Recomendado", "cluster.write_quorum", "2"],
    ["Algoritmo de hash", "Opcional", "cluster.hash_algorithm", "sha256"],
    ["Intervalo de scrubbing", "Opcional", "storage.scrub_interval_s", "60 s"],
    ["Limite de uso do disco", "Opcional", "storage.disk_usage_limit", "0.90"],
    ["Política de posicionamento", "Opcional", "placement.policy / zone_aware", "free_space / true"],
    ["Limite de re-replicação", "Opcional", "rereplication.max_concurrent / max_per_node / bandwidth_mbps",
     "4 / 2 / 50 MB/s"],
    ["(extra) política de sub-replicação", "—", "cluster.under_replication_policy", "accept"],
    ["(extra) lease de escrita", "—", "leases.write_lease_s", "60 s"],
], [4.3, 2.4, 6.4, 3.3])
P("Todos os parâmetros pedidos, inclusive os opcionais, estão implementados.")

# ============================================================================ 7 tecnologias
H1("7. Tecnologias escolhidas")
table([
    ["Necessidade", "Sugestões do enunciado", "Escolha", "Por quê"],
    ["Comunicação", "gRPC (recomendado), REST, ZeroMQ, TCP", "REST/JSON (FastAPI + httpx)",
     "O navegador fala HTTP nativamente (gRPC exigiria gRPC-Web + proxy); testável com curl e /docs; blocos vão como "
     "binário puro, sem base64."],
    ["Hash e cripto", "SHA-256, AES-256-GCM, libsodium, Tink", "hashlib, cryptography, WebCrypto",
     "Bibliotecas padrão e auditadas, compatíveis entre Python e navegador (verificado: o que um cifra, o outro "
     "decifra)."],
    ["Descoberta e falhas", "Registro ativo + heartbeat, mDNS, Consul...", "Registro ativo + heartbeat próprio",
     "O mais simples segundo o enunciado e o que funciona em qualquer rede."],
    ["Metadados", "PostgreSQL, RocksDB, etcd, log próprio", "SQLite (WAL)",
     "Transações ACID sem servidor extra; embutido no controlador."],
    ["Consenso", "Raft, ZooKeeper (opcional)", "Não usado", "Opcional; o controlador se recupera reiniciando (RF10). "
                                                                "Listado como limitação."],
    ["Contêineres", "Docker Compose (recomendado)", "Docker Compose",
     "Volumes distintos simulam discos; um comando sobe 1 + 5 nós."],
    ["Testes", "pytest, scripts shell, locust...", "scripts/e2e_test.py",
     "Sobe o cluster, injeta falhas e verifica 17 cenários; serve de base para a Parte 2."],
    ["Frontend", "(livre)", "Vue 3 + Vite", "Pedido da equipe; leve e suficiente para uma interface simples."],
], [2.5, 3.7, 3.4, 6.8])

# ============================================================================ 8 fluxos
H1("8. Fluxos de escrita e leitura")
P("O enunciado sugere um fluxo de escrita e um de leitura; a implementação segue os dois, com estas escolhas:")
H2("Escrita")
code("""
1. cliente ─POST /api/uploads─► controlador
      aloca N destinos por bloco, concede o lease, devolve W (e a chave, se cifrado)
2. cliente ─PUT bloco─► node A ─► node B ─► node C      (pipeline)
      cada nó grava, faz fsync, relê e confere o hash; volta: acks {A, B, C}
3. se acks < W: POST /allocate (exclui os nós que falharam) e reenvia
4. cliente ─POST /commit (hash do arquivo + réplicas)─► controlador
      publica a versão numa única transação; réplicas até N: segundo plano
""")
P("<b>Pipeline em vez de envio paralelo:</b> o enunciado aceita os dois. No pipeline, o cliente envia cada byte "
  "uma vez só e sua banda de saída não é multiplicada por N (a mesma escolha do HDFS). Se um nó do meio falhar, o "
  "anterior pula para o seguinte.")
H2("Leitura")
code("""
1. cliente ─GET /api/fs/locate─► controlador
      blocos + réplicas ordenadas (viva e menos carregada primeiro)
2. cliente ─GET bloco─► réplica 1      (o nó confere o hash antes de enviar)
      hash errado ou nó fora: reporta a corrupção e tenta a réplica 2, 3...
3. cliente decifra (se for o caso), confere o hash do arquivo inteiro
      e só então renomeia para o arquivo final
""")
P("Escolher a réplica viva e menos carregada, com desempate aleatório, é a \"mais próxima ou menos carregada\" "
  "sugerida no enunciado, e espalha as leituras entre os nós.")

# ============================================================================ 9 entregáveis
H1("9. Entregáveis e situação")
table([
    ["Entregável", "Situação"],
    ["1. Repositório Git com commits de todos", "Pendente: o projeto ainda não é um repositório Git. Cada "
                                               "integrante precisa ter commits próprios."],
    ["2. README com instalação, execução e exemplos", "Pronto: README.md."],
    ["3. Relatório técnico (4 a 8 páginas)", "Base pronta: docs/DECISOES.md e este guia cobrem arquitetura, "
                                            "protocolos, algoritmos, concorrência, consistência, cripto e "
                                            "limitações. Falta a divisão de tarefas da equipe."],
    ["4. Scripts que sobem 1 controlador + 5 nós", "Pronto: docker-compose.yml (testado) e scripts/run_local.py."],
    ["5. Demonstração (vídeo até 5 min)", "Pendente de gravação; roteiro na seção 3.4."],
], [6, 10.4])
H2("Testes já realizados")
P("scripts/e2e_test.py passou 17/17: put/get com 3 réplicas em nós, discos e zonas distintos; operações de "
  "diretório; GC; escritores concorrentes; leitura durante sobrescrita; queda de nó; escrita com nó fora; nó que "
  "volta; corrupção na leitura; corrupção no scrubbing; falha de disco; política de sub-replicação; N por "
  "diretório; drain; reinício do controlador; criptografia; métricas e logs. No Docker também foram verificados "
  "a CLI fora e dentro da rede, o navegador e a queda/volta do storage3.")

# ============================================================================ 10 limitações
H1("10. Limitações conhecidas")
P("O enunciado valoriza relatar falhas com honestidade. As limitações atuais:")
bullets([
    "<b>Controlador único:</b> reinicia e se reconstrói, mas não há réplica de prontidão (Raft/ZooKeeper seriam a "
    "evolução natural). Enquanto está fora, o sistema fica indisponível.",
    "<b>Sem autenticação nem TLS:</b> a chave de dados trafega em HTTP. Em produção, TLS e autenticação de clientes "
    "e nós seriam obrigatórios.",
    "<b>Endpoints de depuração</b> (/admin/...) ficam ligados para a demonstração e a Parte 2; desligue com "
    "DEBUG_ENDPOINTS=false.",
    "O tempo de detecção em /metrics fica em memória e zera quando o controlador reinicia; os logs JSON preservam o "
    "histórico.",
    "A re-replicação é limitada por cópia e por concorrência, mas não pela banda total de cada nó.",
    "A interface web lê o arquivo inteiro na memória para calcular o hash; para arquivos grandes, use a CLI.",
])
callout("Atenção ao enunciado",
        "O enunciado pede commits de todos os integrantes e proíbe o uso de IA \"de forma indiscriminada\". "
        "Antes da entrega, estude este guia e o código até conseguir explicar e defender cada decisão, e confirme "
        "com o professor o uso permitido de ferramentas de IA.", colors.HexColor("#b7791f"))


# ============================================================================ build
class Doc(SimpleDocTemplate):
    def afterFlowable(self, f):
        if hasattr(f, "_toc"):
            level, text = f._toc
            key = f"s{id(f)}"
            self.canv.bookmarkPage(key)
            self.canv.addOutlineEntry(text, key, level=level, closed=level > 0)
            self.notify("TOCEntry", (level, text, self.page, key))


def deco(canv, doc):
    if doc.page == 1:
        return
    canv.saveState()
    canv.setFont("Segoe", 8)
    canv.setFillColor(MUTED)
    canv.drawString(2.3 * cm, 1.3 * cm, "DComp Drive — Guia da Parte 1")
    canv.drawRightString(A4[0] - 2.3 * cm, 1.3 * cm, f"{doc.page}")
    canv.setStrokeColor(LINE)
    canv.line(2.3 * cm, 1.7 * cm, A4[0] - 2.3 * cm, 1.7 * cm)
    canv.restoreState()


doc = Doc(OUT, pagesize=A4, leftMargin=2.3 * cm, rightMargin=2.3 * cm, topMargin=2 * cm, bottomMargin=2.2 * cm,
          title="DComp Drive — Guia da Parte 1", author="Equipe DComp Drive",
          subject="Sistema distribuído de armazenamento de arquivos")
doc.multiBuild(story, onFirstPage=deco, onLaterPages=deco)
print("ok", OUT)
