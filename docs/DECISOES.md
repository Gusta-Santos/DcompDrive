# Decisões de projeto

Este documento explica **o que** foi feito e **por que**, requisito por requisito.
Serve de base para o relatório técnico (arquitetura, protocolos, algoritmos,
concorrência/consistência, cripto/hash e limitações).

---

## 1. Arquitetura e papéis

**Decisão:** cliente/servidor no estilo HDFS/GFS, com três tipos de processo:

| Processo | Papéis acumulados | Código |
|---|---|---|
| `dfs.controller` | **Controlador** + **Nó de metadados** | `controller/cluster.py`, `replication.py` (controlador); `store.py` (metadados); `app.py` (API) |
| `dfs.storage` | **Nó de armazenamento** | `storage/chunkstore.py`, `storage/app.py` |
| `dfs.client` / navegador | **Cliente** | `client/lib.py`, `client/cli.py`, `frontend/src/dfsClient.js` |

**Por quê:**
- Um controlador central simplifica o posicionamento, a contagem de réplicas e os leases
  (não é preciso consenso distribuído). É o modelo que o próprio enunciado sugere.
- Controlador e metadados ficam no mesmo processo porque o controlador precisa consultar o
  mapa bloco → réplicas a cada decisão; separá-los adicionaria uma chamada de rede em todo
  laço de re-replicação sem benefício na escala do trabalho. Mesmo assim estão em **módulos
  separados**, como o enunciado pede.
- **Separação dos planos:** o conteúdo dos arquivos **nunca** passa pelo controlador. O
  cliente recebe a lista de nós e transfere os blocos diretamente com eles. O controlador só
  troca JSON pequeno; com isso ele não vira gargalo de dados.

**Por que o navegador é um cliente completo:** a interface web não envia arquivos ao
controlador; ela faz exatamente o que a CLI faz (divide em blocos, calcula SHA-256, cifra
com WebCrypto e envia direto aos nós). Assim a interface também respeita a separação dos
planos. Para isso os nós têm CORS liberado.

## 2. Comunicação: REST/JSON sobre HTTP (FastAPI + httpx)

**Alternativa considerada:** gRPC (recomendado no enunciado).
**Por que REST:**
- o navegador fala HTTP/JSON nativamente; gRPC no navegador exigiria gRPC-Web + proxy;
- dá para inspecionar e testar tudo com `curl` e com a página `/docs` gerada pelo FastAPI,
  o que ajuda na demonstração e na Parte 2 (locust/k6/wrk funcionam direto);
- blocos trafegam como corpo binário puro (`application/octet-stream`), sem codificação
  base64, então o custo do HTTP é só o cabeçalho.

**Mensagens principais**

| De → Para | Rota | Conteúdo |
|---|---|---|
| cliente → controlador | `POST /api/uploads` | `{path,size,replication?,overwrite}` → `{upload_id, chunk_size, write_quorum, encryption{key}, chunks[{chunk_id, targets[{node_id,disk_id,url,internal_url,zone}]}]}` |
| cliente → nó | `PUT /chunks/{id}?disk=` | corpo = bloco; cabeçalhos `X-Chunk-Hash`, `X-Hash-Alg`, `X-Pipeline` (próximos destinos) → `{acks[], errors[]}` |
| nó → nó | `PUT /chunks/{id}` | mesmo formato (pipeline) |
| cliente → controlador | `POST /api/uploads/{id}/commit` | `{file_hash, chunks[{index,size,hash,replicas[]}]}` |
| cliente → controlador | `GET /api/fs/locate?path=` | blocos com réplicas ordenadas (+ chave, se cifrado) |
| cliente → nó | `GET /chunks/{id}` | bloco (o nó verifica o hash antes de enviar; 409 se corrompido) |
| nó → controlador | `POST /api/internal/register` / `heartbeat` / `blockreport` | identidade, discos (total/usado/livre/status), lista de blocos |
| controlador → nó | `POST /replicate`, `DELETE /chunks/{id}`, `POST /chunks/{id}/verify` | ordens de cópia/remoção/verificação |

## 3. Blocos (chunks) e versões imutáveis

**Decisão:** arquivos são divididos em blocos (padrão 4 MB, configurável). Cada gravação cria
uma **versão nova e imutável** com blocos novos (ids UUID). Sobrescrever = criar versão nova e
trocar o ponteiro do arquivo atomicamente.

**Por quê:** blocos imutáveis eliminam quase todos os problemas de consistência entre
réplicas. Uma réplica tem o conteúdo exato (confirmado pelo hash) ou não existe: não há
"réplica desatualizada" de um bloco. Isso permite leitura de qualquer réplica sem
coordenação, e a recuperação vira simplesmente "copiar bytes que têm hash conhecido".
Blocos também paralelizam a transferência e espalham arquivos grandes por vários discos.

## 4. Replicação e posicionamento (RF3, RF7)

**Algoritmo** (`ClusterRegistry.choose_targets`):
1. candidatos = discos `ok` de nós `alive`, fora de drain, cujo uso **com este bloco** não
   passe de `disk_usage_limit` (90%), descontando o que já foi reservado para gravações em
   andamento;
2. ordena os candidatos por sorteio ponderado (Efraimidis–Spirakis): na política
   `free_space` o peso é o espaço livre; `load` usa o inverso das operações ativas; `random`
   usa peso igual;
3. escolhe em passadas: (a) máquina **e** zona ainda não usadas; (b) máquina não usada;
   (c) só se `allow_same_machine_fallback`, **disco** não usado em máquina já usada.
   Dois blocos no mesmo disco nunca são permitidos.

**Por que sorteio ponderado e não "o disco mais livre":** escolher sempre o mais livre manda
todos os blocos de um arquivo grande (e todas as gravações simultâneas) para o mesmo disco.
O sorteio tende a preencher os mais livres, mas espalha a carga.

**Domínios de falha (extensão):** cada nó tem uma `zone` (rack/zona). A primeira passada
prefere zonas distintas, então a perda de uma zona inteira não leva todas as réplicas.

**Menos máquinas que N (exigido documentar):**
- se há discos suficientes em máquinas já usadas e `allow_same_machine_fallback: true`,
  as réplicas vão para **discos distintos** dessas máquinas. O usuário recebe um aviso.
  Isso protege contra falha de disco, mas não contra falha da máquina;
- se nem assim há destinos, vale `under_replication_policy`:
  `accept` grava com as réplicas possíveis, marca o arquivo **sub-replicado**, avisa o
  usuário e o controlador completa as réplicas quando surgirem nós; `reject` recusa a
  gravação com HTTP 507 e mensagem clara.

**N por arquivo/diretório:** `put -r`, `mkdir -r` e `setrep`. O N efetivo é o do arquivo, ou
o do diretório ancestral mais próximo, ou o padrão do cluster.

## 5. Fluxo de escrita, quórum e consistência

1. o cliente abre a sessão: o controlador aloca N destinos por bloco e concede um **lease de
   escrita** exclusivo sobre o caminho;
2. para cada bloco o cliente envia ao 1º nó com o restante como **pipeline**. Cada nó grava em
   `tmp/`, faz `fsync`, **relê e confere o hash**, renomeia atomicamente e repassa ao próximo.
   Se o próximo falhar, ele pula para o seguinte. A resposta lista exatamente quais réplicas
   foram gravadas e verificadas;
3. se um bloco ficar com menos de **W** confirmações, o cliente pede novos destinos
   (`/allocate`), excluindo os nós que falharam, e tenta de novo;
4. no `commit`, o controlador exige ≥ W réplicas por bloco e publica a versão numa **única
   transação**. Réplicas que faltam para chegar a N são completadas em segundo plano pelo
   gerenciador de re-replicação.

**Por que pipeline e não envio paralelo:** o cliente envia cada byte uma vez só; a banda de
saída do cliente não é multiplicada por N (mesma escolha do HDFS). O custo é a latência da
cadeia, que é aceitável.

**Modelo de consistência: forte** (linearizável para metadados e para o conteúdo).
- Metadados: um único controlador serializa todas as mutações.
- Conteúdo: a versão só fica visível após o commit, e seus blocos são imutáveis e
  verificados por hash. Por isso **qualquer** réplica lida devolve exatamente a versão
  confirmada, mesmo com W < N. Não existe leitura "velha" de bloco.
- **Quórum:** padrão N=3, W=2. O cliente recebe confirmação depois de 2 réplicas gravadas e
  verificadas, e a 3ª é garantida em segundo plano. W=2 tolera a perda de um nó durante a
  gravação sem perder o dado já confirmado.

**CAP:** o sistema é **CP**. Numa partição, o lado sem o controlador não aceita escritas nem
resolve nomes (fica indisponível) para não divergir. Escolhemos consistência porque, num
sistema de arquivos, ler uma versão misturada ou perder uma gravação confirmada é pior do que
ficar temporariamente indisponível. Dentro do lado com o controlador, a disponibilidade é alta:
leituras funcionam com até N−1 nós fora e escritas funcionam enquanto houver W destinos.

## 6. Concorrência (RF8)

**Semântica adotada: lease de escrita exclusivo por caminho + versionamento.**
- Só um escritor por caminho (tabela `uploads` com `path UNIQUE`). Um segundo escritor recebe
  HTTP 409 com mensagem clara, em vez de ter sua escrita sobrescrita em silêncio (como
  aconteceria com last-writer-wins).
- O lease expira (60 s) se o cliente morrer; o cliente o renova automaticamente durante a
  transferência. Lease expirado = gravação **desfeita**: a sessão é apagada e os blocos
  enviados viram órfãos, removidos pelo block report.
- **Leitores nunca veem arquivo parcial:** a versão só existe após o commit atômico. Durante
  uma sobrescrita, quem lê obtém a versão antiga inteira. Os blocos da versão antiga só são
  apagados após `delete_grace_s`; se uma leitura longa perder esse prazo, o cliente percebe
  que a versão mudou e **recomeça com a nova**. O resultado é sempre uma versão inteira.
- Operações de diretório (mkdir, rename, rmdir) são transações. rename/rm/rmdir sobre um
  caminho com gravação em andamento são recusados (409). Um commit cujo diretório pai foi
  removido falha.
- No controlador, todos os handlers rodam num único event loop sem `await` dentro de
  transação, o que equivale ao lock global do NameNode. É simples e correto; o limite de
  escala é discutido na seção 13.

## 7. Integridade por hash (RF4)

- **Onde é calculado:** no cliente (SHA-256 padrão, SHA-512 opcional), por bloco e do arquivo
  inteiro.
- **(a) Ao final da gravação em cada réplica:** cada nó confere o hash recebido, grava, faz
  `fsync`, **relê o arquivo do disco** e confere de novo antes de confirmar.
- **(b) Em toda leitura:** o nó verifica antes de enviar (se estiver errado, responde 409,
  descarta a réplica e avisa o controlador); o cliente verifica de novo o bloco recebido
  (protege contra corrupção na rede) e, no fim, o hash do arquivo inteiro.
- **(c) Scrubbing:** cada nó relê periodicamente todos os blocos (`scrub_interval_s`),
  pausando entre eles para não saturar o disco.
- **Réplica corrompida:** é apagada no nó, a linha é removida dos metadados e a re-replicação
  cria uma cópia nova **a partir de uma réplica íntegra**. O nó de destino confere o hash do
  que copiou, então uma origem ruim é rejeitada.
- Relatos de corrupção feitos por **clientes** não apagam nada diretamente: o controlador pede
  ao nó para reverificar (`/verify`). Um cliente com bug não consegue destruir réplicas boas.

## 8. Detecção de falhas e re-replicação (RF5)

- Heartbeat a cada 2 s. Sem heartbeat por 6 s o nó fica **suspeito** (as réplicas ainda
  contam); após 12 s fica **morto** (as réplicas deixam de contar e a re-replicação começa).
  O estado suspeito evita re-replicar em massa por pausas curtas, que é o custo de um falso
  positivo. Os tempos são configuráveis.
- **Falha de disco:** o nó testa cada disco a cada heartbeat (a pasta existe e aceita
  gravação?) e informa o status. Disco `failed` deixa de contar.
- **Algoritmo:** uma consulta SQL calcula as réplicas *vivas* de cada bloco. Os
  sub-replicados são ordenados por número de réplicas vivas (**menos cópias primeiro**). Para
  cada um, a origem é a réplica viva menos ocupada e o destino vem do mesmo `choose_targets`
  (regra de não repetição + zonas). O controlador manda o **nó destino puxar** do nó origem;
  os dados não passam pelo controlador.
- **Limites:** `max_concurrent` cópias simultâneas no cluster, `max_per_node` por nó e
  `bandwidth_mbps` por cópia (o nó destino controla o ritmo da leitura).
- **Super-replicação** (nó que volta, `setrep` menor): o excesso é removido, preferindo réplicas
  que estejam na mesma máquina que outra réplica e, depois, as do disco mais cheio.
- **Métrica de recuperação:** um "episódio" começa quando surge bloco sub-replicado e termina
  quando todos voltam a N. Registra causa, duração, blocos e bytes copiados (`/metrics` e log
  `recovery_completed`).

## 9. Descoberta automática e reconciliação (RF6)

- **Registro ativo:** o nó só precisa de `CONTROLLER_URL`. Ao subir, envia `register` com seus
  discos e começa a receber blocos imediatamente. Nada é editado no controlador.
  **Por que não mDNS/multicast:** multicast costuma não funcionar entre redes Docker,
  máquinas virtuais e redes de laboratório; o registro ativo funciona em qualquer rede
  roteável.
- **Identidade estável:** `NODE_ID` (ou id gerado e salvo no 1º disco). Um nó que volta é
  reconhecido como o mesmo.
- **Reconciliação via block report** (ao registrar, ao voltar de "morto", ao recuperar um
  disco e a cada 30 s): bloco conhecido com hash certo é mantido ou re-anexado; hash errado
  significa réplica corrompida e é apagado; bloco de arquivo apagado ou sobrescrito é obsoleto
  e é apagado; bloco desconhecido (de gravação abortada) é apagado após `orphan_grace_s`;
  réplica que os metadados esperavam mas o nó não tem é removida dos metadados.
- **Controlador reiniciado:** se um nó manda heartbeat a um controlador que não o conhece
  (404), ele se registra de novo sozinho.

## 10. Gerenciamento de espaço e drain (RF7)

- Cada heartbeat leva total/usado/livre/status por disco. O controlador ainda soma
  **reservas** de gravações em andamento, para que gravações simultâneas não estourem um disco.
- Disco acima do limite (90%) não recebe novas réplicas.
- `DISK_CAPACITY_MB` simula capacidade por disco. No Docker todos os volumes estão no mesmo
  sistema de arquivos físico; sem isso, todos os "discos" informariam o mesmo espaço.
- **Drain:** o nó deixa de receber réplicas e as suas deixam de contar, então a re-replicação
  copia tudo para outros nós (usando o próprio nó em drain como origem, o que é mais rápido
  que esperar ele cair). Quando nenhum bloco depende dele, ele passa a `drained`, as cópias
  dele são apagadas e ele pode ser desligado. Depois, `forget` o remove da lista.

## 11. Criptografia (RF9)

- **Algoritmo:** AES-256-GCM (biblioteca `cryptography` no Python e WebCrypto no navegador;
  não implementamos nenhum algoritmo próprio). GCM cifra e autentica.
- **Onde cifra: no cliente.** Os nós (que são muitos e podem estar em máquinas menos
  confiáveis) só guardam texto cifrado, e o dado já trafega cifrado na rede até eles.
- **Onde ficam as chaves: envelope encryption.** O controlador tem uma chave-mestra (KEK) num
  arquivo separado do banco. Cada versão de arquivo recebe uma chave própria (DEK) aleatória,
  guardada nos metadados *cifrada pela KEK*. O cliente recebe a DEK pelo controlador e cifra
  ou decifra localmente. Assim, nós nunca veem chave nem texto claro, e o controlador nunca vê
  o conteúdo. Comprometer o disco de um nó não revela nada.
- **Formato do bloco:** `nonce(12) || texto cifrado || tag(16)`, com o `chunk_id` como *dados
  associados*. Um bloco cifrado não pode ser trocado por outro sem ser detectado.
- **Hash:** o hash de **bloco** é calculado sobre o **conteúdo cifrado** (o que está gravado),
  para que nós e scrubbing verifiquem integridade **sem a chave**. O hash do **arquivo** é sobre
  o **texto claro**, para o usuário comparar com `sha256sum` do original (e o GCM ainda
  autentica cada bloco).
- A cifra é registrada **por versão**. Ligar ou desligar a configuração não quebra a leitura
  de arquivos antigos.

## 12. Persistência dos metadados (RF10)

- **SQLite em modo WAL com `synchronous=FULL`.** O WAL é o log de operações e o checkpoint é
  o snapshot. Cada operação é uma transação ACID; um commit de arquivo grava versão, blocos,
  réplicas e troca de ponteiro de uma vez.
- **Por que não um log próprio:** daria o mesmo resultado com muito mais código e risco de bug
  justamente na parte que não pode falhar.
- **Ao reiniciar:** o banco é reaberto; os nós conhecidos voltam como *suspeitos* com prazo
  renovado (para não disparar re-replicação em massa só porque o controlador reiniciou); os
  heartbeats e block reports restabelecem o estado real. Isso foi verificado no teste e2e.

## 13. Escalabilidade e limites

- **Dados:** a vazão agregada cresce com o número de nós, porque o controlador não transporta
  dados e o posicionamento espalha os blocos.
- **Metadados:** cabem confortavelmente na casa de **dezenas de nós e centenas de milhares de
  arquivos** (cerca de 1 KB de metadados por bloco no SQLite). Limites conhecidos:
  - o laço de re-replicação recalcula a contagem de réplicas de todos os blocos a cada ciclo
    (SQL agregado, com índices). Com milhões de blocos seria preciso um índice incremental;
  - `rename` de diretório reescreve os caminhos dos descendentes (O(tamanho da subárvore));
  - o controlador processa metadados em um único thread. Arquivos minúsculos em massa
    saturam o controlador antes da rede (o experimento E3 deve mostrar isso);
  - a interface web carrega o arquivo inteiro na memória para calcular o hash do arquivo
    (WebCrypto não tem hash incremental). Para arquivos grandes, use a CLI.

## 14. Tolerância a falhas: resumo

| Falha | Comportamento |
|---|---|
| até N−1 nós caídos | leituras continuam (o cliente tenta a próxima réplica); escritas continuam se houver ≥ W destinos |
| nó cai no meio do pipeline | o nó anterior pula para o seguinte; o cliente pede novos destinos se ficar < W |
| cliente cai no meio da gravação | lease expira, gravação desfeita, órfãos removidos |
| nó cai no meio de gravar um bloco | só sobra lixo em `tmp/`, apagado ao reiniciar |
| controlador cai | sistema indisponível até reiniciar (**ponto único de falha**, mas recuperável sem intervenção) |
| disco corrompe dados | detectado em leitura ou scrubbing, cópia substituída |

## 15. Limitações conhecidas

1. **Controlador único:** reinicia e se reconstrói, mas não há réplica de prontidão
   (Raft/ZooKeeper seriam a evolução natural).
2. **Sem autenticação nem TLS:** a chave de dados (DEK) trafega em HTTP. Em produção seria
   obrigatório TLS e autenticar clientes e nós.
3. **Endpoints de depuração** (`/admin/...`) ficam ligados para a demonstração e a Parte 2;
   devem ser desligados (`DEBUG_ENDPOINTS=false`) fora disso.
4. A métrica de tempo de detecção fica em memória e zera quando o controlador reinicia; os
   logs JSON preservam o histórico.
5. Re-replicação limitada por cópia (MB/s) e por concorrência, mas não por banda total do nó.
