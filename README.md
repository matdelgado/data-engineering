# Engenharia de Dados

Projeto prático de um **lakehouse** completo rodando na sua máquina: MinIO (armazenamento),
Spark + Delta Lake (processamento), Hive metastore (catálogo), Airflow (orquestração),
DBT (transformação) e Superset (visualização).

Este README é o **guia de execução**: siga de cima para baixo e você terá o lakehouse
funcionando na sua máquina.

> **Prefere não instalar nada?** Abra o projeto no **GitHub Codespaces** (botão verde
> **Code** → aba **Codespaces** → **Create codespace on main**). O ambiente já vem com
> Docker pronto e o projeto clonado.
>
> Nesse caso: pule as seções 1 e 2, e na seção 3 pule o `chmod` — ele roda sozinho. O
> `docker compose up -d --build` é igual. Para abrir os serviços, use a aba **PORTS** do
> VS Code em vez de `localhost`: cada porta tem uma URL própria.

---

## 1. Pré-requisitos

Instale na sua máquina:

* [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* [Docker Desktop](https://www.docker.com/products/docker-desktop/)
* [VSCode](https://code.visualstudio.com/)

A instalação depende do seu sistema operacional. Preparei guias para
[Windows](./_support/windows.md), [macOS](./_support/macos.md) e [Ubuntu](./_support/ubuntu.md).

Você também precisa de uma conta no [GitHub](https://github.com/).

> **Está no Linux?** Pode usar o Docker Engine em vez do Docker Desktop — é mais leve e
> funciona igual. Só garanta que instalou o plugin do Compose v2 (`docker compose`, com
> espaço, não `docker-compose` com hífen).

---

## 2. Clonar o projeto

Acesse https://github.com/weslleymoura/data-engineering e crie um **fork**. Isso cria uma
cópia do projeto na sua conta do GitHub.

<img src="_support/git-fork.png" width="400">

Abra o terminal, vá até a pasta onde quer salvar o projeto e clone **o seu fork**:

```
git clone <<url-do-seu-repositorio>>
```

A URL está na página do seu fork, no botão verde de código (use a opção HTTPS):

<img src="_support/git-clone.png" width="400">

---

## 3. Subir os serviços

Entre na pasta do projeto (`data-engineering`) e libere as permissões das pastas que os
containers precisam escrever:

```
sudo chmod -R 777 airflow/
sudo chmod -R 777 dbt_lakehouse/
```

Suba tudo:

```
docker compose up -d --build
```

> A primeira execução baixa vários GB de imagens e constrói três delas — leve de 15 a 30
> minutos, dependendo da sua internet. Dê pelo menos **8 GB de RAM** ao Docker Desktop
> (Settings → Resources), senão o Spark e o Superset morrem ao iniciar.

Acompanhe o progresso com:

```
docker compose ps
docker compose logs -f airflow
```

### Conferindo se deu certo

O ambiente está correto se você conseguir abrir estes endereços:

| Serviço | URL | Usuário | Senha |
| --- | --- | --- | --- |
| MinIO (armazenamento) | http://localhost:9001 | `minio` | `minio123` |
| Airflow (orquestração) | http://localhost:8080 | `admin` | `admin` |
| Spark Master | http://localhost:8081 | – | – |
| Superset (visualização) | http://localhost:8088 | `admin` | `admin` |

> A porta **9001** é o painel do MinIO. A **9000** é a API — se abrir essa no navegador,
> você vê um XML ou um erro, não a tela de login.
>
> No **Codespaces**, os endereços são outros: abra pela aba **PORTS**. Os usuários e senhas
> são os mesmos.
>
> Estas senhas são descartáveis e valem só para o ambiente local do curso.

---

## 4. Montando o lakehouse — pipeline de exemplo

Neste momento os serviços estão no ar, mas **o lakehouse está vazio**. Os passos abaixo são
os que constroem os dados, e a **ordem importa**: cada um depende do anterior.

### Passo 1 — Rodar o pipeline de ingestão

Abra o [Airflow](http://localhost:8080), encontre a DAG `lakehouse_pipeline`, **despause**
no botão da esquerda e clique no ▶ para executar.

As duas tasks devem ficar verdes:

* `bronze_to_silver` lê o CSV da camada bronze e grava como Delta na silver
* `silver_to_gold` agrega por cliente e grava o resultado na gold

Você pode acompanhar no [Spark Master](http://localhost:8081) e ver os arquivos aparecerem
no [MinIO](http://localhost:9001).

### Passo 2 — Registrar a tabela no catálogo

```
docker exec -it spark-master beeline -u jdbc:hive2://spark-thrift-server:10000 -e "CREATE TABLE IF NOT EXISTS default.order_summary USING DELTA LOCATION 's3a://gold/warehouse/default/order_summary';"
```

Deve responder `No rows selected`.

**Por que este passo existe?** O pipeline grava usando a própria sessão Spark, que tem um
catálogo separado. O Superset e o DBT conversam com outro catálogo — o do Thrift server. Este
comando registra a tabela lá.

> Você só faz isso **uma vez**. O catálogo fica guardado num volume e sobrevive a
> `docker compose down`, restart e reinício da máquina. Só é apagado com
> `docker compose down --volumes`, que apaga os dados junto.

> ⚠️ A ordem importa. Se rodar este passo **antes** do passo 1, você registra uma tabela
> apontando para um lugar vazio: o comando passa sem erro e só quebra depois, na primeira
> consulta.

### Passo 3 — Rodar o projeto DBT

De volta ao Airflow, despause e execute a DAG `dbt_run_lakehouse_project`.

### Passo 4 — Conferir

```
docker exec -it spark-master beeline -u jdbc:hive2://spark-thrift-server:10000 -e "SELECT SUM(total_amount) FROM marts.fct_summary;"
```

Se voltar um número, a corrente inteira funciona: MinIO → Spark → Delta → catálogo → DBT.

---

## 5. Mantendo seu fork atualizado

Seu fork é uma **fotografia** tirada no momento em que você clicou em Fork. Ele não
acompanha o repositório original sozinho, e o seu `git pull` busca do **seu** fork — então
correções publicadas aqui não chegam até você automaticamente.

Faça isso sempre que algo não funcionar como descrito no guia, e uma vez antes de começar
cada nova parte do curso.

O jeito fácil é o botão **Sync fork**, na página do seu fork no GitHub. Ele aparece logo
acima da lista de arquivos sempre que sua cópia está atrasada.

Depois, na sua máquina:

```
git pull
docker compose up -d --build
```

<details>
<summary>Alternativa pela linha de comando (e o que fazer em caso de conflito)</summary>

A primeira linha você roda só uma vez, para sempre:

```
git remote add upstream https://github.com/weslleymoura/data-engineering.git
git fetch upstream
git merge upstream/main
git push origin main
```

Se o `git merge` acusar conflito, é porque você alterou as mesmas linhas que foram
corrigidas. Para ficar com a sua versão: `git checkout --ours <arquivo>`. Para ficar com a
correção: `git checkout --theirs <arquivo>`. Depois `git add` no arquivo e `git commit`.

Algumas correções mudam **como** os arquivos são baixados, e não o conteúdo deles. Nesses
casos é preciso recarregar os arquivos. Commite ou guarde seu trabalho antes, porque o
`reset --hard` descarta alterações não commitadas:

```
git rm --cached -r .
git reset --hard
```

</details>

---

## 6. Comandos úteis

**Conectar o Superset ao lakehouse** — use esta string de conexão:

```
hive://spark-thrift-server:10000/default
```

Depois de conectado, você pode consultar as tabelas do DBT:

```
SELECT SUM(total_amount) AS total_amount FROM marts.fct_summary
```

**Ver a documentação e a linhagem do DBT** — as DAGs de DBT já geram a documentação. Para
servir na porta 8091:

```
docker exec -d airflow bash -c "cd /home/airflow/dbt_lakehouse/target && exec python3 -m http.server 8091"
```

Acesse http://localhost:8091 e clique no ícone azul no canto inferior direito para ver o
grafo de linhagem.

**Explorar o catálogo:**

```
docker exec -it spark-master beeline -u jdbc:hive2://spark-thrift-server:10000 -e "SHOW SCHEMAS;"
docker exec -it spark-master beeline -u jdbc:hive2://spark-thrift-server:10000 -e "SHOW TABLES IN marts;"
```

**Explorar os arquivos no MinIO:**

```
docker exec -it mc mc ls -r local/gold/warehouse/
```

**Reconstruir um modelo específico do DBT** (e tudo que depende dele):

```
docker exec -it airflow bash -c "cd /home/airflow/dbt_lakehouse && dbt build --select fct_summary"
```

**Desligar os serviços:**

```
docker compose down
```

Isso **preserva** tudo — o lakehouse volta exatamente como você deixou, catálogo incluído.

Para zerar de verdade e recomeçar do passo 1:

```
docker compose down --volumes --remove-orphans
```
