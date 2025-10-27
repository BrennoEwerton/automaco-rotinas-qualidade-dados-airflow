# Data Quality Orchestrator (GE + Airflow + LLM + SMTP)

Projeto do TCC que orquestra validações de qualidade de dados com **Great Expectations (0.18.x)**, **Apache Airflow (2.9.3, Docker)**, geração de **sugestões automáticas via LLM local (Ollama/llama3.1)** e **alerta por e-mail** quando houver falhas.

## Fluxo resumido
1) Airflow roda o checkpoint do GE no CSV de entrada  
2) Se **100% sucesso** → move o arquivo para `validated/` e **encerra**  
3) Se **falha** → move para `quarantine/`, extrai regras quebradas, chama **LLM** (Ollama) para gerar **sugestões** em `.md` e envia **e-mail** com o relatório

---

## Onde encontrar cada arquivo/parte do projeto

- **DAG principal do Airflow**:  
  `airflow_project/dags/microdados_validation_extended.py`

- **Compose do Airflow + volumes + envs**:  
  `airflow_project/docker-compose.yaml`

- **Dockerfile da imagem do Airflow (GE + client OpenAI)**:  
  `airflow_project/Dockerfile`

- **Variáveis (.env) usadas pelo compose**:  
  `airflow_project/.env`

- **Dados (entrada/saída)**:  
  CSV de entrada: `airflow_project/data/microdados_2022.CSV`  
  Aprovados (sucesso): `airflow_project/data/validated/`  
  Reprovados (falha): `airflow_project/data/quarantine/`  
  Relatórios LLM: `airflow_project/data/reports/`

- **Projeto Great Expectations** (suites, checkpoints, config):  
  Raiz do GE: `gx/`  
  Suites: `gx/expectations/`  
  Checkpoints: `gx/checkpoints/`  
  Config: `gx/great_expectations.yml`  
  (artefatos locais não versionados): `gx/uncommitted/`

> Dicas complementares por pasta: veja `airflow_project/README.md` e `gx/README.md`.

---

## Pré-requisitos
- **Windows** com Docker Desktop (WSL2)  
- **Ollama** no host (Windows) com o modelo: `ollama pull llama3.1`  
- (Opcional) **Python 3.12** + venv local se quiser editar/rodar GE fora do Docker

---

## Como executar (rápido)

1) **Ollama** (no Windows/host):
   ```powershell
   ollama pull llama3.1
   ollama serve
O Airflow acessa via http://host.docker.internal:11434/v1.

2) **Variables do Airflow/SMTP/LLM**: ajustar airflow_project/.env (modelo exemplo em airflow_project/README.md).

3) **Subir o Airflow**(powershell/CMD):
   ``` powershell
   cd airflow_project
   docker compose up airflow-init
   docker compose up -d --build
   ```
UI: http://localhost:8080 (airflow/airflow)

4) **CSV de teste**:

   Coloque o arquivo em: ``airflow_project/data/microdados_2022.CSV``
   
   Dispare a DAG ``microdados_validation_extended`` na UI.


# Como replicar (detalhado)

  - **Airflow e SMTP**: ver ``airflow_project/README.md``
  
  - **Great Expectations** (suites/checkpoints): ver ``gx/README.md``


---

# 2) `airflow_project/README.md`

```markdown
# Airflow Project — Orquestração e Infra

Este diretório contém todos os artefatos necessários para rodar o **Apache Airflow** com Docker e orquestrar a DAG `microdados_validation_extended.py`.

## Estrutura
```


airflow_project/

├─ dags/

│   └─ microdados_validation_extended.py # DAG principal (branching: sucesso vs falha)

├─ data/

│ ├─   microdados_2022.CSV # CSV de entrada (coloque aqui)

│ ├─   validated/ # destino em caso de sucesso

│ ├─   quarantine/ # destino em caso de falha

│ └─   reports/ # relatórios .md com sugestões da LLM

├─ logs/ # logs do Airflow (runtime)

├─ plugins/ # (se utilizar)

├─ docker-compose.yaml # compose (volumes, serviços, envs)

├─ Dockerfile # imagem customizada (GE + client OpenAI)

└─ .env # variáveis usadas pelo compose


> Pastas criadas/limpas no repositório via `.gitkeep` para manter a estrutura sem dados sensíveis.

## Variáveis (.env)

Crie/edite `airflow_project/.env` assim:

```ini
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.

# LLM (Ollama)
LLM_MODEL=llama3.1
LLM_BASE_URL=http://host.docker.internal:11434/v1

# E-mail de alerta (SMTP Gmail)
ALERT_EMAIL_TO=seu_email@exemplo.com
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_STARTTLS=true
SMTP_SSL=false
SMTP_USER=seu_email@gmail.com
SMTP_PASSWORD=<SENHA_DE_APP_GMAIL>
SMTP_MAIL_FROM=seu_email@gmail.com
```

# Observações

  - SMTP Gmail requer senha de app (Conta Google → Segurança → Senhas de app).

  - ``LLM_BASE_URL`` deve terminar em ``/v1``.

# Dockerfile

A imagem inclui Great Expectations e o client openai (compatível com API do Ollama):
  ```
  FROM apache/airflow:2.9.3
  USER airflow
  RUN pip install --no-cache-dir great_expectations==0.18.12 openai==1.52.2
  ```

# docker-compose.yaml (volumes importantes)

  - Montamos o ``gx`` do Windows dentro do container para que o Airflow acesse a suite/checkpoint:
    ```
    - "C:/Users/<SEU_USUARIO>/Projeto/data_quality_project/gx:/opt/airflow/gx"
    - "C:/Users/<SEU_USUARIO>/Projeto/data_quality_project/gx/uncommitted/data:/opt/airflow/data"
    ```

# Subida do Airflow
  ```
  cd airflow_project
  docker compose up airflow-init
  docker compose up -d --build
  ```

UI: http://localhost:8080
 (user: ``airflow``, pass: ``airflow``)

# Rodando a DAG

  - Coloque o CSV em airflow_project/data/microdados_2022.CSV

  - Na UI, ative e Trigger DAG: microdados_validation_extended

  - Se sucesso (100%): o arquivo vai para validated/ e a DAG encerra (LLM/email skip)

  - Se falha: vai para quarantine/, gera relatório .md em data/reports/ e envia e-mail com anexo + resumo

# Testes rápidos

  - Validar acesso ao Ollama de dentro do container:
      ```
      docker compose exec airflow-webserver python - <<'PY'
      import os
      from openai import OpenAI
      url=os.getenv('LLM_BASE_URL','http://host.docker.internal:11434/v1')
      client=OpenAI(base_url=url, api_key='ollama')
      r=client.chat.completions.create(model=os.getenv('LLM_MODEL','llama3.1'),
          messages=[{'role':'user','content':'Responda apenas OK'}])
      print(r.choices[0].message.content)
      PY
      ```

  - Conferir logs da DAG: UI → Graph → clicar na task → Log

# Onde encontrar esta DAG no repositório

  - airflow_project/dags/microdados_validation_extended.py (é a mesma referenciada no README principal)

    ---
    # 3) `gx/README.md`
    
    ```markdown
    # Great Expectations — Suites & Checkpoints
    
    Este diretório contém o projeto **Great Expectations (0.18.x)** utilizado pela DAG do Airflow.
    
    ## Estrutura
    ```


gx/

├─ expectations/ # suites versionadas (ex.: microdados_suite.json)

├─ checkpoints/ # checkpoints versionados (ex.: microdados_checkpoint.yml)

├─ great_expectations.yml # config raiz do GE

└─ uncommitted/ # artefatos locais (gitignored)


### Onde encontrar
- **Suite de expectativas** usada pela DAG:  
  `gx/expectations/microdados_suite.json`  
  (criada/ajustada via CLI/Notebook; contém as 5 regras definidas no TCC)
  
- **Checkpoint** utilizado:  
  `gx/checkpoints/microdados_checkpoint.yml`  
  > A DAG também cria dinamicamente um checkpoint `microdados_2022_checkpoint` se necessário, usando um **RuntimeBatchRequest** (encoding `latin1`, separador `;`) para ler o CSV.

- **Config do GE**:  
  `gx/great_expectations.yml`

- **Dados locais e artefatos não versionados**:  
  `gx/uncommitted/` (ex.: data docs locais, run store local etc.)

## Como replicar localmente (opcional)
Se quiser atualizar/validar a suite fora do Airflow:

1) Criar venv (na raiz do projeto) e instalar requisitos:
   ```bat
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt


2) Inicializar GE (se necessário) e abrir Jupyter para editar regras:
   ```
   great_expectations init
   jupyter notebook
   ```

  No notebook, monte um RuntimeBatchRequest apontando para seu CSV com:

  - encoding="latin1"
  - sep=";"

3) Salve a suite em gx/expectations/ e o checkpoint em gx/checkpoints/.

  No Airflow, o diretório gx/ do Windows é montado em /opt/airflow/gx. Assim, a DAG enxerga a suite/checkpoint exatamente como estão aqui.

# Observação

> Os data docs e stores locais que o GE cria vão em gx/uncommitted/ (não versionados). Se quiser regerar/visualizar, rode localmente ou adapte uma action na DAG para publicar onde preferir.

