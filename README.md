# OKX HFT Airflow DAGs

Production Airflow DAGs для **Tumar.tech OKX HFT Platform**.  
Деплоятся через **git-sync** в Docker Airflow (Airflow 2.10+).

## Назначение

Этот репозиторий содержит DAG-и для оркестрации data pipelines OKX HFT платформы:

| Pipeline | Описание | Schedule |
|----------|----------|----------|
| `okx_health_checks` | Проверка работоспособности системы | `*/5 * * * *` |
| `okx_raw_to_core` | Репликация raw -> core таблиц (trades, orderbook, funding) | `*/1 * * * *` |
| `okx_feature_engineering` | Расчёт фич для ML моделей | `@hourly` |
| `okx_data_quality` | Проверки качества данных (gaps, duplicates) | `@daily` |
| `okx_retention` | Очистка старых данных и агрегация | `@weekly` |

## Архитектура платформы

```
OPS Node (tumar.tech)
  - Airflow :8080
  - Grafana :3000  
  - MLflow :5000
  - Superset :8088
  - Traefik :443
  |
  | git-sync (этот репозиторий)
  v
TimescaleDB Node (167.86.110.201)
  - okx_raw (hypertables)
  - okx_core (hypertables)
  - okx_feat (hypertables)
  ^
  | INSERT via asyncpg
  |
Collector Node (217.216.73.20)
  - okx-hft-collector
  - WebSocket -> OKX API -> okx_raw.* tables

## Как репозитории работают вместе

Платформа намеренно разделена на два независимых слоя:

### tumar-ops (Infrastructure layer)
Отвечает за:
- запуск Airflow (webserver, scheduler, metadata DB)
- docker-compose конфигурацию
- Traefik, сети, volumes
- секреты и SSH-ключи
- git-sync для доставки DAG-ов

Этот репозиторий определяет **КАК и ГДЕ работает Airflow**,  
но не содержит бизнес-логики пайплайнов.

---

### tumar-airflow (Logic layer, этот репозиторий)
Отвечает за:
- DAG-и
- data health checks
- ETL/ELT пайплайны
- общую библиотеку для DAG-ов (`okx.common`)

Этот репозиторий определяет **ЧТО именно делает Airflow**.

---

### Почему это важно

- Инфраструктура меняется редко → ops репозиторий стабилен
- DAG-и меняются часто → dags репозиторий развивается быстро
- Обновление DAG-ов не требует SSH-доступа к серверу
- Деплой DAG-а = `git push`

Такой подход близок к GitOps и хорошо масштабируется с ростом проекта.
```

## Поток выполнения данных (Data Flow)

1. **Collector Node**
   - WebSocket соединения с OKX API
   - Получение trades, orderbook, funding
   - Асинхронные INSERT в `okx_raw.*` (TimescaleDB)

2. **TimescaleDB Node**
   - Хранение raw данных (`okx_raw`)
   - Core-таблицы (`okx_core`)
   - Feature layer (`okx_feat`)
   - Hypertables + retention policies

3. **Airflow (OPS Node)**
   - Оркестрация ETL
   - Контроль качества данных
   - Health-проверки
   - Расчёт фич и агрегаций

4. **Analytics / ML**
   - Superset (дашборды)
   - MLflow (эксперименты)
   - Executor (торговая логика)

Airflow **не участвует в real-time ingestion**,  
он работает поверх уже записанных данных и контролирует их состояние.

## Ingestion-based raw -> core (важно)

Все raw→core DAG'и используют **ingestion-time** (ts_ingest/ts_ingest_ms), а не ts_event:

- `to_dt` = начало суток UTC (00:00) для logical run-date
- `from_dt` = max(ts_ingest) в okx_core (с небольшим overlap)
- фильтрация только по `ts_ingest_ms`, без ограничений по `ts_event`
- вставка идемпотентна через `ON CONFLICT DO NOTHING`

Это гарантирует:
- поздние данные не теряются
- перезапуски безопасны
- pipeline повторяем и предсказуем


## Структура репозитория

```
okx-hft-airflow-dags/
  dags/
    okx/
      common/           # Общий код
        settings.py     # DEFAULT_ARGS, OWNER, TZ
        alerting.py     # Telegram/Slack уведомления
        utils.py        # Хелперы (date ranges, SQL templates)
      pipelines/        # DAG-файлы
        okx_health_checks.py
        okx_raw_to_core.py
  tests/
    test_import_dags.py # Проверка импорта всех DAG-ов
  scripts/
    lint.sh             # ruff + black
    test.sh             # pytest
  .github/workflows/
    ci.yml              # GitHub Actions
  pyproject.toml        # Python config
  requirements-dev.txt  # Dev dependencies
  README.md
```

## Конфигурация

### Airflow Connections (настраиваются в UI)

| Connection ID | Тип | Описание |
|---------------|-----|----------|
| `timescaledb` | Postgres | TimescaleDB (167.86.110.201:5432) |
| `telegram_bot` | HTTP | Telegram Bot API для алертов |
| `minio_s3` | S3 | MinIO для артефактов (s3.tumar.tech) |

### Airflow Variables

| Variable | Описание | Пример |
|----------|----------|--------|
| `OKX_INSTRUMENTS` | Торгуемые инструменты | `["BTC-USDT-SWAP"]` |
| `DQ_ALERT_THRESHOLD` | Порог для DQ алертов | `0.01` |

## Правила разработки

### 1. Никаких секретов в коде

```python
# ПЛОХО
conn_string = "postgresql://user:password@host:5432/db"

# ХОРОШО
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("timescaledb")
```

### 2. DAG-и должны быть тонкими

```python
# ПЛОХО - бизнес-логика в DAG
@task
def process_data():
    df = pd.read_sql("SELECT * FROM raw_table", conn)
    df["feature"] = df["price"].rolling(100).mean()
    df.to_sql("features", conn)

# ХОРОШО - вызов SQL через Hook
@task
def replicate_raw_to_core():
    hook = PostgresHook(postgres_conn_id="timescaledb")
    hook.run("""
        INSERT INTO okx_core.trades
        SELECT * FROM okx_raw.trades
        WHERE ts_ingest_ms > {{ params.last_ts }}
    """)
```

### 3. Используй TaskFlow API

```python
from airflow.decorators import dag, task

@dag(...)
def my_pipeline():
    @task
    def extract(): ...
    
    @task
    def transform(data): ...
    
    @task
    def load(data): ...
    
    load(transform(extract()))
```

### 4. Добавляй tags для фильтрации

```python
@dag(
    dag_id="okx_raw_to_core",
    tags=["okx", "etl", "core"],
    ...
)
```

## Как добавить новый DAG

1. **Создай файл** в `dags/okx/pipelines/my_pipeline.py`

2. **Используй шаблон:**

```python
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from okx.common.settings import DEFAULT_ARGS

@dag(
    dag_id="okx_my_pipeline",
    default_args=DEFAULT_ARGS,
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["okx", "my-feature"],
    doc_md="""
    ### My Pipeline
    Описание что делает DAG.
    """,
)
def my_pipeline():
    @task
    def my_task():
        hook = PostgresHook(postgres_conn_id="timescaledb")
        hook.run("SELECT 1")
        print("Task completed")
    
    my_task()

my_pipeline()
```

3. **Проверь импорт:**

```bash
pytest tests/test_import_dags.py -v
```

4. **Запусти линтер:**

```bash
bash scripts/lint.sh
```

5. **Создай PR** -> CI проверит -> merge в main -> git-sync подхватит

## Локальная разработка

```bash
# Клонировать репозиторий
git clone git@github.com:SorokaD/tumar-airflow.git
cd tumar-airflow

# Создать виртуальное окружение
python3.12 -m venv venv
source venv/bin/activate

# Установить зависимости
pip install -r requirements-dev.txt

# Линтинг
bash scripts/lint.sh

# Тесты
bash scripts/test.sh
```

## CI/CD

### GitHub Actions

Workflow: [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

На каждый PR и push в `main` / `develop`:
1. Checkout
2. Setup Python **3.12** + constraints Airflow **2.10.2**
3. Install dependencies
4. Lint (ruff, black) — пока `continue-on-error`
5. Test (pytest, DagBag / import checks)

Отдельного deploy workflow нет: после merge в `main` DAG-и забирает git-sync на ops.

### Git-sync

На ops (tumar-ops, `docker/docker-compose.ml.yaml`):

```yaml
airflow-git-sync:
  image: registry.k8s.io/git-sync/git-sync:v4.2.3
  environment:
    GITSYNC_REPO: ssh://git@ssh.github.com:443/SorokaD/tumar-airflow.git
    GITSYNC_REF: main
    GITSYNC_PERIOD: 30s
    GITSYNC_SSH: "true"
    GITSYNC_MAX_FAILURES: "-1"
```

SSH через `ssh.github.com:443` (исходящий :22 на части VPS режется). После merge в `main` DAG-и обычно видны в UI за ~30–60 с.

## Мониторинг

- **Airflow UI**: https://airflow.tumar.tech
- **Grafana Dashboard**: https://grafana.tumar.tech/d/airflow
- **Logs**: Loki -> Grafana (label: `container_name=~"airflow.*"`)

## Связанные репозитории

| Репозиторий | Описание |
|-------------|----------|
| tumar-ops | Infrastructure (Docker, Traefik, Monitoring, Airflow runtime) |
| tumar-collector | WebSocket collector → TimescaleDB |
| tumar-timescaledb | Database schema, migrations |
| tumar-executor | Trade execution service |

## Контакты

- **Team**: OKX Data Team
- **Owner**: okx-data

## Как происходит деплой DAG-ов (подробно)

1. Разработчик вносит изменения в DAG или общий код
2. Делает `git push` в ветку `main`
3. GitHub Actions выполняет:
   - проверку импорта DAG-ов
   - lint (ruff / black)
4. После merge в `main`:
   - сервис `git-sync` на OPS-ноде подтягивает репозиторий
   - код попадает в Docker volume `airflow_dags_git`
5. Airflow Scheduler:
   - автоматически перепарсивает DAG-файлы
   - регистрирует новые или обновлённые DAG-и

⏱ Среднее время доставки DAG-а в production: **30–60 секунд**

Rollback:
- `git revert` → push → git-sync → Airflow
