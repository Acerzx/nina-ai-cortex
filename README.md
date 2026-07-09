# 🌌 N.I.N.A. AI Cortex 🧠

**N.I.N.A. AI Cortex** — это интеллектуальная когнитивная надстройка и многоагентная система (Multi-Agent Swarm) для автономного управления, мониторинга и оптимизации астрономической обсерватории под управлением программного комплекса **N.I.N.A.** (Nighttime Imaging 'N' Astronomy).

Система в реальном времени анализирует телеметрию, логи, метрики качества (HFR, FWHM, RMS) и погодные условия, принимая автономные решения для максимизации результата съемки и обеспечения абсолютной безопасности оборудования.

---

## 🚀 Ключевые возможности

- **🤖 Multi-Agent Swarm (9 AI-агентов)**: Специализированные агенты для мониторинга, диагностики, оптимизации и обеспечения безопасности, координируемые через Dual Orchestrator System.
- **🎭 Dual Orchestrator System**: Гибридная система оркестрации с Event-Driven Orchestrator для простых реакций и Hybrid LangGraph Orchestrator для сложных многошаговых workflow (диагностика, post-mortem анализ, адаптивные реакции).
- **📖 OpenAPI-Driven Execution**: Динамическое построение реестра триггеров из OpenAPI спецификации N.I.N.A. API — автоматическая валидация параметров и защита от некорректных команд.
- **👁️ Shadow Engine**: Неинвазивный парсинг `Sequence.json` N.I.N.A. для построения теневого графа (DAG) и предсказания действий секвенсора без вмешательства в его работу.
- **🛡️ Hardware Abstraction Layer (HAL)**: Финальный барьер безопасности, блокирующий опасные команды (slew, triggers) во время критических фаз (Meridian Flip, Park, Shutdown) или при `UNSAFE` статусе.
- **📚 RAG-система (Retrieval-Augmented Generation)**: Векторная база знаний (Qdrant) с LRU-кэшированием эмбеддингов для обучения на истории прошлых сессий, документаций и генерации Post-Mortem отчетов.
- **🔌 Deep N.I.N.A. Integration**: Двусторонняя связь через Advanced API (HTTP) и WebSocket. Поддержка триггеров, инъекции глобальных переменных и управления Dynamic Sequencer.
- **📊 Dual Telemetry Ingestion**: Агрегация метрик из InfluxDB 2.x (основной источник) и Prometheus (резервный), парсинг FITS-заголовков, CSV-отчетов (Hocus Focus) и логов.
- **🎭 Simulation Mode**: Встроенные эмуляторы `FakeNina` и `FakePhd2` для безопасного тестирования агентов и инжекта аномалий без реального оборудования.
- **📜 Decision Audit Trail**: Полная объяснимость ИИ (Explainable AI) с сохранением всех решений в SQLite, оценкой постфактум (Hindsight Verdict) и политиками ретеншена.
- **📈 Prometheus Metrics**: Экспорт метрик самого Cortex для мониторинга через Prometheus/Grafana.

---

## 🏗️ Архитектура системы

Система построена на паттерне **Orchestrator-Worker** с асинхронным EventBus ядром и **Dual Orchestrator System**.

### Dual Orchestrator System

```
┌─────────────────────────────────────────────────────────┐
│                    EventBus (Events)                     │
└──────────────────┬──────────────────────────────────────┘
                   │
        ┌──────────┴──────────┐
        │                     │
        ▼                     ▼
┌───────────────┐    ┌─────────────────┐
│  Orchestrator │    │ Hybrid LangGraph│
│  (Reactive)   │    │  Orchestrator   │
│               │    │  (Proactive)    │
│ Простые       │    │  Сложные        │
│ реакции:      │    │  workflow:      │
│ - ALERT →     │    │  - Diagnostic   │
│   Guardian    │    │  - Post-Mortem  │
│ - NEW_FRAME → │    │  - Adaptive     │
│   Watcher     │    │                 │
└───────────────┘    └─────────────────┘
```

**Event-Driven Orchestrator** (`orchestrator.py`):

- Реактивная маршрутизация событий
- Простые реакции: `ALERT` → `Guardian`, `NEW_FRAME` → `Watcher`
- Быстрая обработка с минимальной задержкой

**Hybrid LangGraph Orchestrator** (`hybrid_langgraph_orchestrator.py`):

- Проактивные многошаговые workflow
- **Diagnostic Workflow**: поиск root cause через RAG + корреляции метрик
- **Post-Mortem Workflow**: анализ завершённой сессии с генерацией Session Digest
- **Adaptive Workflow**: адаптация к изменяющимся условиям (погода, оборудование)
- Retry logic, checkpointing, состояние workflow

### 1. Ingestion Layer (Сбор данных)

- **Watchers**: Мониторинг файловых систем (Session Metadata, Masters Library, Hocus Focus, LiveStack, AI Weather).
- **Pollers**: Опрос InfluxDB (Flux queries) и Prometheus Exporter.
- **Log Tailer**: Стриминг и классификация логов N.I.N.A. в реальном времени (Regex-паттерн матчинг).
- **WebSocket Client**: Подписка на нативные события N.I.N.A. (`SequenceItemStarted`, `MeridianFlip`, и т.д.).

### 2. Core (Ядро)

- **EventBus**: Асинхронная шина событий с метриками и дедупликацией.
- **ObservatoryState**: Единый in-memory стейт обсерватории (агрегатор всех источников).
- **Mode Manager**: Управление режимами (`FULL_AI`, `SAFE_AUTONOMOUS`, `MANUAL`, `SIMULATION`) с автоматическим fallback при потере LLM.
- **RAG Engine**: Гибридные эмбеддинги (Ollama `nomic-embed-text` + LRU-кэш 10000 записей) и векторный поиск.
- **Cortex Metrics**: Prometheus-совместимые метрики для мониторинга системы.

### 3. Execution Layer (Исполнение)

- **OpenAPI-Driven Trigger Emulator**: Динамическое построение реестра триггеров из OpenAPI спецификации N.I.N.A. API с автоматической валидацией параметров.
- **Global Var Injector**: Изменение переменных Sequencer+ с маскированием чувствительных данных в логах.
- **Python Bridge**: Whitelist-выполнение IronPython/C# скриптов внутри N.I.N.A. (защита от произвольного кода).
- **Safety Interceptor**: Перехват инструкций `ShutdownPc` для предотвращения внезапного отключения ПК.
- **HAL (Hardware Abstraction Layer)**: Финальная валидация всех команд перед отправкой в N.I.N.A.

---

## 🤖 Директория AI-Агентов

| Агент             | Роль                        | Описание                                                                                        |
| :---------------- | :-------------------------- | :---------------------------------------------------------------------------------------------- |
| **Watcher**       | Monitor & Anomaly Detection | Непрерывный анализ трендов (Z-Score, HFR, RMS, ветер). Генерация алертов.                       |
| **Guardian**      | Safety & Security           | Высший приоритет. Аварийная парковка (`EMERGENCY_PARK`) при порывах ветра или `UNSAFE`.         |
| **Diagnostician** | Root Cause Analysis         | Поиск корреляций (например, HFR vs Температура) и анализ похожих кейсов через RAG/LLM.          |
| **Strategist**    | Parameter Optimization      | Расчет оптимальной экспозиции (SNR), адаптация интервалов автофокуса, выбор подветренных целей. |
| **Auditor**       | Post-Mortem Analysis        | Генерация `Session Digest`, индексация в RAG, расчет Quality Score.                             |
| **Calibrator**    | Calibration Management      | Контроль свежести мастеров (Bias/Dark/Flat) и температурного допуска.                           |
| **Scheduler**     | Session Planning            | Динамическое приоритизация целей в Dynamic Sequencer на основе видимости и погоды.              |
| **Copilot**       | Interactive Assistant       | Генерация пошаговых UI-гайдов для ручных шагов (2PA, OAG Focus, MessageBox).                    |
| **Orchestrator**  | Coordinator                 | Маршрутизация задач, управление очередями, приоритетами и Decision Audit.                       |

---

## 🛠️ Технологический стек

### Backend

- **Python 3.11+**
- **FastAPI** — современный async web framework
- **Uvicorn** — ASGI server
- **Pydantic V2** — валидация данных и settings management
- **Asyncio** — асинхронное программирование

### AI / Orchestration

- **LangGraph** — state-based workflow orchestration для сложных сценариев
- **Ollama** — локальный LLM сервер
  - `gemma4:31b-cloud` — основная модель (облачная, мощная)
  - `gemma4:e4b` — fallback модель (локальная, быстрая)
  - `nomic-embed-text` — эмбеддинги для RAG (768 dim)

### Databases

- **Qdrant** — векторная база данных для RAG-системы (хранение эмбеддингов сессий и документации)
- **InfluxDB 2.x** — time-series база данных для метрик (основной источник телеметрии)
- **SQLite** — Decision Audit Trail (хранение всех решений AI-агентов)

### Monitoring & Observability

- **Prometheus** — экспорт метрик Cortex + резервный источник метрик N.I.N.A.
- **WebSocket Broadcasting** — real-time события для Frontend

### Infrastructure

- **Docker Compose** — оркестрация контейнеров (Qdrant, InfluxDB)
- **HTTPX** — async HTTP client для N.I.N.A. API
- **Watchdog** — файловый monitoring
- **Astropy** — парсинг FITS-заголовков

---

## ⚙️ Установка и настройка

### 1. Предварительные требования

- **N.I.N.A.** с установленными плагинами:
  - `Advanced API` (v2.2.15+) — REST API и WebSocket
  - `Prometheus Exporter` (jewzaam) — метрики оборудования
  - `InfluxDB Exporter` (daleghent) — метрики в InfluxDB
- **Python 3.11+**
- **Docker Desktop** (для Qdrant и InfluxDB)
- **Ollama** (локальный LLM сервер)

### 2. Клонирование репозитория

```bash
git clone <repository-url>
cd nina-ai-cortex
```

### 3. Инфраструктура (Docker)

Запустите базы данных:

```bash
docker-compose up -d
```

Это запустит:

- **Qdrant** на порту `6333` (веб-интерфейс) и `6334` (gRPC)
- **InfluxDB** на порту `8086`

### 4. Установка и настройка Ollama

Скачайте и установите Ollama: https://ollama.ai/download

Загрузите необходимые модели:

```bash
# Эмбеддинги для RAG (обязательно)
ollama pull nomic-embed-text

# Fallback LLM модель (обязательно, быстрая локальная)
ollama pull gemma4:e4b

# Основная LLM модель (опционально, мощная облачная)
ollama pull gemma4:31b-cloud
```

Проверьте доступность моделей:

```bash
ollama list
```

### 5. Установка зависимостей Python

**Windows (автоматическая установка):**

```cmd
install_deps.bat
```

**Ручная установка:**

```bash
# Создание виртуального окружения
python -m venv venv

# Активация
# Windows:
venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

# Установка зависимостей
pip install -r backend/requirements.txt
```

### 6. Конфигурация

#### 6.1. Переменные окружения

Скопируйте шаблон и отредактируйте:

```bash
cp backend/.env.example backend/.env
```

Отредактируйте `backend/.env`:

```env
# InfluxDB токен (получить из InfluxDB UI: Data > Tokens)
INFLUXDB_TOKEN=your-influxdb-token-here

# Ollama конфигурация
OLLAMA_HOST=http://localhost:11434
LLM_PRIMARY_MODEL=gemma4:31b-cloud
LLM_FALLBACK_MODEL=gemma4:e4b
LLM_PRIMARY_TIMEOUT=30.0
LLM_FALLBACK_TIMEOUT=15.0
LLM_MAX_TOKENS=1500
LLM_TEMPERATURE=0.3
LLM_FALLBACK_ENABLED=true
```

#### 6.2. Настройки приложения

Отредактируйте `config/settings.yaml`:

```yaml
# Пути к файлам N.I.N.A. (ОБЯЗАТЕЛЬНО измените под свою систему!)
nina_environment:
  appdata_root: "C:\\Users\\YourUser\\AppData\\Local\\NINA"
  sessions_root: "C:\\YourPath\\Sessions"
  masters_root: "C:\\YourPath\\Masters"
  profiles_dir: "C:\\Users\\YourUser\\AppData\\Local\\NINA\\Profiles"
  logs_dir: "C:\\Users\\YourUser\\AppData\\Local\\NINA\\Logs"
  plugins_dir: "C:\\Users\\YourUser\\AppData\\Local\\NINA\\Plugins\\3.0.0"

# Сетевые подключения
network:
  nina_api_host: "http://localhost:1888"
  nina_ws_url: "ws://localhost:1888/v2/socket"
  prometheus_url: "http://localhost:9876"

# InfluxDB
influxdb:
  url: "http://localhost:8086"
  token: "${INFLUXDB_TOKEN}" # Берётся из .env
  org: "observatory"
  bucket: "nina_telemetry"

# Пороговые значения агентов (настройте под своё оборудование)
thresholds:
  watcher:
    hfr_increase_percent: 30.0
    rms_ra_critical: 2.0
    # ... другие пороги
```

#### 6.3. OpenAPI спецификация N.I.N.A. API

Скачайте актуальную спецификацию:

```bash
# Windows PowerShell
Invoke-WebRequest -Uri "https://christian-photo.github.io/github-page/projects/ninaAPI/v2/doc/api.json" -OutFile "config/nina_api_spec.json"

# Linux/macOS
curl -o config/nina_api_spec.json https://christian-photo.github.io/github-page/projects/ninaAPI/v2/doc/api.json
```

Или используйте уже скачанную `config/nina_api_spec.json` из репозитория.

---

## 🏃 Запуск системы

**Windows:**

```cmd
start_cortex.bat
```

**Linux/macOS:**

```bash
# Активация виртуального окружения
source venv/bin/activate

# Запуск сервера
cd backend
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Сервер запустится на `http://localhost:8000`.

### Доступные endpoints

- **Swagger UI (API Docs)**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **ReDoc (альтернативная документация)**: [http://localhost:8000/redoc](http://localhost:8000/redoc)
- **Prometheus Metrics**: [http://localhost:8000/metrics](http://localhost:8000/metrics)
- **Health Check**: [http://localhost:8000/health](http://localhost:8000/health)
- **WebSocket Endpoint**: `ws://localhost:8000/ws`

---

## 🔐 Безопасность

**Архитектурное решение**: N.I.N.A. AI Cortex предназначен для **локального использования** на ПК астронома в доверенной сети. Поэтому система использует упрощённую модель безопасности:

- ✅ **Без аутентификации**: все endpoints открыты в локальной сети
- ✅ **CORS whitelist**: доступ только с разрешённых доменов Frontend
- ✅ **HAL валидация**: защита от опасных команд на уровне оборудования
- ✅ **OpenAPI валидация**: автоматическая проверка параметров триггеров
- ✅ **Маскирование логов**: чувствительные данные (токены, пароли) маскируются как `***`

**Для production развёртывания** рекомендуется:

- Использовать reverse proxy (nginx, Traefik) с HTTPS
- Настроить firewall для ограничения доступа к порту 8000
- Разместить систему в изолированной сети обсерватории

---

## 🧪 Тестирование и Симуляция

Проект покрыт Unit, Integration и E2E тестами. Встроенный **Simulation Mode** позволяет тестировать реакции агентов на аномалии без реального телескопа.

### Запуск тестов

**Windows:**

```cmd
run_tests.bat
```

**Linux/macOS:**

```bash
source venv/bin/activate
cd backend
pytest tests/unit tests/integration -v
pytest tests/e2e -v
```

### Симуляция через API

Запустите симуляцию сессии:

```bash
curl -X POST "http://localhost:8000/api/v1/simulation/start?target=M31&frames=10"
```

Инжектируйте аномалию:

```bash
# Резкий рост HFR
curl -X POST "http://localhost:8000/api/v1/simulation/inject-anomaly?anomaly_type=hfr_spike"

# Потеря гидирования
curl -X POST "http://localhost:8000/api/v1/simulation/inject-anomaly?anomaly_type=guiding_lost"

# Safety Monitor UNSAFE
curl -X POST "http://localhost:8000/api/v1/simulation/inject-anomaly?anomaly_type=safety_unsafe"
```

**Ожидаемое поведение**:

1. `Watcher` детектирует аномалию → генерирует `ALERT`
2. `Diagnostician` анализирует root cause через RAG
3. `Guardian`/`Strategist` инициирует corrective action (Autofocus, Park, etc.)
4. Решение логируется в Decision Audit Trail

### Тестирование LangGraph Workflows

Запустите diagnostic workflow:

```bash
curl -X POST "http://localhost:8000/api/v1/langgraph/start?workflow_type=diagnostic&trigger_event=HFR_degradation"
```

Проверьте статус:

```bash
curl "http://localhost:8000/api/v1/langgraph/workflows"
```

---

## 📡 API Endpoints

### System

- `GET /health` — Health check
- `GET /metrics` — Prometheus metrics

### AI Agents

- `GET /api/v1/agents/status` — Статус всех агентов
- `POST /api/v1/agents/mode` — Установка режима работы
- `GET /api/v1/agents/decisions` — Последние решения агентов
- `POST /api/v1/agents/test-llm` — Тест LLM генерации

### Observatory State

- `GET /api/v1/observatory/state` — Полное состояние обсерватории
- `GET /api/v1/observatory/session-summary` — Сводка текущей сессии

### Metrics

- `GET /api/v1/metrics` — Текущие метрики
- `GET /api/v1/metrics/history?metric=hfr&limit=100` — История метрики

### Execution Layer

- `POST /api/v1/execution/trigger` — Вызов триггера
- `POST /api/v1/execution/variable` — Изменение глобальной переменной
- `GET /api/v1/triggers` — Список доступных триггеров

### LangGraph Workflows (НОВОЕ)

- `GET /api/v1/langgraph/types` — Типы workflow (diagnostic, post_mortem, adaptive)
- `GET /api/v1/langgraph/workflows` — Активные workflow
- `GET /api/v1/langgraph/workflow/{id}` — Статус конкретного workflow
- `POST /api/v1/langgraph/start` — Запуск нового workflow
- `POST /api/v1/langgraph/cancel/{id}` — Отмена workflow
- `GET /api/v1/langgraph/stats` — Статистика LangGraph оркестратора

### RAG Engine

- `POST /api/v1/rag/search` — Семантический поиск
- `GET /api/v1/rag/context?query=...` — Получение контекста для LLM
- `GET /api/v1/rag/stats` — Статистика RAG

### Shadow Engine

- `GET /api/v1/sequence/shadow` — Теневой граф секвенсора
- `GET /api/v1/sequence/state` — Текущее состояние выполнения

### Simulation

- `POST /api/v1/simulation/start` — Запуск симуляции
- `POST /api/v1/simulation/stop` — Остановка симуляции
- `POST /api/v1/simulation/inject-anomaly` — Инжект аномалии

### Decision Audit

- `GET /api/v1/audit/decisions` — История решений
- `GET /api/v1/audit/stats` — Статистика audit trail

Полная документация доступна в Swagger UI: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## 📂 Структура проекта

```
nina-ai-cortex/
├── backend/
│   ├── app/
│   │   ├── agents/                      # 9 AI-агентов + оркестраторы
│   │   │   ├── orchestrator.py          # Event-Driven Orchestrator
│   │   │   ├── hybrid_langgraph_orchestrator.py  # LangGraph Orchestrator (НОВОЕ)
│   │   │   ├── watcher_agent.py        # Monitor & Anomaly Detection
│   │   │   ├── guardian_agent.py        # Safety & Security
│   │   │   ├── diagnostician_agent.py   # Root Cause Analysis
│   │   │   ├── strategist_agent.py      # Parameter Optimization
│   │   │   ├── auditor_agent.py         # Post-Mortem Analysis
│   │   │   ├── calibrator_agent.py      # Calibration Management
│   │   │   ├── scheduler_agent.py       # Session Planning
│   │   │   ├── copilot_agent.py         # Interactive Assistant
│   │   │   ├── base_agent.py            # Базовый класс агентов
│   │   │   ├── observatory_state.py     # Единое состояние обсерватории
│   │   │   ├── llm_client.py            # LLM клиент
│   │   │   └── llm_provider.py          # LLM провайдер (Ollama)
│   │   │
│   │   ├── core/                        # Ядро системы
│   │   │   ├── config.py                # Конфигурация (Pydantic Settings)
│   │   │   ├── events.py                # EventBus
│   │   │   ├── rag_engine.py            # RAG система (Qdrant)
│   │   │   ├── embeddings.py            # Эмбеддинги (Ollama)
│   │   │   ├── metrics.py               # Prometheus метрики
│   │   │   ├── mode_manager.py          # Управление режимами
│   │   │   ├── ws_broadcast.py          # WebSocket broadcasting
│   │   │   └── capability_registry.py   # Реестр возможностей плагинов
│   │   │
│   │   ├── execution/                   # Исполнительный слой
│   │   │   ├── trigger_emulator.py      # OpenAPI-driven триггеры (НОВОЕ)
│   │   │   ├── openapi_client.py        # OpenAPI клиент (НОВОЕ)
│   │   │   ├── nina_client.py           # N.I.N.A. API клиент
│   │   │   ├── global_var_injector.py   # Инъекция глобальных переменных
│   │   │   ├── hal.py                   # Hardware Abstraction Layer
│   │   │   ├── python_bridge.py         # Python скрипты в N.I.N.A.
│   │   │   ├── safety_interceptor.py    # Перехват Shutdown
│   │   │   ├── device_commander.py      # Команды оборудованию
│   │   │   └── dynamic_editor.py        # Редактор Dynamic Sequencer
│   │   │
│   │   ├── ingestion/                   # Сбор данных
│   │   │   ├── watchers/                # Файловые вотчеры
│   │   │   │   ├── session_watcher.py
│   │   │   │   ├── masters_auditor.py
│   │   │   │   ├── hocus_focus_watcher.py
│   │   │   │   ├── livestack_watcher.py
│   │   │   │   ├── log_tailer.py
│   │   │   │   ├── websocket_client.py
│   │   │   │   └── ...
│   │   │   ├── parsers/                 # Парсеры данных
│   │   │   │   ├── fits_header.py
│   │   │   │   ├── hocus_focus.py
│   │   │   │   ├── prometheus_metrics.py
│   │   │   │   └── session_metadata.py
│   │   │   ├── providers/               # Поставщики данных
│   │   │   │   └── influxdb_metrics.py
│   │   │   └── subscribers/             # Подписчики
│   │   │       └── influxdb_subscriber.py
│   │   │
│   │   ├── shadow_engine/               # Теневой движок
│   │   │   ├── sequence_parser.py       # Парсер Sequence.json
│   │   │   └── state_tracker.py         # Трекер состояния
│   │   │
│   │   ├── simulation/                  # Симуляция
│   │   │   ├── fake_nina.py             # Эмулятор N.I.N.A.
│   │   │   └── fake_phd2.py             # Эмулятор PHD2
│   │   │
│   │   ├── safety/                      # Безопасность
│   │   │   └── preflight.py             # Pre-flight checklist
│   │   │
│   │   ├── storage/                     # Хранилище
│   │   │   ├── decision_audit.py        # Decision Audit Trail (SQLite)
│   │   │   └── disk_monitor.py          # Мониторинг диска
│   │   │
│   │   └── main.py                      # FastAPI приложение
│   │
│   ├── data/                            # Данные (runtime)
│   │   ├── decision_audit.db            # SQLite БД решений
│   │   └── vector_db/                   # Локальная vector DB (fallback)
│   │
│   ├── tests/                           # Тесты
│   │   ├── unit/                        # Unit тесты
│   │   ├── integration/                 # Integration тесты
│   │   ├── e2e/                         # End-to-End тесты
│   │   └── fixtures/                    # Тестовые данные
│   │
│   ├── scripts/                         # Вспомогательные скрипты
│   │   └── analyze_nina_api.py          # Анализатор OpenAPI spec
│   │
│   ├── requirements.txt                 # Python зависимости
│   ├── pytest.ini                       # Pytest конфигурация
│   └── .env.example                     # Шаблон переменных окружения
│
├── config/                              # Конфигурация
│   ├── settings.yaml                    # Основные настройки
│   ├── nina_api_spec.json               # OpenAPI спецификация N.I.N.A.
│   └── nina_trigger_mapping.json        # Маппинг триггеров
│
├── docs/                                # Документация
│   └── nina_api_reference.md            # Справочник API
│
├── docker-compose.yml                   # Docker конфигурация
├── install_deps.bat                     # Скрипт установки (Windows)
├── start_cortex.bat                     # Скрипт запуска (Windows)
├── run_tests.bat                        # Скрипт тестов (Windows)
└── README.md                            # Этот файл
```

---

## 🔄 Режимы работы

Система поддерживает 4 режима работы:

### 1. `FULL_AI` (по умолчанию)

- Все 9 агентов активны
- LLM используется для сложного анализа
- Полная автономность

### 2. `SAFE_AUTONOMOUS`

- Активны только `Watcher` и `Guardian`
- LLM отключён (fallback при потере связи)
- Только критические реакции

### 3. `MANUAL`

- Только мониторинг, без автодействий
- Все триггеры заблокированы
- Для отладки и наблюдения

### 4. `SIMULATION`

- Режим симуляции с `FakeNina` и `FakePhd2`
- Для тестирования без реального оборудования
- Инжект аномалий через API

Переключение режимов:

```bash
curl -X POST "http://localhost:8000/api/v1/agents/mode?mode=safe"
```

---

## 📊 Мониторинг

### Prometheus Metrics

Cortex экспортирует метрики в Prometheus формате на `/metrics`:

```bash
curl http://localhost:8000/metrics
```

**Основные метрики**:

- `cortex_events_total` — количество обработанных событий
- `cortex_decisions_total` — количество решений агентов
- `cortex_llm_requests_total` — запросы к LLM
- `cortex_api_requests_total` — HTTP запросы к API
- `cortex_triggers_fired_total` — срабатывания триггеров
- `cortex_active_ws_connections` — активные WebSocket подключения
- `cortex_sequence_running` — статус выполнения секвенсора
- `cortex_safety_status` — статус Safety Monitor

### Grafana Dashboard

Импортируйте метрики в Grafana для визуализации:

1. Добавьте Prometheus data source: `http://localhost:8000/metrics`
2. Создайте dashboard с метриками Cortex
3. Настройте алерты на критические события

---

## 🐛 Troubleshooting

### Проблема: `Cannot connect to Ollama`

**Решение**:

```bash
# Проверьте, что Ollama запущен
ollama list

# Запустите Ollama если не запущен
# Windows: Ollama должен быть в системном трее
# Linux/macOS: ollama serve
```

### Проблема: `InfluxDB 401 Unauthorized`

**Решение**:

1. Получите токен из InfluxDB UI: http://localhost:8086 → Data → Tokens
2. Обновите `backend/.env`:
   ```env
   INFLUXDB_TOKEN=your-actual-token-here
   ```
3. Перезапустите Cortex

### Проблема: `N.I.N.A. API not reachable`

**Решение**:

1. Убедитесь, что N.I.N.A. запущена
2. Проверьте, что Advanced API плагин установлен и включён
3. Проверьте порт в `config/settings.yaml`:
   ```yaml
   network:
     nina_api_host: "http://localhost:1888"
   ```

### Проблема: `Qdrant connection failed`

**Решение**:

```bash
# Проверьте, что Qdrant запущен
docker ps | grep qdrant

# Перезапустите если нужно
docker-compose restart qdrant
```

---

## 📝 Changelog

### v3.0.0 (2026-07-09) — Major Refactoring

**Архитектурные изменения**:

- ✅ Добавлен **Hybrid LangGraph Orchestrator** для сложных workflow
- ✅ Реализована **Dual Orchestrator System** (Event-Driven + LangGraph)
- ✅ Переписан **Trigger Emulator** на OpenAPI-driven архитектуру
- ✅ Добавлена автоматическая валидация параметров из OpenAPI spec

**Удалённые компоненты** (упрощение для локального использования):

- ❌ JWT аутентификация (`auth.py`)
- ❌ Credential Vault (`vault.py`)
- ❌ Rate limiting (`slowapi`, `limits`)
- ❌ Home Assistant bridge (`home_assistant_bridge.py`)
- ❌ External launcher (`external_launcher.py`)
- ❌ Memory Manager agent (`memory_manager_agent.py`)

**Упрощения**:

- ✅ Убрана избыточная безопасность (JWT, API keys, Vault)
- ✅ Все endpoints открыты в локальной сети
- ✅ Упрощена конфигурация (убраны секции auth, security)

**Новые возможности**:

- ✅ LangGraph workflow endpoints (`/api/v1/langgraph/*`)
- ✅ OpenAPI-driven trigger validation
- ✅ Динамическое построение реестра триггеров
- ✅ LRU-кэш эмбеддингов (10000 записей)
- ✅ Prometheus метрики Cortex

**Оптимизации**:

- ✅ Удалены неиспользуемые зависимости
- ✅ Уменьшен размер кодовой базы
- ✅ Улучшена производительность RAG (кэширование)

### v2.0.0 (2026-07-07) — Initial Release

- ✅ 10 AI-агентов (Multi-Agent Swarm)
- ✅ Shadow Engine (Sequence.json парсинг)
- ✅ RAG система (Qdrant + Ollama)
- ✅ Dual Telemetry (InfluxDB + Prometheus)
- ✅ Decision Audit Trail (SQLite)
- ✅ Simulation Mode (FakeNina, FakePhd2)
- ✅ HAL (Hardware Abstraction Layer)
- ✅ WebSocket broadcasting

---

## 🤝 Contributing

Приветствуются:

- 🐛 Bug reports
- 💡 Feature requests
- 🔧 Pull requests
- 📖 Documentation improvements

---

## 📄 License

[Your License Here]

---

## 🙏 Acknowledgments

- **N.I.N.A.** — Nighttime Imaging 'N' Astronomy (https://nighttime-imaging.eu/)
- **Advanced API Plugin** by Christian Photo
- **Prometheus Exporter** by jewzaam
- **InfluxDB Exporter** by daleghent
- **Ollama** — Local LLM server (https://ollama.ai/)
- **LangGraph** — State-based workflow orchestration
- **Qdrant** — Vector database (https://qdrant.tech/)
- **FastAPI** — Modern web framework (https://fastapi.tiangolo.com/)

---

## 📞 Support

- **Issues**: [GitHub Issues](your-repo-url/issues)
- **Discussions**: [GitHub Discussions](your-repo-url/discussions)
- **N.I.N.A. Discord**: [discord.gg/nina](https://discord.gg/nina)

---

**Made with ❤️ for astrophotographers**

Тесты
