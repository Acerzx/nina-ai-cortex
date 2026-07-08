# 🌌 N.I.N.A. AI Cortex 🧠

**N.I.N.A. AI Cortex** — это интеллектуальная когнитивная надстройка и многоагентная система (Multi-Agent Swarm) для автономного управления, мониторинга и оптимизации астрономической обсерватории под управлением программного комплекса **N.I.N.A.** (Nighttime Imaging 'N' Astronomy).

Система в реальном времени анализирует телеметрию, логи, метрики качества (HFR, FWHM, RMS) и погодные условия, принимая автономные решения для максимизации результата съемки и обеспечения абсолютной безопасности оборудования.

---

## 🚀 Ключевые возможности

- **🤖 Multi-Agent Swarm (10 AI-агентов)**: Специализированные агенты для мониторинга, диагностики, оптимизации и обеспечения безопасности, координируемые через LangGraph.
- **👁️ Shadow Engine**: Неинвазивный парсинг `Sequence.json` N.I.N.A. для построения теневого графа (DAG) и предсказания действий секвенсора без вмешательства в его работу.
- **🛡️ Hardware Abstraction Layer (HAL)**: Финальный барьер безопасности, блокирующий опасные команды (slew, triggers) во время критических фаз (Meridian Flip, Park, Shutdown) или при `UNSAFE` статусе.
- **📚 RAG-система (Retrieval-Augmented Generation)**: Векторная база знаний (Qdrant) для обучения на истории прошлых сессий, документаций и генерации Post-Mortem отчетов.
- **🔌 Deep N.I.N.A. Integration**: Двусторонняя связь через Advanced API (HTTP) и WebSocket. Поддержка триггеров, инъекции глобальных переменных и управления Dynamic Sequencer.
- **📊 Dual Telemetry Ingestion**: Агрегация метрик из InfluxDB 2.x (основной источник) и Prometheus (резервный), парсинг FITS-заголовков, CSV-отчетов (Hocus Focus) и логов.
- **🏠 Smart Observatory (Home Assistant)**: Мост для управления умным домом обсерватории (Flat Panel, Dew Heater, питание).
- **🎭 Simulation Mode**: Встроенные эмуляторы `FakeNina` и `FakePhd2` для безопасного тестирования агентов и инжекта аномалий без реального оборудования.
- **📜 Decision Audit Trail**: Полная объяснимость ИИ (Explainable AI) с сохранением всех решений в SQLite, оценкой постфактум (Hindsight Verdict) и политиками ретеншена.

---

## 🏗️ Архитектура системы

Система построена на паттерне **Orchestrator-Worker** с асинхронным EventBus ядром.

### 1. Ingestion Layer (Сбор данных)

- **Watchers**: Мониторинг файловых систем (Session Metadata, Masters Library, Hocus Focus, LiveStack, AI Weather).
- **Pollers**: Опрос InfluxDB (Flux queries) и Prometheus Exporter.
- **Log Tailer**: Стриминг и классификация логов N.I.N.A. в реальном времени (Regex-паттерн матчинг).
- **WebSocket Client**: Подписка на нативные события N.I.N.A. (`SequenceItemStarted`, `MeridianFlip`, и т.д.).

### 2. Core (Ядро)

- **EventBus**: Асинхронная шина событий с метриками и дедупликацией.
- **ObservatoryState**: Единый in-memory стейт обсерватории (агрегатор всех источников).
- **Mode Manager**: Управление режимами (`FULL_AI`, `SAFE_AUTONOMOUS`, `MANUAL`, `SIMULATION`) с автоматическим fallback при потере LLM.
- **RAG Engine**: Гибридные эмбеддинги (Ollama `nomic-embed-text` + LRU-кэш) и векторный поиск.

### 3. Execution Layer (Исполнение)

- **Trigger Emulator**: Безопасный вызов триггеров N.I.N.A. API с валидацией параметров и защитой от перезаписи.
- **Global Var Injector**: Изменение переменных Sequencer+ с маскированием чувствительных данных в логах.
- **Python Bridge**: Whitelist-выполнение IronPython/C# скриптов внутри N.I.N.A. (защита от произвольного кода).
- **Safety Interceptor**: Перехват инструкций `ShutdownPc` для предотвращения внезапного отключения ПК.

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
| **MemoryManager** | Context Management          | Управление краткосрочной и долгосрочной памятью (TTL, очистка).                                 |
| **Orchestrator**  | Coordinator                 | Маршрутизация задач, управление очередями, приоритетами и Decision Audit.                       |

---

## 🛠️ Технологический стек

- **Backend**: Python 3.11+, FastAPI, Uvicorn, Pydantic V2, Asyncio.
- **AI / Orchestration**: LangGraph, LangChain, Ollama (`gemma4:31b-cloud` + `gemma4:e4b` fallback).
- **Vector DB**: Qdrant (Хранение эмбеддингов сессий и документации).
- **Time-Series DB**: InfluxDB 2.x (Основной источник телеметрии).
- **Metrics**: Prometheus (Экспорт метрик самого Cortex + резервный источник N.I.N.A.).
- **Storage**: SQLite (Decision Audit Trail), Argon2id + AES-256-GCM (Credential Vault).
- **Infrastructure**: Docker Compose, WebSockets, HTTPX.

---

## ⚙️ Установка и настройка

### 1. Предварительные требования

- **N.I.N.A.** с установленными плагинами: `Advanced API`, `Prometheus Exporter` (jewzaam), `InfluxDB Exporter` (daleghent).
- **Python 3.11+**
- **Docker Desktop** (для Qdrant и InfluxDB)
- **Ollama** (локальный LLM сервер)

### 2. Инфраструктура (Docker)

Запустите базы данных:

```bash
docker-compose up -d
```

### 3. Загрузка LLM моделей (Ollama)

```bash
ollama pull nomic-embed-text      # Для RAG (Embeddings)
ollama pull gemma4:e4b            # Fallback модель (Быстрая, локальная)
# Опционально: ollama pull gemma4:31b-cloud (Облачная/Мощная)
```

### 4. Установка зависимостей Python

Запустите скрипт автоматической установки (Windows):

```cmd
install_deps.bat
```

_Или вручную:_

```bash
python -m venv venv
venv\Scripts\activate
pip install -r backend/requirements.txt
```

### 5. Конфигурация

1. Скопируйте `backend/.env.example` в `backend/.env` и укажите токены (InfluxDB, Home Assistant, JWT Secret).
2. Отредактируйте `config/settings.yaml`:
   - Укажите актуальные пути к папкам N.I.N.A. (`appdata_root`, `sessions_root`, `masters_root`).
   - Настройте пороги срабатывания агентов (`thresholds`).
   - Укажите сетевые адреса API.

---

## 🏃 Запуск системы

```cmd
start_cortex.bat
```

Сервер запустится на `http://localhost:8000`.

- **Swagger UI (API Docs)**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Prometheus Metrics**: [http://localhost:8000/metrics](http://localhost:8000/metrics)
- **WebSocket Endpoint**: `ws://localhost:8000/ws`

---

## 🔐 Безопасность и Аутентификация

- **JWT & API Keys**: Все критические endpoints (Execution, Vault, Simulation) защищены JWT Bearer токенами или API-ключами (RBAC: Admin, Operator, Readonly).
- **Rate Limiting**: Защита от перегрузки LLM и оборудования через `slowapi`.
- **Credential Vault**: Безопасное хранение секретов (токенов, паролей) с использованием Argon2id и AES-256-GCM.
- **CORS**: Строгий whitelist доменов фронтенда.
- **Маскирование**: Чувствительные данные (токены, пароли) автоматически маскируются в логах (`***`).

---

## 🧪 Тестирование и Симуляция

Проект покрыт Unit, Integration и E2E тестами. Встроенный **Simulation Mode** позволяет тестировать реакции агентов на аномалии без реального телескопа.

Запуск тестов:

```cmd
run_tests.bat
```

**Пример инжекта аномалии через API (в режиме симуляции):**

```http
POST /api/v1/simulation/inject-anomaly?anomaly_type=hfr_spike
```

_Ожидаемое поведение: Watcher детектирует аномалию -> Diagnostician находит причину -> Guardian/Strategist инициирует Autofocus через Trigger Emulator._

---

## 📂 Структура проекта (Кратко)

```text
backend/app/
├── agents/          # 10 AI-агентов, Orchestrator, LLM Client
├── core/            # EventBus, Config, RAG, Metrics, ModeManager
├── execution/       # HAL, Trigger Emulator, N.I.N.A. API Client
├── ingestion/       # Watchers, Parsers (FITS, CSV), InfluxDB/Prometheus
├── safety/          # Pre-flight gates
├── security/        # Auth (JWT), Vault
├── shadow_engine/   # Sequence Parser, State Tracker
├── simulation/      # Fake N.I.N.A., Fake PHD2
└── storage/         # Decision Audit (SQLite), Disk Monitor
```
