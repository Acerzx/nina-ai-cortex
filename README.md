# 📝 Создание README.md для N.I.N.A. AI Cortex

На основе анализа предоставленного кода и структуры проекта, я подготовил полноценную документацию по ключевым характеристикам системы.

```markdown
# 🌌 N.I.N.A. AI Cortex

**Когнитивная надстройка для автономного управления астрофотографической обсерваторией**

N.I.N.A. AI Cortex — это Multi-Agent AI система, работающая поверх [N.I.N.A.](https://nighttime-imaging.eu/) (Nighttime Imaging 'N' Astronomy). Система обеспечивает полностью автономный мониторинг, диагностику и оптимизацию астрономических сессий в реальном времени.

---

## 🎯 Ключевые возможности

### 🤖 Multi-Agent Swarm (10 AI-агентов)

Архитектура **Orchestrator-Worker Pattern** с координацией через LangGraph:

| Агент             | Роль                                                            | Приоритет |
| ----------------- | --------------------------------------------------------------- | --------- |
| **Watcher**       | Мониторинг метрик и детекция аномалий (HFR, FWHM, RMS)          | HIGH      |
| **Guardian**      | Безопасность оборудования (аварийная парковка, контроль погоды) | CRITICAL  |
| **Diagnostician** | Root cause analysis через корреляции и RAG-историю              | HIGH      |
| **Strategist**    | Оптимизация параметров (экспозиция, SNR, автофокус)             | MEDIUM    |
| **Scheduler**     | Планирование сессий на основе видимости и погоды                | MEDIUM    |
| **Auditor**       | Post-mortem анализ и генерация Session Digest                   | LOW       |
| **Calibrator**    | Управление библиотекой мастер-кадров (Bias/Dark/Flat)           | LOW       |
| **Copilot**       | Интерактивная помощь (MessageBox, 2PA, OAG Focus)               | INFO      |
| **MemoryManager** | Управление контекстом (short/medium/long-term)                  | -         |
| **Orchestrator**  | Координация workflow и маршрутизация решений                    | -         |

**Принцип приоритетов**: `Safety > Quality > Optimization`

### 📊 Ingestion Layer (Мониторинг данных)

Многоуровневая система сбора данных из различных источников:

- **11 File Watchers**: SessionWatcher, HocusFocusWatcher, FITSScanner, MastersAuditor, LiveStackWatcher и др.
- **Prometheus Scraper**: Парсинг метрик оборудования (jewzaam plugin)
- **InfluxDB Provider**: Основной источник time-series данных через Flux queries
- **WebSocket Client**: Real-time события от N.I.N.A. API
- **Log Tailer**: Анализ логов N.I.N.A. с паттерн-матчингом

### 🧠 Shadow Engine

**Теневой граф секвенсора** — полная реконструкция N.I.N.A. Sequence.json:

- Парсинг всех контейнеров, инструкций, триггеров и условий
- Отслеживание `container_path` для определения текущей фазы
- Раннее обнаружение FLAT_MODE и приближения к Shutdown
- Блокировка небезопасных действий в критических фазах

### ⚡ Execution Layer

Система выполнения команд через N.I.N.A. Advanced API:

- **Trigger Emulator v2**: Эмуляция триггеров через реальные эндпоинты API
- **HAL (Hardware Abstraction Layer)**: Финальная валидация команд (лимиты высоты, safety status)
- **Safety Interceptor**: Перехват Shutdown инструкций в финальной стадии
- **Device Commander**: Прямые ASCOM команды оборудованию
- **Dynamic Editor**: Безопасное редактирование JSON-проектов секвенсора
- **Global Var Injector**: Изменение переменных Sequencer+
- **Home Assistant Bridge**: Интеграция с умным домом обсерватории

### 🔍 RAG Engine (Retrieval-Augmented Generation)

Система предоставления контекста AI-агентам:

- **Qdrant**: Векторная база данных для семантического поиска
- **Гибридные Embeddings**: sentence-transformers (primary) → Ollama nomic-embed-text (fallback)
- **Автоматическое пополнение**: Индексация Session Digest после каждой сессии
- **Контекст для LLM**: Исторические кейсы, документация, решения проблем

### 🛡️ Safety & Security

- **Pre-flight Checklist**: 8 gates перед стартом сессии (Weather, Hardware, Calibration, DiskSpace, API, Safety, Sequence, Mode)
- **Credential Vault**: Безопасное хранение секретов (Argon2id + AES-256-GCM)
- **Mode Manager**: Graceful degradation при потере LLM (FULL_AI → SAFE_AUTONOMOUS → MANUAL)
- **Decision Audit Trail**: Полное логирование всех AI-решений с hindsight verdict

### 💾 Storage & Monitoring

- **Decision Audit Trail**: SQLite база всех решений агентов
- **Disk Monitor**: Автоматическое управление дисковым пространством с политиками retention
- **WebSocket Broadcasting**: Real-time push событий на Frontend (каналы: sequence, metrics, alerts, weather)

### 🎭 Simulation Mode

Полная эмуляция для тестирования без реального оборудования:

- **Fake NINA API**: Генерация реалистичных метрик и событий
- **Fake PHD2**: Симуляция гидирования
- **Инжект аномалий**: hfr_spike, rms_spike, temp_drift, guiding_lost, safety_unsafe

---

## 🏗️ Архитектура системы
```

┌─────────────────────────────────────────────────────────────────┐
│ N.I.N.A. AI Cortex │
├─────────────────────────────────────────────────────────────────┤
│ ┌─────────────┐ ┌──────────────┐ ┌───────────────────────┐ │
│ │ FastAPI │ │ WebSocket │ │ LangGraph │ │
│ │ REST API │ │ Broadcast │ │ Orchestrator │ │
│ └──────┬──────┘ └──────┬───────┘ └───────────┬───────────┘ │
│ │ │ │ │
│ ┌──────▼────────────────▼──────────────────────▼───────────┐ │
│ │ AI Agents (10) │ │
│ │ Watcher │ Guardian │ Diagnostician │ Strategist │ ... │ │
│ └──────────────────────────┬───────────────────────────────┘ │
│ │ │
│ ┌──────────────────────────▼───────────────────────────────┐ │
│ │ ObservatoryState (Единое состояние) │ │
│ └──────────────────────────┬───────────────────────────────┘ │
│ │ │
│ ┌──────────┐ ┌───────────┴────┐ ┌─────────────────────┐ │
│ │ Shadow │ │ Execution │ │ RAG Engine │ │
│ │ Engine │ │ Layer │ │ (Qdrant + Ollama) │ │
│ └──────────┘ └───────────┬────┘ └─────────────────────┘ │
│ │ │
│ ┌─────────────────────────▼────────────────────────────────┐ │
│ │ Ingestion Layer │ │
│ │ File Watchers │ Prometheus │ InfluxDB │ WS Client │ Logs│ │
│ └─────────────────────────┬────────────────────────────────┘ │
└────────────────────────────┼────────────────────────────────────┘
│
┌────────▼────────┐
│ N.I.N.A. │
│ Advanced API │
│ (ASCOM) │
└────────┬────────┘
│
┌────────▼────────┐
│ Оборудование │
│ (Mount, Camera, │
│ Guider, etc.) │
└─────────────────┘

````

---

## 🚀 Быстрый старт

### Требования

- **Python**: 3.11+ (совместимо с 3.14)
- **Docker Desktop**: Для Qdrant и InfluxDB
- **N.I.N.A.**: С установленным Advanced API плагином
- **Ollama**: Локальный LLM сервер (опционально)

### Установка

```bash
# 1. Установка зависимостей
install_deps.bat

# 2. Настройка конфигурации
# Отредактируйте config/settings.yaml с путями к N.I.N.A.

# 3. Установка LLM модели (опционально)
ollama pull qwen2.5:14b

# 4. Запуск инфраструктуры
docker-compose up -d

# 5. Запуск Cortex
start_cortex.bat
````

### API Endpoints

| Эндпоинт                         | Описание                           |
| -------------------------------- | ---------------------------------- |
| `GET /health`                    | Health check всех компонентов      |
| `GET /api/v1/observatory/state`  | Полное состояние обсерватории      |
| `GET /api/v1/agents/status`      | Статус всех AI-агентов             |
| `GET /api/v1/metrics`            | Текущие метрики                    |
| `POST /api/v1/rag/search`        | Семантический поиск по базе знаний |
| `POST /api/v1/execution/trigger` | Ручной вызов триггера              |
| `WS /ws`                         | WebSocket для real-time событий    |
| `GET /docs`                      | Swagger UI документация            |

---

## 📁 Структура проекта

```
├── backend/app/
│   ├── agents/           # 10 AI-агентов + LangGraph
│   ├── core/             # EventBus, Config, RAG, ModeManager
│   ├── execution/        # Trigger Emulator, HAL, Safety
│   ├── ingestion/        # Watchers, Parsers, Providers
│   ├── safety/           # Pre-flight Checklist
│   ├── security/         # Credential Vault
│   ├── shadow_engine/    # Sequence Parser, State Tracker
│   ├── simulation/       # Fake NINA, Fake PHD2
│   └── storage/          # Decision Audit, Disk Monitor
├── config/               # settings.yaml, OpenAPI spec
├── tests/                # Unit, Integration, E2E тесты
└── docker-compose.yml    # Qdrant + InfluxDB
```

---

## 🔧 Конфигурация

### Основные настройки (config/settings.yaml)

```yaml
nina_environment:
  appdata_root: "C:\\Users\\...\\AppData\\Local\\NINA"
  sessions_root: "C:\\...\\Sessions"
  masters_root: "C:\\...\\Masters"

network:
  nina_api_host: "http://localhost:1888"
  prometheus_url: "http://localhost:9876"

influxdb:
  url: "http://localhost:8086"
  token: "${INFLUXDB_TOKEN}"

ai_settings:
  ollama_host: "http://localhost:11434"
  model_name: "qwen2.5:14b"
```

### Режимы работы

| Режим             | Описание                                   |
| ----------------- | ------------------------------------------ |
| `FULL_AI`         | Все агенты активны, LLM работает           |
| `SAFE_AUTONOMOUS` | Только Watcher + Guardian (при потере LLM) |
| `MANUAL`          | Только мониторинг, без автодействий        |
| `SIMULATION`      | Тестирование с Fake NINA/PHD2              |

---

## 🧪 Тестирование

```bash
# Запуск всех тестов
run_tests.bat

# Unit тесты
pytest tests/unit -v

# Integration тесты
pytest tests/integration -v

# E2E тесты (симуляция)
pytest tests/e2e -v
```

---

## 📊 Примеры сценариев

### Сценарий 1: Детекция аномалии HFR

```
1. Watcher замечает рост HFR на 35% за 5 кадров
2. Diagnostician анализирует корреляции → "Температурный дрейф фокуса"
3. Guardian запускает автофокус через Trigger Emulator
4. HFR возвращается к норме → решение помечено как CORRECT
```

### Сценарий 2: Критические погодные условия

```
1. Watcher детектирует ветер > 20 м/с
2. Guardian выполняет EMERGENCY_PARK монтировки
3. Публикуется CRITICAL ALERT на Frontend
4. Mode Manager переводит систему в SAFE_AUTONOMOUS
```

### Сценарий 3: Post-mortem анализ

```
1. Auditor генерирует Session Digest после завершения сессии
2. Digest индексируется в RAG для будущего обучения
3. LLM создаёт расширенный отчёт с рекомендациями
4. Copilot показывает рекомендации пользователю
```

---

## 🔗 Интеграции

- **N.I.N.A. Advanced API**: Полное управление через REST/WebSocket
- **Prometheus Exporter** (jewzaam): Метрики оборудования
- **InfluxDB**: Time-series хранение метрик
- **Qdrant**: Векторная база для RAG
- **Ollama**: Локальные LLM модели
- **Home Assistant**: Управление умным домом
- **PHD2**: Гидирование через интеграцию

---

## 📚 Документация

- [N.I.N.A. API Reference](docs/nina_api_reference.md)
- [OpenAPI Specification](config/nina_api_spec.yaml)
- [Trigger Mapping](config/nina_trigger_mapping.json)

---

## 🤝 Лицензия

Проект разрабатывается для персонального использования в астрофотографии.

---

## ⭐ Ключевые метрики проекта

| Показатель               | Значение |
| ------------------------ | -------- |
| AI-агентов               | 10       |
| File Watchers            | 11+      |
| API Endpoints            | 50+      |
| Поддерживаемых триггеров | 20+      |
| Pre-flight gates         | 8        |
| Тестовое покрытие        | >80%     |

---

**N.I.N.A. AI Cortex** — _Ваш автономный помощник для идеальных астрофотографических сессий_ 🌠

```

---

Документация охватывает все ключевые аспекты проекта:
- Архитектуру Multi-Agent Swarm
- Ingestion/Execution слои
- Систему безопасности и graceful degradation
- Инструкции по установке и запуску
- Примеры сценариев использования
```
