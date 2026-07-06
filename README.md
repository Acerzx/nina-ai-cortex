# 📋 N.I.N.A. AI CORTEX: ПОЛНАЯ СПЕЦИФИКАЦИЯ ПРОЕКТА v2.0

---

## 1. EXECUTIVE SUMMARY

### 1.1. Концепция
**N.I.N.A. AI Cortex** — это когнитивная надстройка (overlay) над экосистемой N.I.N.A., превращающая её из мощного, но "слепого" инструмента в **интеллектуальную автономную систему** с Multi-Agent AI архитектурой, способную:
- **Понимать** физику астрофотографического процесса
- **Анализировать** корреляции между метриками качества, погодой и оборудованием
- **Принимать** автономные решения на основе предиктивной аналитики
- **Обучаться** на истории сессий через RAG-систему
- **Интегрироваться** с постобработкой (Siril) для замкнутого цикла качества

### 1.2. Ключевая философия
> *«Мы не изобретаем велосипед — мы создаём мозг для уже существующего двигателя»*

- Cortex **не заменяет** N.I.N.A. и **не дублирует** её функции
- Всё управление оборудованием **делегируется** N.I.N.A. через официальные API
- Cortex **читает следы** (логи, метаданные, телеметрию, FITS-хедеры) и воздействует только через безопасные интерфейсы
- Система работает как **цифровой двойник** сессии с предиктивной аналитикой
- **Полное покрытие** всех 72 установленных плагинов через динамический реестр

### 1.3. Гарантированное покрытие
Система **взаимодействует со всеми 72 плагинами** через:
- **Plugin Registry** (динамическое обнаружение через сканирование профиля и папки плагинов)
- **File Watchers** для всех экспортов (JSON, CSV, FITS headers)
- **WebSocket events** в реальном времени от ninaAPI
- **REST API** для управления (Advanced API)
- **InfluxDB/Prometheus** для агрегированных метрик
- **FITS Header Scanner** для астрономических данных (WCS, MOONANGL, SUNANGLE)

### 1.4. Критические отличия от предыдущих версий
- **ЗАПРЕТ на упрощения**: каждый модуль должен быть реализован полностью, без сокращений
- **Конфигурируемость**: все пути вынесены в `settings.yaml`, система должна работать на любом ПК
- **Масштабируемость**: архитектура поддерживает добавление новых плагинов без изменения ядра
- **Безопасность**: Safety Interceptor для предотвращения Shutdown PC, HAL для валидации команд
- **Интеграция с Siril**: автоматический анализ качества постобработки для замкнутого цикла

---

## 2. АРХИТЕКТУРНЫЕ ПРИНЦИПЫ (ЖЁСТКИЕ ПРАВИЛА)

### 2.1. Read-Only Ingestion (сбор данных без вмешательства)
Cortex **никогда не опрашивает** N.I.N.A. частыми REST-запросами. Вместо этого:
- **Файловая система**: профили (`%LOCALAPPDATA%\NINA\Profiles\`), логи (`%LOCALAPPDATA%\NINA\Logs\`), метаданные сессий, FITS-заголовки
- **Сетевые протоколы**: WebSocket (события), HTTP (Prometheus `localhost:9876/metrics`), InfluxDB (Flux queries)
- **Внутренние данные**: настройки плагинов из `<pluginStorage>`, библиотеки мастер-кадров
- **Паттерн-матчинг**: извлечение метрик из имен FITS-файлов (ExposureTime, Filter, Gain, Offset, RMS, HFR, FWHM, Stars)

### 2.2. Safe Injection (безопасное влияние)
Cortex **никогда не изменяет** `Sequence.json` напрямую. Воздействие происходит через:
- **Advanced API**: изменение глобальных переменных (`SetGlobalVariable`), эмуляция триггеров (`FireTrigger`)
- **nina.plugin.python**: выполнение Python-скриптов внутри N.I.N.A. для сложной логики
- **nina.external**: запуск batch/PowerShell скриптов для интеграции с внешними инструментами
- **Device Commands**: вызов ASCOM `Action()`, `CommandBool()`, `CommandString()` через Advanced API
- **Встроенные механизмы безопасности** N.I.N.A. (Safety Monitor, лимиты монтировки, Meridian Flip logic)

### 2.3. Event-Driven Shadow Engine
- При старте парсится `Sequence.json` → строится **теневой граф (DAG)** с разрешением `$ref` ссылок
- WebSocket-события (`SequenceItemStarted`, `SequenceItemCompleted`) сопоставляются с узлами графа
- Система **точно знает**, какой шаг выполняется в текущий момент
- Позволяет **перехватывать критичные точки** (MessageBox, ShutdownPcInstruction) и прогнозировать события
- **Инъекции** происходят только через глобальные триггеры (InjectAutofocusTrigger, PHD2Tools triggers)

### 2.4. Масштабируемость и расширяемость
- **Plugin Registry** динамически подгружает ридеры для новых плагинов на основе GUID из профиля
- **MCP-серверы** подключаются как внешние инструменты для AI (Siril, каталоги Simbad/Vizier)
- **RAG-база знаний** пополняется автоматически после каждой сессии (`Session_Digest.md`)
- **Frontend** поддерживает плагинную архитектуру виджетов
- **Конфигурация** полностью вынесена в `settings.yaml` — система должна работать на любом ПК без хардкода

---

## 3. ПОЛНАЯ КАРТА ЭКОСИСТЕМЫ N.I.N.A. (72 ПЛАГИНА)

### 3.1. Категория A: Core Infrastructure (5 плагинов)

| # | Плагин | Автор | GUID | Назначение | Места хранения данных | Способы получения | Статус |
|---|--------|-------|------|------------|----------------------|-------------------|--------|
| 1 | **N.I.N.A. Core** | isbeorn | - | Основной софт астрофотографии | `%LOCALAPPDATA%\NINA\Profiles\` (XML профили)<br>`%LOCALAPPDATA%\NINA\Logs\` (логи)<br>`%LOCALAPPDATA%\NINA\Plugins\3.0.0\` (плагины) | REST API, WebSocket, File System | ✅ Core |
| 2 | **ninaAPI** | christian-photo | - | Базовое API v2 + WebSocket events | Нет собственного хранилища | `http://localhost:1888/api/v2/*` (REST)<br>`ws://localhost:1888/v2/socket` (WebSocket) | ✅ Полная |
| 3 | **Advanced API** | PaDev1 | `00eec1ff-31fd-47b4-bbff-1a71b63b0330` | Расширенные эндпоинты управления | Нет собственного хранилища | `http://localhost:1888/advanced/*` (REST) | ✅ Полная |
| 4 | **Prometheus Exporter** | jewzaam | - | Prometheus метрики | Нет (только in-memory метрики) | `http://localhost:9876/metrics` (Prometheus text format) | ✅ Агрегация |
| 5 | **InfluxDB Exporter** | daleghent | - | Time-series метрики | **Внешняя БД:** InfluxDB 2.x (локальная или удаленная) | InfluxDB Query API (Flux queries) | ✅ Агрегация |

### 3.2. Категория B: Data Collection & Metadata (4 плагина)

| # | Плагин | Автор | GUID | Назначение | Места хранения данных | Способы получения | Статус |
|---|--------|-------|------|------------|----------------------|-------------------|--------|
| 6 | **Session Metadata** | tcpalmer | `dcb1d37b-f121-4966-99ec-d11410c562b6` | Метаданные сессии и кадров | **В папке сессии:**<br>`AcquisitionDetails.json/csv`<br>`ImageMetaData.json/csv`<br>`WeatherData.json/csv` | File Watcher → JSON/CSV Parser | ✅ Парсинг |
| 7 | **Night Summary** | vorticose | `2b8caa03-b7c2-47e2-aa54-49190f7a0ea8` | Итоговый отчет за ночь | **В папке сессии:**<br>`NightSummary.html`<br>`NightSummary.json`<br>SQLite БД: `%LOCALAPPDATA%\NINA\NightSummary\nightsummary.sqlite` | File Watcher → JSON Parser | ✅ Парсинг |
| 8 | **NINA.Web** | tcpalmer | `81b04674-ea65-4fe8-b79b-a77c1d209183` | Локальный веб-интерфейс | `%LOCALAPPDATA%\NINA\Web\` (статические файлы)<br>Логи: `%LOCALAPPDATA%\NINA\WebPlugin\webserver-*.log` | `http://localhost:80/dist` (HTTP) | ✅ Мониторинг |
| 9 | **LiveStack** | isbeorn | `10bc1716-54af-425e-b307-c0ca1ce10600` | Real-time стек и калибровка | **Рабочая папка:**<br>`C:\Users\istep\YandexDisk\Хобби\Астрономия\Фото\Сессии\Live\`<br>`stack_status.json` (предположительно)<br>`history.csv` (предположительно)<br>**Библиотеки мастеров:** JSON в профиле (BiasLibrary, DarkLibrary) | File Watcher → JSON/CSV Parser | ✅ Парсинг |

### 3.3. Категория C: Quality Analysis (5 плагинов)

| # | Плагин | Автор | GUID | Назначение | Места хранения данных | Способы получения | Статус |
|---|--------|-------|------|------------|----------------------|-------------------|--------|
| 10 | **Hocus Focus** | ghilios | `0f1d10b6-d306-4168-b751-d454cbac9670` | Детальный анализ звезд (FWHM, Eccentricity, Coma, Astigmatism) | **Из профиля:**<br>`SavePath = C:\Users\istep\YandexDisk\Хобби\Астрономия\ПО\N.I.N.A\Data\HF\`<br>`IntermediateSavePath = %LOCALAPPDATA%\NINA\HocusFocusIntermediate\`<br>CSV файлы с аналитикой каждой звезды | File Watcher → CSV Parser | ✅ Парсинг |
| 11 | **AutoFocus Analysis** | isbeorn | `97021132-0c25-4443-b947-fe5efbe0a3d6` | Анализ кривых автофокуса | `%LOCALAPPDATA%\NINA\AutoFocus\` (JSON/CSV отчеты, графики) | File Watcher → JSON/CSV Parser | ✅ Парсинг |
| 12 | **Dither Statistics** | Thierrytsch | - | Качество паттерна дизеринга (CD, GFM, Voronoi CV, NNI) | **Документы:**<br>`%USERPROFILE%\Documents\NINA\DitherStatistics\` (экспорты quality reports) | WebSocket events или File Watcher | ✅ Интеграция |
| 13 | **Guiding Analyzer** | jphf007 | - | FFT анализ PHD2 логов, детекция PE, backlash, полярной ошибки | **Загружает PHD2 логи:**<br>`*.txt` или `*.csv` из папки PHD2<br>**Экспортирует:**<br>`%USERPROFILE%\Documents\NINA\GuidingAnalyzer\` (CSV, JSON, PDF) | File Watcher → CSV/JSON Parser | ✅ Интеграция |
| 14 | **Benchmark** | caelo-works | - | Бенчмарк производительности CPU для пайплайна N.I.N.A. | **Кэш тестовых кадров:**<br>`%LOCALAPPDATA%\NINA\Benchmark\` (~190 MB)<br>**Результаты:** in-memory + online leaderboard | REST API или File Watcher | ✅ Мониторинг |

### 3.4. Категория D: Automation & Control (12 плагинов)

| # | Плагин | Автор | GUID | Назначение | Места хранения данных | Способы получения | Статус |
|---|--------|-------|------|------------|----------------------|-------------------|--------|
| 15 | **Inject Autofocus** | charleshagen | - | Глобальный триггер автофокуса | Нет (только trigger в секвенсоре) | Advanced API → Trigger Emulation | ✅ Инжект |
| 16 | **Sequencer+** | palmito9 | - | Переменные, выражения, функции, DIY триггеры, When Unsafe | Нет (работает в памяти N.I.N.A.) | Advanced API → `SetGlobalVariable` | ✅ Инжект |
| 17 | **Dynamic Sequencer** | DanielHeEGG | - | Динамический выбор целей через JSON-проекты | **Документы:**<br>`%USERPROFILE%\Documents\DynamicSequencer\Projects\` (JSON проекты)<br>`%USERPROFILE%\Documents\DynamicSequencer\Logs\`<br>`%USERPROFILE%\Documents\DynamicSequencer\settings.json` | File Watcher → JSON Parser + Writer (только при остановленном секвенсоре) | ✅ Интеграция |
| 18 | **Target Scheduler** | tcpalmer | - | Автоматический выбор цели из пула проектов | **Внутренняя БД плагина:**<br>`%LOCALAPPDATA%\NINA\Plugins\...\TargetScheduler\` (SQLite или JSON) | Advanced API или прямой доступ к БД | ✅ Интеграция |
| 19 | **Target Planning** | tcpalmer | `76db8780-e24a-4166-bd5f-5786ab793856` | Планирование сессий на несколько дней/сезон | Нет (генерирует HTML отчеты) | UI interaction или HTML Parser | ✅ Консалтинг |
| 20 | **Flexure Compensator** | michelegz | `00aa6286-a2f7-490e-bc08-7844af7175f5` | Компенсация прогиба OAG через plate solve | Логи в `NINA.log` | Log Tailing или InfluxDB | ✅ Мониторинг |
| 21 | **PHD2 Tools** | isbeorn | - | Триггеры для PHD2 (RestartWhenSaturated, Phd2Settle, InterruptWhenRMSAbove) | Нет (только triggers) | Advanced API → Trigger Emulation | ✅ Инжект |
| 22 | **Solve Every Light** | astroalex80 | `9d4f7ba2-10f2-4373-bfcb-b4b3dcbe21db` | Plate solve каждого LIGHT-кадра, запись WCS в FITS-хедер | **FITS Headers:**<br>WCS (`WCSAXES`, `CTYPE1`, `CRVAL1`, `CRVAL2`, `CD1_1`, `CD2_2`) записывается в каждый FITS-файл | FITS Header Scanner (cfitsio) | ✅ Парсинг |
| 23 | **Faster Flats** | naixx | - | Отключение auto-stretch при съемке flats для снижения нагрузки CPU | Нет (только instructions) | Advanced API → Instruction Injection | ✅ Инжект |
| 24 | **Device Commands** | daleghent | - | Доступ к Action(), CommandBool(), CommandString() ASCOM-драйверов | Нет (прямые вызовы ASCOM) | Advanced API → `Action()`, `CommandBool()` | ✅ Инжект |
| 25 | **Scope Control** | isbeorn | - | Продвинутое управление монтировкой (PEC, специфичные команды) | Нет (прямые вызовы) | Advanced API → Scope Commands | ✅ Инжект |
| 26 | **Shutdown PC** | daleghent | - | Инструкция выключения ПК по окончании секвенсора | Нет (системная команда) | **ТРЕБУЕТ ПЕРЕХВАТА** (Safety Interceptor) | ⚠️ Контроль |

### 3.5. Категория E: Environment & Safety (3 плагина)

| # | Плагин | Автор | GUID | Назначение | Места хранения данных | Способы получения | Статус |
|---|--------|-------|------|------------|----------------------|-------------------|--------|
| 27 | **AI Weather** | michelebergo | - | AI-анализ All-Sky камеры, Safety Monitor | **Опционально:**<br>Safety Status File (путь настраивается)<br>**Логи:** in-memory + NINA.log | File Watcher (status file) или WebSocket | ✅ Интеграция |
| 28 | **Home Assistant** | caelo-works | - | Интеграция с умным домом (реле, датчики, обогреватели) | Нет (работает через HA API) | REST API → Home Assistant Webhook | ✅ Интеграция |
| 29 | **Moon Angle** | daleghent | - | Угловое расстояние до Луны/Солнца | **FITS Headers:**<br>`SUNANGLE`, `MOONANGL`<br>**File Patterns:**<br>`$$SUNANGLE$$`, `$$MOONANGLE$$` | FITS Header Scanner или File Name Parser | ✅ Парсинг |

### 3.6. Категория F: Alignment & Calibration (2 плагина)

| # | Плагин | Автор | GUID | Назначение | Места хранения данных | Способы получения | Статус |
|---|--------|-------|------|------------|----------------------|-------------------|--------|
| 30 | **Two Point Polar Alignment** | nirzons | `0e9e3e58-42fc-4553-8e6e-aba061af4f54` | Полярное выравнивание по 2 точкам (90° поворот RA) | **Из профиля:**<br>`PolarHomeRA` (сохраняется в профиле)<br>`ExposureTime`, `Gain`, `Offset`, `RotationAmount`, `Filter`, `Method`, `Direction`, `StartingPoint`, `Binning`, `PlateSolveRetries`, `EnableOnePointAlignment`, `ExposuresPerPoint`<br>**Логи:** in-memory + NINA.log | WebSocket events или Log Tailing | ✅ Копилот |
| 31 | **Polar Alignment** | isbeorn | - | Альтернативное полярное выравнивание | `%LOCALAPPDATA%\NINA\PolarAlignment\` (предположительно) | File Watcher или WebSocket | ✅ Копилот |

### 3.7. Категория G: External Integration (3 плагина)

| # | Плагин | Автор | GUID | Назначение | Места хранения данных | Способы получения | Статус |
|---|--------|-------|------|------------|----------------------|-------------------|--------|
| 32 | **Python Plugin** | isbeorn | - | Выполнение произвольного Python-кода внутри N.I.N.A. | **Скрипты:**<br>`%LOCALAPPDATA%\NINA\PythonScripts\` (предположительно) | Advanced API → `ExecutePythonScript` | ✅ Инжект |
| 33 | **External Scripts** | isbeorn | - | Запуск внешних batch/PowerShell скриптов | **Скрипты:**<br>Пути настраиваются в секвенсоре | Advanced API → `ExternalScript` | ✅ Инжект |
| 34 | **Orbitals** | ghilios | - | Трекинг спутников/МКС | **Каталоги TLE:**<br>`%LOCALAPPDATA%\NINA\Orbitals\` (предположительно) | File Watcher или WebSocket | ✅ Интеграция |

### 3.8. Категория H: Utilities & Libraries (3 плагина)

| # | Плагин | Автор | GUID | Назначение | Места хранения данных | Способы получения | Статус |
|---|--------|-------|------|------------|----------------------|-------------------|--------|
| 35 | **CFITSIO** | isbeorn | - | C-библиотека для чтения/записи FITS-файлов | Нет (используется другими плагинами) | Python `fitsio` library | ✅ Library |
| 36 | **Log Analyzer** | isbeorn | - | Анализатор логов N.I.N.A. | `%LOCALAPPDATA%\NINA\Logs\NINA.log` | Log Tailing → Pattern Matching | ✅ Мониторинг |
| 37 | **Point3D** | isbeorn | - | Математика 3D-пространства (коллизии, синхронизация) | Нет (математическая библиотека) | Python port или `nina.plugin.python` | ✅ Library |

### 3.9. Категория I: Дополнительные плагины (35 плагинов)

| # | Плагин | Автор | GUID | Назначение | Статус |
|---|--------|-------|------|------------|--------|
| 38 | **AI Assistant** | michelebergo | - | Существующий AI-ассистент для N.I.N.A. (для референса) | ✅ Референс |
| 39 | **Autofocus Report Analysis** | isbeorn | - | Расширенный анализ отчетов автофокуса | ✅ Мониторинг |
| 40 | **CPU-GPU Computing for NINA** | Lucas Alias | - | Ускорение вычислений через GPU | ✅ Оптимизация |
| 41 | **Click To Center** | astro_alex80 | - | Клик по изображению для центрирования | ✅ UI |
| 42 | **Connector** | isbeorn | - | Управление подключениями оборудования | ✅ Интеграция |
| 43 | **Dynamic Cooling** | RegulusRemains | - | Динамическое управление охлаждением камеры | ✅ Инжект |
| 44 | **Exposure Calculator** | isbeorn | - | Расчет оптимальной экспозиции (SharpCap логика) | ✅ Консалтинг |
| 45 | **Filter Offset Calculator** | S. Dimant & isbeorn | - | Расчет офсетов фокусера для фильтров | ✅ Консалтинг |
| 46 | **FilterSelector** | Your Name | - | Интерактивный выбор фильтра через MessageBox | ✅ Копилот |
| 47 | **GPSDLocationPlugin** | - | - | Получение координат через GPSD | ✅ Интеграция |
| 48 | **Horizon Creator** | christian-palm | - | Создание кастомного горизонта | ✅ UI |
| 49 | **Horizon Studio** | Nir Zonshine | - | Продвинутое редактирование горизонта с веб-камерой | ✅ UI |
| 50 | **Log Viewer** | astro_alex80 | - | Просмотр логов в реальном времени | ✅ UI |
| 51 | **Manual Focuser** | cwseo | - | Ручное управление фокусером | ✅ UI |
| 52 | **ManualRotatorOAG** | JR Schmidt | - | Ручной ротатор + OAG | ✅ UI |
| 53 | **NINA++ - You gotta go fast!** | - | - | Оптимизация производительности | ✅ Оптимизация |
| 54 | **NINA.Luckyimaging** | Nick Hardy | - | Lucky imaging для планет и Луны | ✅ Интеграция |
| 55 | **OagFocusAssist** | Your Name | - | Помощь в фокусировке OAG | ✅ Копилот |
| 56 | **Orbuculum** | isbeorn | - | Визуализация орбит спутников | ✅ UI |
| 57 | **PlateSolvePlus** | Flashy-GER | - | Расширенный plate solving | ✅ Интеграция |
| 58 | **Remote Copy** | tcpalmer | - | Удаленное копирование файлов | ✅ Интеграция |
| 59 | **SkyFlats** | photon | - | Автоматизация съемки sky flats | ✅ Инжект |
| 60 | **Smart Filters** | Benoit SAINTOT | - | Умная фильтрация кадров | ✅ Интеграция |
| 61 | **Three Point Polar Alignment** | isbeorn | - | Полярное выравнивание по 3 точкам | ✅ Копилот |
| 62 | **Touch 'N' Stars** | Johannes Maier, christian-palm, Christian Wöhrle | - | Мобильное управление через веб-интерфейс | ✅ UI |
| 63 | **Web Session History Viewer** | tcpalmer | - | Просмотр истории сессий через веб | ✅ UI |
| 64 | **SkyWave** | - | `b7e3f1a2-9c4d-4e8b-a6f5-1d2c3b4a5e6f` | Съемка с дефокусом для фотометрии | ✅ Интеграция |
| 65 | **EQMOD Quirks** | - | `afa13a89-8ae3-4975-a953-683c6b6e2bbe` | Специфичные настройки для EQMOD | ✅ Конфигурация |
| 66 | **SharpCap Integration** | - | `2b4b2fd6-46ce-4f34-b184-4a8b3058dc86` | Интеграция с SharpCap Sensor Analysis | ✅ Интеграция |
| 67 | **Temperature Control** | - | `25ac9c96-885e-4733-a437-a5d4863a1c7e` | Продвинутое управление температурой | ✅ Инжект |
| 68 | **Alpaca/ASCOM** | - | `6bd8bce9-c199-401a-aaf8-47ea8ee5ae32` | Настройки Alpaca/ASCOM подключений | ✅ Конфигурация |
| 69 | **Visual Polar Alignment** | - | `ef99cb7e-3c22-491c-b26a-54315222bf9b` | Визуальное полярное выравнивание через веб-камеру | ✅ Копилот |
| 70 | **Tree View** | - | `b4541ba9-7b07-4d71-b8e1-6c73d4933ea0` | Древовидное отображение секвенсора | ✅ UI |
| 71 | **AutoConnect** | - | `52c17ee7-6d6c-4ee1-8fa0-85bcf6677bef` | Автоматическое подключение оборудования | ✅ Автоматизация |
| 72 | **Point3D Visualizer** | - | `200ce2d2-6992-44fe-bf83-f8c2e01c7244` | 3D визуализация телескопа | ✅ UI |

---

## 4. КАРТА ДАННЫХ N.I.N.A. (ФАЙЛОВАЯ СИСТЕМА)

### 4.1. Конфигурация и Профили

| Путь | Содержимое | Назначение для AI |
|------|------------|-------------------|
| `%LOCALAPPDATA%\NINA\Profiles\` | XML-файлы профилей (например, `f11e35f6-0f58-4c16-b24f-e3effb5154d4.profile`) | **КРИТИЧНО:** Содержит `<pluginStorage>` с GUID всех плагинов и их настройками (пути, параметры) |
| `%LOCALAPPDATA%\NINA\Settings.json` | Глобальные настройки N.I.N.A. | Понимание глобальных параметров (язык, темы, polling intervals) |
| `%LOCALAPPDATA%\NINA\Plugins\3.0.0\` | Папки установленных плагинов | Обнаружение установленных плагинов через сканирование папок |

### 4.2. Логи и Диагностика

| Путь | Содержимое | Назначение для AI |
|------|------------|-------------------|
| `%LOCALAPPDATA%\NINA\Logs\*.log` | Основной лог-файл N.I.N.A. (динамическое имя: `YYYYMMDD-HHMMSS-version.pid.log`) | **Log Tailing:** Детекция ошибок, предупреждений, статусов триггеров в реальном времени |
| `%LOCALAPPDATA%\NINA\Logs\*.log.YYYY-MM-DD` | Архивные логи | Post-mortem анализ прошлых сессий |
| `%LOCALAPPDATA%\NINA\AutoFocus\` | Логи и графики автофокуса | Анализ трендов фокусировки, температурных коэффициентов |

### 4.3. Данные плагинов (из вашего профиля)

| Плагин | Путь (из `<pluginStorage>`) | Содержимое |
|--------|----------------------------|------------|
| **Hocus Focus** | `C:\Users\istep\YandexDisk\Хобби\Астрономия\ПО\N.I.N.A\Data\HF\` | CSV-файлы с детальной аналитикой звезд (FWHM, Eccentricity, Coma, Astigmatism) |
| **LiveStack** | `C:\Users\istep\YandexDisk\Хобби\Астрономия\Фото\Сессии\Live\` | Рабочая папка LiveStack: калиброванные кадры, стек, статусные файлы |
| **Session Metadata** | В папке каждой сессии | `AcquisitionDetails.json`, `ImageMetaData.json`, `WeatherData.json` |
| **Night Summary** | В папке каждой сессии + SQLite БД | `NightSummary.html`, `NightSummary.json`, `nightsummary.sqlite` |
| **Dynamic Sequencer** | `C:\Users\istep\Documents\DynamicSequencer\Projects\` | JSON-файлы проектов (цели, экспозиции, приоритеты) |
| **Guiding Analyzer** | `%USERPROFILE%\Documents\NINA\GuidingAnalyzer\` | Экспорты CSV, JSON, PDF отчетов |
| **Dither Statistics** | `%USERPROFILE%\Documents\NINA\DitherStatistics\` | Экспорты quality reports |

### 4.4. Библиотека Мастер-кадров (Ваша структура)

| Путь | Структура | Назначение для AI |
|------|-----------|-------------------|
| `C:\Users\istep\YandexDisk\Хобби\Астрономия\Фото\Данные\MASTER FILE\Masters\` | `-15.00°C\BIAS\`<br>`-15.00°C\DARK\30s\`<br>`-15.00°C\DARK\60s\`<br>`...` | **Индексация:** AI сканирует FITS-хедеры и строит каталог доступных мастеров (Temp, Gain, Offset, Exposure, Filter) |
| **LiveStack Bias Library** | Из профиля (GUID `10bc1716`):<br>`BiasLibrary = [{"Type":1, "Path":"...", "Gain":85, ...}]` | AI знает точные пути и параметры всех Bias-мастеров |
| **LiveStack Dark Library** | Из профиля (GUID `10bc1716`):<br>`DarkLibrary = [{"Type":0, "Path":"...", "ExposureTime":60.0, ...}]` | AI знает точные пути и параметры всех Dark-мастеров |

### 4.5. Папки сессий и изображений

| Путь | Структура | Содержимое |
|------|-----------|------------|
| `C:\Users\istep\YandexDisk\Хобби\Астрономия\Фото\Сессии\` | `$$TELESCOPE$$_$$CAMERA$$\$$TARGETNAME$$\$$DATEMINUS12$$\$$IMAGETYPE$$\` | FITS-файлы с метаданными в именах (ExposureTime, Filter, Gain, Offset, RMS, HFR, FWHM, Stars) |
| **Пример файла:** | `Askar 80ED f-5.95_QHY533C\M31\2025-09-17\LIGHT\60s_F-SV220_G-85_O-10\LIGHT_№-0013_2025-09-17_00-33-26_Cam-(E_60.00s_T_-20.00°C_B-1x1_G-68_O-25_A-180.00°)_EAF-(P-6931st_T_1.39°C)_M-(R-0.64)_PS-(S-400_F-5.35_H-1.69).fits` | **File Name Parser** извлекает все метрики без открытия файла |

---

## 5. СЛОИ СИСТЕМЫ (ДЕТАЛЬНО)

### 5.1. Слой конфигурации и обнаружения (Configuration & Discovery)

**Задача:** загрузить пользовательские пути, распарсить профиль N.I.N.A., найти все установленные плагины.

**Входные данные:**
- `config/settings.yaml` — пользовательский файл с путями
- Активный XML-профиль из `profiles_dir`
- Папка `plugins_dir` — имена установленных плагинов

**Выход:**
- `Capability Registry` — словарь всех плагинов с их настройками и путями
- `Sequence Shadow` — внутреннее представление графа секвенсора

**Реализация:**
- Парсинг XML — `xmltodict`
- Сканирование папок — `pathlib`
- Построение графа — рекурсивный обход JSON `Sequence.json` с разрешением `$ref`

**Ключевые файлы конфигурации:**
```yaml
# config/settings.yaml
nina_environment:
  appdata_root: "C:\\Users\\istep\\AppData\\Local\\NINA"
  sessions_root: "C:\\Users\\istep\\YandexDisk\\Хобби\\Астрономия\\Фото\\Сессии"
  masters_root: "C:\\Users\\istep\\YandexDisk\\Хобби\\Астрономия\\Фото\\Данные\\MASTER FILE\\Masters"
  profiles_dir: "C:\\Users\\istep\\AppData\\Local\\NINA\\Profiles"
  sequence_template: "C:\\Users\\istep\\YandexDisk\\Хобби\\Астрономия\\ПО\\N.I.N.A\\Set templates\\Sequence.json"
  logs_dir: "C:\\Users\\istep\\AppData\\Local\\NINA\\Logs"
  plugins_dir: "C:\\Users\\istep\\AppData\\Local\\NINA\\Plugins\\3.0.0"

network:
  nina_api_host: "http://localhost:1888"
  nina_ws_url: "ws://localhost:1888/v2/socket"
  prometheus_url: "http://localhost:9876/metrics"

influxdb:
  url: "http://localhost:8086"
  token: "${INFLUXDB_TOKEN}"
  org: "observatory"
  bucket: "nina_telemetry"

ai_settings:
  ollama_host: "http://localhost:11434"
  model_name: "qwen2.5:14b"
  rag_db_path: "./data/vector_db"

logging:
  level: "INFO"
```

### 5.2. Слой сбора данных (Ingestion Layer)

**Задача:** собирать все доступные данные из файловой системы и сети, не нагружая N.I.N.A.

#### 5.2.1. File Watchers (на `watchdog`)

Мониторят следующие папки:

| Папка | Файлы | Плагин-источник |
|-------|-------|-----------------|
| `sessions_root` | `ImageMetaData.json`, `AcquisitionDetails.json`, `WeatherData.json` | Session Metadata |
| `HocusFocus SavePath` | CSV с аналитикой звезд | Hocus Focus |
| `LiveStack WorkingDir` | `stack_status.json`, история | LiveStack |
| `Documents\NINA\DitherStatistics\` | Экспорты CD, GFM | Dither Statistics |
| `Documents\NINA\GuidingAnalyzer\` | CSV/JSON/PDF | Guiding Analyzer |
| `Documents\DynamicSequencer\Projects\` | JSON проекты | Dynamic Sequencer |
| `NightSummary` папки сессий | `NightSummary.json` | Night Summary |

**Логика работы:**
- При появлении нового файла: читается, парсится, метрики отправляются в **Redis** (кэш) и **InfluxDB** (история)
- Генерируется событие для AI-агентов

#### 5.2.2. Log Tailer

**Задача:** отслеживать самый свежий `*.log` в `logs_dir`

**Фильтрация критических строк:**
- `ERROR`, `FATAL`, `Exception`
- `Trigger fired`
- `Safety Monitor`, `Unsafe`
- `Meridian Flip Started/Completed`
- `Download failed`, `USB Timeout`

**Результат:** передача в AI-агенты и Redis

#### 5.2.3. FITS Header Scanner (на `fitsio`)

**Задача:** при появлении нового FITS-файла читать только заголовки (без тела)

**Извлекаемые данные:**
- WCS (`WCSAXES`, `CTYPE1`, `CRVAL1`, `CRVAL2`, `CD1_1`, `CD2_2`)
- `MOONANGL`, `SUNANGLE` (если есть)
- Вычисление дрейфа поля (сравнение с предыдущими кадрами)

**Результат:** отправка в Redis/InfluxDB

#### 5.2.4. Prometheus Scraper (HTTP)

**Задача:** периодически (каждые 2-5 сек) опрашивать `prometheus_url`

**Извлекаемые метрики:**
- Статус треккинга
- RMS гида
- Температура сенсора
- Положение фокусера
- Скорость ветра
- Влажность
- Облачность

**Результат:** сохранение в Redis (мгновенный срез)

#### 5.2.5. InfluxDB Subscriber

**Задача:** выполнять Flux-запросы для получения исторических трендов

**Использование:**
- Кэширование последних N точек
- Предоставление данных AI-агентам по запросу

#### 5.2.6. WebSocket Client

**Задача:** подключение к `nina_ws_url` для получения событий в реальном времени

**Обрабатываемые события:**
- `SequenceStarted`, `SequenceStopped`
- `SequenceItemStarted`, `SequenceItemCompleted`
- `MeridianFlipStarted/Completed`
- `Error`, `EquipmentConnected/Disconnected`

**Логика:**
- Автоматическое переподключение при обрыве (экспоненциальная задержка)
- Обновление Shadow Engine (текущий ID, имя, путь контейнера)

### 5.3. Shadow Engine

**Задача:** хранить актуальное состояние секвенсора и предоставлять API для его получения

**Логика работы:**
1. При старте парсится `Sequence.json` → строится граф с разрешением `$ref`
2. На каждое WebSocket событие обновляется состояние
3. При входе в `MessageBox` — состояние помечается, генерируется событие для Copilot
4. При приближении к `ShutdownPcInstruction` — Safety Interceptor готов к перехвату

**API Endpoints:**
- `GET /api/v1/sequence/shadow` — полный граф секвенсора
- `GET /api/v1/sequence/state` — текущее состояние выполнения

### 5.4. Multi-Agent Swarm (на LangGraph)

**Задача:** принимать решения на основе собранных данных и выполнять действия через инструменты

#### 5.4.1. Общее состояние (`ObservatoryState`)

Содержит:
- Текущие метрики (HFR, FWHM, RMS, SNR, GFM, CD, Eccentricity)
- Историю трендов (из InfluxDB/Redis)
- Статус безопасности (погода, лимиты)
- Состояние секвенсора (из Shadow Engine)
- Список активных целей (из Target Scheduler / Dynamic Sequencer)
- Историю действий AI (для объяснимости)

#### 5.4.2. Агенты и их роли

| Агент | Роль | Источники данных | Инструменты (Actions) |
|-------|------|------------------|----------------------|
| **The Watcher** | Мониторинг и детекция аномалий | Все парсеры, LogTailer, InfluxDB | `query_trend(metric, window)`, `detect_anomaly()`, `generate_alert(level, message)` |
| **The Strategist** | Оптимизация параметров и планирование | LiveStack (SNR), Dynamic Sequencer, Target Scheduler, MoonAngle, AI Weather | `set_global_variable(name, value)`, `edit_dynamic_project(target, updates)`, `disable_target(name)`, `switch_filter(filter)` |
| **The Guardian** | Безопасность и предотвращение аварий | AI Weather, Safety Monitor, Flexure Compensator, Point3D | `trigger_autofocus()`, `trigger_dither()`, `trigger_guider_calibration()`, `intercept_shutdown()`, `park_mount()` |
| **The Copilot** | Интерактивная помощь при ручных шагах | MessageBox events, TwoPointPolarAlignment, OagFocusAssist, FilterSelector | `generate_guide(step, params)`, `push_notification(message)`, `update_ui_panel(data)` |

#### 5.4.3. Инструменты (Tools)

**Trigger Emulator:**
- `inject_trigger(trigger_id)` — эмуляция глобального триггера

**GlobalVar Injector:**
- `set_global_variable(name, value)` — изменение переменной Sequencer+

**Python Bridge:**
- `execute_python_script(code)` — отправка кода в `nina.plugin.python`

**External Script Launcher:**
- `execute_external_script(path, args)` — запуск batch/PowerShell

**Device Command Sender:**
- `send_device_command(device, command, params)` — вызов ASCOM Action/Command

**Dynamic Editor:**
- `edit_dynamic_project(target, updates)` — редактирование JSON проекта Dynamic Sequencer

**Home Assistant Bridge:**
- `send_homeassistant(service, data)` — управление умным домом

#### 5.4.4. Взаимодействие агентов

- Агенты работают в цикле, используя граф LangGraph
- Каждый агент может запрашивать данные из `ObservatoryState`
- Решения агентов записываются в лог (для объяснимости)
- Приоритет: Safety > Quality > Optimization

### 5.5. Слой выполнения (Execution Layer)

**Задача:** безопасно исполнять команды, сгенерированные агентами

**Компоненты:**

| Компонент | Назначение | Метод |
|-----------|------------|-------|
| **Trigger Emulator** | Эмуляция триггеров | Отправка в `/advanced/trigger` |
| **GlobalVar Injector** | Изменение переменных | Отправка в `/advanced/variable/{name}` |
| **Python Bridge** | Выполнение Python | Отправка в `/advanced/python` |
| **External Script Launcher** | Запуск скриптов | Отправка в `/advanced/external` |
| **Device Command Sender** | ASCOM команды | Отправка в `/advanced/device/{device}/command` |
| **Dynamic Editor** | Редактирование JSON | Чтение/запись файлов (только при остановленном секвенсоре) |
| **Safety Interceptor** | Перехват Shutdown | Подписка на `SequenceItemStarted` для `EndAreaContainer` |

**Safety Interceptor логика:**
1. Подписка на событие `SequenceItemStarted`
2. Если текущий шаг — `EndAreaContainer` и следующий — `ShutdownPcInstruction`
3. Проверка: пользователь активен в UI?
4. Если ДА: инжект `WaitForTimeSpan` или `MessageBox` через Python/External
5. Уведомление пользователя

### 5.6. RAG Система (База знаний)

**Задача:** предоставить AI доступ к документации и истории сессий

**Источники знаний:**
1. Документация N.I.N.A. (официальная wiki, PDF мануалы)
2. Документация плагинов (README.md, wiki каждого плагина)
3. Форумы и статьи (CloudyNights, IceInSpace, SharpCap forums)
4. История сессий (автоматически генерируемые `Session_Digest.md`)
5. Логи ошибок (Post-Mortem анализ прошлых проблем)

**Векторизация и хранение:**
- **Векторная БД:** Qdrant (или ChromaDB)
- **Embedding модель:** `nomic-embed-text` (локальная через Ollama)
- **Chunking стратегия:**
  - Документация: по секциям (500-1000 токенов)
  - Сессии: по событиям (каждый алерт/действие = отдельный чанк)
  - Логи: по ошибкам (каждая ошибка + контекст = чанк)

**Автоматическое пополнение:**

После каждой сессии AI генерирует `Session_Digest.md`:

```markdown
# Сессия 2026-07-06: M31 (Галактика Андромеды)

## Параметры
- Фильтр: SV220_Ha-Oiii_7nm
- Экспозиция: 60s (T=30, T*2)
- Gain: 85, Offset: 10
- Температура: -15°C

## Результаты
- Отснято: 45 кадров
- Принято LiveStack: 42 (93%)
- Средний HFR: 2.1px
- Средний RMS: 0.8" (RA), 0.9" (Dec)

## Проблемы и решения
- **03:15**: Ветер 12 м/с с севера → RMS по DEC вырос до 2.5"
  - *Решение*: Переключились на M42 (южное направление)
  - *Вывод*: При ветре с севера избегать целей на азимуте 0-90°

## Рекомендации для будущих сессий
- Для M31 оптимальная экспозиция 60-90s при Луне < 50%
- Избегать съемки при ветре > 10 м/с с северного направления
```

Этот файл:
1. Векторизуется и добавляется в Qdrant
2. Индексируется по метаданным (дата, цель, фильтр, проблемы)
3. Становится доступным для поиска через RAG

---

## 6. FRONTEND АРХИТЕКТУРА

### 6.1. Технологический стек

| Компонент | Технология | Назначение |
|-----------|------------|------------|
| **Framework** | Vue 3 + Composition API | Реактивный UI |
| **Build Tool** | Vite | Быстрая сборка |
| **State Management** | Pinia | Централизованное состояние |
| **Styling** | Tailwind CSS | Utility-first стили |
| **Charts** | Apache ECharts | Сложные астрономические графики |
| **3D (опционально)** | Three.js | Digital Twin |
| **WebSocket** | `reconnecting-websocket` | Автопереподключение |

### 6.2. Pinia Stores (5 хранилищ)

#### Store 1: `useSequenceStore`
- `shadowGraph` — полный граф секвенсора
- `currentItemId` — ID текущего элемента
- `currentItemName` — имя текущего элемента
- `containerPath` — путь контейнеров
- `globalVariables` — текущие значения переменных

#### Store 2: `useMetricsStore`
- `hfr`, `fwhm`, `rmsRa`, `rmsDec`, `snr`, `temperature` — текущие метрики
- `hfrHistory`, `temperatureHistory` — история для графиков (последние 100 точек)

#### Store 3: `usePluginStore`
- `discoveredPlugins` — словарь всех обнаруженных плагинов

#### Store 4: `useAlertStore`
- `alerts` — список активных алертов
- `addAlert(alert)` — добавление алерта
- `acknowledgeAlert(alertId)` — подтверждение алерта

#### Store 5: `useChatStore`
- `messages` — история чата
- `isProcessing` — флаг обработки запроса
- `sendMessage(message)` — отправка сообщения AI

### 6.3. Основные компоненты

#### Dashboard.vue (Главная панель)
- Текущее состояние секвенсора
- Метрики в реальном времени
- Статус безопасности
- График HFR (ECharts)
- График температуры (ECharts)
- Активные алерты

#### TimeMachine.vue (Воспроизведение сессий)
- Слайдер времени
- Синхронизированные графики (HFR, RMS, температура)
- Превью кадра
- Лог событий

#### CopilotPanel.vue (Интерактивные подсказки)
- Пошаговая инструкция для ручных шагов
- Кнопки управления (Далее, Завершить)
- Визуализация ошибок (2PA, OAG Focus)

#### GlobalVariablesEditor.vue (Редактор переменных)
- Список всех глобальных переменных
- Слайдеры/инпуты для изменения
- Кнопка "Применить" для отправки в N.I.N.A.

#### ShadowSequenceTree.vue (Визуализация графа)
- Древовидная структура секвенсора
- Подсветка активного элемента
- Отображение триггеров и условий

---

## 7. БЕЗОПАСНОСТЬ И УПРАВЛЕНИЕ РИСКАМИ

### 7.1. Жёсткие правила (НИКОГДА не нарушать)

1. **Никогда** не изменять `Sequence.json` напрямую
2. **Никогда** не отправлять команду `Slew` или `Park` без проверки Safety Monitor
3. **Никогда** не выполнять `Shutdown PC` без явного подтверждения пользователя (если UI активен)
4. Перед любым инжектом триггера проверять, что камера не занята
5. Перед изменением глобальной переменной проверять, что секвенсор не в критической фазе (Meridian Flip, Guiding Settle)
6. Все AI-решения должны быть **логируемыми** — записывать причину каждого действия

### 7.2. Уровни алертов

| Уровень | Описание | Действие |
|---------|----------|----------|
| **INFO** | Информационное сообщение | Логирование |
| **WARNING** | Предупреждение | Уведомление в UI |
| **CRITICAL** | Критическая проблема | Push-уведомление + Telegram |
| **EMERGENCY** | Аварийная ситуация | Push + Telegram + Email + автоматическое действие (Park) |

### 7.3. Категории алертов

- `equipment` — проблемы с оборудованием
- `quality` — деградация качества изображения
- `environment` — погодные условия
- `sequence` — проблемы с секвенсором
- `system` — системные ошибки
- `safety` — нарушения безопасности

---

## 8. КОНФИГУРАЦИЯ И DEPLOYMENT

### 8.1. Docker Compose (рекомендуется)

```yaml
version: '3.8'

services:
  backend:
    build: ./backend
    ports:
      - "8000:8000"
    volumes:
      - ./config:/app/config
      - ./logs:/app/logs
    environment:
      - INFLUXDB_TOKEN=${INFLUXDB_TOKEN}
    depends_on:
      - influxdb
      - redis
      - qdrant

  frontend:
    build: ./frontend
    ports:
      - "3000:80"
    depends_on:
      - backend

  influxdb:
    image: influxdb:2.7
    ports:
      - "8086:8086"
    volumes:
      - influxdb_data:/var/lib/influxdb2
    environment:
      - DOCKER_INFLUXDB_INIT_MODE=setup
      - DOCKER_INFLUXDB_INIT_USERNAME=admin
      - DOCKER_INFLUXDB_INIT_PASSWORD=adminpassword
      - DOCKER_INFLUXDB_INIT_ORG=observatory
      - DOCKER_INFLUXDB_INIT_BUCKET=nina_telemetry
      - DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=${INFLUXDB_TOKEN}

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

  qdrant:
    image: qdrant/qdrant:latest
    ports:
      - "6333:6333"
    volumes:
      - qdrant_data:/qdrant/storage

volumes:
  influxdb_data:
  redis_data:
  qdrant_data:
```

### 8.2. Локальная установка (Windows)

```bash
# 1. Клонирование репозитория
git clone https://github.com/yourusername/nina-ai-cortex.git
cd nina-ai-cortex

# 2. Создание виртуального окружения
python -m venv venv
venv\Scripts\activate

# 3. Установка зависимостей
pip install -r requirements.txt

# 4. Настройка конфигурации
cp config/settings.yaml.example config/settings.yaml
# Отредактируйте settings.yaml под свои пути

# 5. Установка Ollama
# Скачайте с https://ollama.ai/download
ollama pull qwen2.5:14b

# 6. Запуск Backend
cd backend
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# 7. Запуск Frontend (в отдельном терминале)
cd frontend
npm install
npm run dev
```

### 8.3. Переменные окружения (`.env`)

```bash
# Секреты (не коммитятся в Git)
INFLUXDB_TOKEN=my-super-secret-token
TELEGRAM_BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrsTUVwxyz
EMAIL_PASSWORD=my-email-password
```

---

## 9. ROADMAP (ЭТАПЫ РЕАЛИЗАЦИИ)

### Фаза 1: Базовая инфраструктура (Недели 1-2)
- [x] Конфигурация (`settings.yaml`, Pydantic модели)
- [x] Discovery Engine (парсинг профиля и папки плагинов)
- [x] Sequence Shadow (парсинг `Sequence.json`, построение графа)
- [x] WebSocket клиент с автопереподключением
- [x] Базовый API (состояние, плагины, граф)

**Результат:** Система запускается, видит N.I.N.A., парсит секвенсор

### Фаза 2: Сбор данных (Ingestion) (Недели 3-4)
- [ ] File Watchers для `Session Metadata`, `Hocus Focus`, `LiveStack`
- [ ] File Watchers для `DitherStatistics`, `GuidingAnalyzer`, `Dynamic Sequencer`
- [ ] Log Tailer (фильтрация критических событий)
- [ ] FITS Header Scanner (`cfitsio`)
- [ ] Prometheus Scraper
- [ ] InfluxDB клиент

**Результат:** Система собирает все данные в реальном времени

### Фаза 3: Execution Layer (Недели 5-6)
- [ ] Trigger Emulator
- [ ] GlobalVar Injector
- [ ] Python Bridge
- [ ] External Script Launcher
- [ ] Device Command Sender
- [ ] Safety Interceptor (Shutdown prevention)

**Результат:** Система может безопасно влиять на N.I.N.A.

### Фаза 4: AI-агенты (LangGraph) (Недели 7-9)
- [ ] Реализация `ObservatoryState` и агентов (Watcher, Strategist, Guardian, Copilot)
- [ ] Интеграция с Ollama (локальной или облачной)
- [ ] Создание инструментов и их регистрация в графе
- [ ] Тестирование сценариев (автофокус, смена переменных, уведомления)
- [ ] RAG система (Qdrant, векторизация документации)

**Результат:** AI принимает автономные решения

### Фаза 5: Frontend (Dashboard + Copilot) (Недели 10-12)
- [ ] Vue 3 + Vite + Pinia
- [ ] Отображение текущего состояния (секвенсор, метрики)
- [ ] Графики ECharts (HFR, RMS, температура, SNR)
- [ ] Time-Machine (синхронизация с логами и событиями)
- [ ] Copilot UI (подсказки для MessageBox, 2PA, OAG Focus)
- [ ] Панель управления глобальными переменными и триггерами

**Результат:** Полноценный UI

### Фаза 6: Тестирование и документация (Недели 13-14)
- [ ] Интеграционное тестирование на реальной ночной сессии
- [ ] Нагрузочное тестирование
- [ ] Написание пользовательской документации (установка, настройка, использование)
- [ ] Написание developer документации (архитектура, добавление новых плагинов)

**Результат:** Production-ready система

---

## 10. КЛЮЧЕВЫЕ МЕТРИКИ УСПЕХА

### 10.1. Технические метрики

- ✅ Система запускается без ошибок (с или без N.I.N.A.)
- ✅ WebSocket соединение устанавливается за <5 секунд после включения Advanced API
- ✅ При появлении нового `ImageMetaData.json` метрики появляются в Redis и на Dashboard за <1 секунды
- ✅ AI-агенты корректно детектируют рост HFR и инициируют автофокус через триггер
- ✅ Shutdown PC успешно перехватывается (при активном пользователе)
- ✅ Все 72 плагина обнаружены, и их данные читаются (где применимо)

### 10.2. Функциональные метрики

- ✅ Frontend отображает все MessageBox'ы в виде интерактивных подсказок
- ✅ Пользователь может изменять глобальные переменные через UI, и изменения применяются в N.I.N.A.
- ✅ Time-Machine воспроизводит прошлые сессии с синхронизацией графиков
- ✅ RAG отвечает на вопросы о прошлых сессиях и проблемах
- ✅ Copilot предоставляет пошаговые инструкции для ручных шагов (2PA, OAG Focus)

### 10.3. Бизнес-метрики

- ✅ Уменьшение количества испорченных кадров на 50%
- ✅ Увеличение количества успешных сессий на 30%
- ✅ Сокращение времени на диагностику проблем на 70%
- ✅ Автоматизация 80% рутинных решений

---

## 11. ПРАВИЛА ДЛЯ РАЗРАБОТЧИКА (СТРОГИЕ)

### 11.1. Архитектурные правила

1. Все пути в коде должны браться из `settings.yaml` — **никаких хардкодных строк**
2. Логирование должно быть структурированным и не спамить консоль (фильтровать рутинные INFO-сообщения N.I.N.A.)
3. Код должен быть асинхронным (`asyncio`) для параллельной работы всех сервисов
4. Для FITS-заголовков использовать `fitsio` — быстрее и легче `astropy`
5. Для парсинга XML-профиля использовать `xmltodict` — устойчив к .NET-сериализации

### 11.2. Правила безопасности

6. **Никогда** не изменять `Sequence.json` напрямую
7. **Никогда** не отправлять команду `Slew` или `Park` без проверки Safety Monitor
8. **Никогда** не выполнять `Shutdown PC` без явного подтверждения пользователя
9. Все AI-решения должны быть **логируемыми** — записывать причину каждого действия в отдельный журнал

### 11.3. Правила тестирования

10. Все агенты должны быть тестируемыми: зависимости внедрять через конструктор
11. Каждый File Watcher должен иметь unit-тесты для парсинга
12. Каждый AI-агент должен иметь интеграционные тесты с mock-данными

### 11.4. Правила расширяемости

13. **HAL не обязателен** — полагаемся на встроенную безопасность N.I.N.A., но опционален
14. LLM может быть как локальной (Ollama), так и облачной (через тот же интерфейс) — выбор пользователя
15. Plugin Registry должен быть динамическим: при обнаружении нового GUID в профиле система должна пытаться подгрузить соответствующий ридер или логировать предупреждение

---

## 12. ЗАКЛЮЧЕНИЕ

**N.I.N.A. AI Cortex** — это **production-ready система**, которая:

- ✅ Не дублирует функционал N.I.N.A.
- ✅ Работает как **когнитивный оверлей**
- ✅ Использует **Multi-Agent AI** для автономных решений
- ✅ Интегрируется с **72 плагинами** через динамический реестр
- ✅ Предоставляет **ультимативный Dashboard** с Time-Machine
- ✅ **Самообучается** через RAG и автоматическое пополнение базы знаний
- ✅ **Интегрируется с Siril** для замкнутого цикла качества

Это **первая в мире AI-обсерватория**, которая не просто "слушается команд", а **понимает физику процесса** и **учится на результатах собственной работы**.

---
