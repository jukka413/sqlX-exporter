# sqlx-exporter

Prometheus-экспортёр на Go: периодически выполняет SQL-запросы к базам данных и публикует результаты как метрики на `/metrics`. Поддерживает PostgreSQL, MySQL, MSSQL и Oracle, hot-reload конфига без рестарта Pod, multi-row метрики с динамическими лейблами и гибкую систему инклюдов для конфигурации в Kubernetes.

---

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Поддерживаемые БД](#поддерживаемые-бд)
- [Формат конфигурации](#формат-конфигурации)
    - [databases](#databases)
    - [queries](#queries)
    - [settings](#settings)
- [Запросы: single-value и multi-row](#запросы-single-value-и-multi-row)
- [Расписание (schedule)](#расписание-schedule)
- [БД по умолчанию (default_db)](#бд-по-умолчанию-default_db)
- [Инклюды конфига](#инклюды-конфига)
    - [Простой инклюд](#простой-инклюд)
    - [include_defaults — переиспользуемые файлы метрик](#include_defaults--переиспользуемые-файлы-метрик)
    - [Один файл метрик — несколько БД](#один-файл-метрик--несколько-бд)
- [Подключение к Oracle](#подключение-к-oracle)
    - [Стандартный URL](#стандартный-url)
    - [TNS-дескриптор](#tns-дескриптор)
    - [Кодировка сообщений об ошибках](#кодировка-сообщений-об-ошибках)
- [Переменные окружения в конфиге](#переменные-окружения-в-конфиге)
- [Health-метрики и мониторинг состояния](#health-метрики-и-мониторинг-состояния)
- [Hot-reload](#hot-reload)
- [Helm chart](#helm-chart)
    - [Структура values.yaml](#структура-valuesyaml)
    - [Способ 1: папка config/ в chart](#способ-1-папка-config-в-chart)
    - [Способ 2: configIncludes в values](#способ-2-configincludes-в-values)
    - [ArgoCD multi-source](#argocd-multi-source)
- [Переподключение к недоступным БД](#переподключение-к-недоступным-бд)
- [Мягкая валидация конфига](#мягкая-валидация-конфига)
- [Метрики самого приложения](#метрики-самого-приложения)
- [Память и производительность](#память-и-производительность)

---

## Быстрый старт

```bash
go build -o sqlx-exporter .
./sqlx-exporter --config ./config.yaml
```

Метрики появятся на `http://localhost:2112/metrics`.

Минимальный конфиг:

```yaml
databases:
  main:
    driver: "pgx"
    url: "postgres://user:password@localhost:5432/mydb"
    max_conns: 10
    max_idle_conns: 2

queries:
  active_connections:
    db: "main"
    sql: "SELECT count(1) FROM pg_stat_activity WHERE state = 'active'"
    timeout: "5s"
    interval: "30s"
```

---

## Поддерживаемые БД

| Драйвер в конфиге | СУБД | Go-библиотека |
|---|---|---|
| `pgx` | PostgreSQL | `github.com/jackc/pgx/v5` |
| `mysql` | MySQL / MariaDB | `github.com/go-sql-driver/mysql` |
| `sqlserver` | Microsoft SQL Server | `github.com/microsoft/go-mssqldb` |
| `oracle` | Oracle Database | `github.com/sijms/go-ora/v2` |

---

## Формат конфигурации

Конфиг — YAML-файл с тремя основными секциями: `settings`, `databases`, `queries`. Путь к файлу передаётся через `--config` (по умолчанию `./config.yaml`).

### databases

Карта подключений. Ключ — произвольное имя, используется в `queries.<name>.db`.

```yaml
databases:
  main:
    driver: "pgx"                              # pgx | mysql | sqlserver | oracle
    url: "postgres://user:${DB_PASS}@host:5432/mydb"

    max_conns: 10                # максимум открытых соединений одновременно
    max_idle_conns: 2            # сколько держать в пуле простаивающими
    max_conn_lifetime: "1h"      # принудительно закрывать соединение старше этого возраста
    max_conn_idle_time: "30m"    # закрывать соединение если не использовалось дольше этого
```

`max_conn_lifetime` и `max_conn_idle_time` важны если перед БД стоит балансировщик (PgBouncer, AWS RDS Proxy) — они часто закрывают долгоживущие соединения со своей стороны, и эти настройки позволяют закрыть соединение самостоятельно раньше, избегая ошибок `connection reset`.

### queries

Карта запросов. Ключ — имя запроса, становится именем метрики в Prometheus (если не указано иначе через клонирование для нескольких БД, см. ниже).

```yaml
queries:
  my_query:
    db: "main"                    # имя БД из databases (можно не указывать, см. default_db)
    sql: "SELECT count(1) FROM users"
    timeout: "5s"                 # таймаут на выполнение запроса
    interval: "30s"                # как часто выполнять (либо interval, либо schedule)
    labels:                        # статические лейблы, добавляются к каждой точке
      environment: "prod"
    value_column: ""               # см. раздел про multi-row метрики
```

### settings

Глобальные настройки приложения, не привязанные к конкретной БД:

```yaml
settings:
  db_reconnect_interval: "5m"   # как часто пытаться переподключиться к упавшим БД (default: 5m)
  default_db: "main"             # глобальный фолбэк БД для запросов без db (см. ниже)
```

---

## Запросы: single-value и multi-row

**Single-value** (по умолчанию) — SELECT возвращает одно число, которое становится значением метрики:

```yaml
queries:
  total_users:
    db: "main"
    sql: "SELECT count(*) FROM users"
    timeout: "5s"
    interval: "1m"
```

```
total_users{db="main"} 1542
```

**Multi-row** — SELECT возвращает несколько строк, каждая становится отдельным time series. Указываешь `value_column` — какой столбец содержит число, остальные столбцы автоматически становятся лейблами:

```yaml
queries:
  users_by_region:
    db: "main"
    sql: "SELECT region, status, count(*) as cnt FROM users GROUP BY region, status"
    value_column: "cnt"
    timeout: "5s"
    interval: "1m"
```

```
users_by_region{db="main", region="eu", status="active"} 320
users_by_region{db="main", region="us", status="idle"}   58
```

**Reconciliation**: если на следующем запуске какая-то комбинация лейблов пропала из результата (например регион `eu`/`idle` больше не существует) — соответствующая метрика автоматически удаляется из Prometheus и пропадёт с графиков в Grafana, а не зависнет с последним известным значением.

При ошибке запроса (таймаут, обрыв соединения) все метрики предыдущего успешного запуска удаляются — устаревшие данные не остаются на дашбордах молча.

---

## Расписание (schedule)

Вместо `interval` можно задать конкретные дни недели и время через `schedule` — полезно для тяжёлых отчётных запросов которые не нужно гонять каждую минуту:

```yaml
queries:
  weekly_report:
    db: "main"
    sql: "SELECT count(*) FROM big_table"
    timeout: "30s"
    schedule:
      timezone: "Europe/Moscow"
      at:
        - weekday: "mon, wed, fri"
          time: "03:00, 15:00"
        - weekday: "sun"
          time: "23:30"
```

`weekday` и `time` поддерживают список через запятую — генерируются все комбинации. В примере выше запрос выполнится: понедельник/среда/пятница в 03:00 и 15:00, и воскресенье в 23:30.

`interval` и `schedule` взаимоисключающие — указывается либо один, либо другой.

---

## БД по умолчанию (default_db)

Если у запроса не указано `db:`, оно подставляется по следующему приоритету (от самого специфичного к самому общему):

1. Явный `db:` внутри самого запроса — высший приоритет.
2. Локальный `default_db:` в корне файла, где определён запрос (см. инклюды ниже).
3. `default_db`, унаследованный от родительского файла (если дочерний файл не задал свой).
4. `settings.default_db` — глобальный фолбэк после мержа всех файлов.

```yaml
default_db: "main"   # действует только на запросы в ЭТОМ файле

databases:
  main:
    driver: "pgx"
    url: "postgres://user:${DB_PASS}@host:5432/mydb"

queries:
  q1:
    sql: "SELECT 1"      # db не указан → возьмёт "main"
    timeout: "5s"
    interval: "30s"
```

Это особенно полезно когда у вас несколько файлов метрик для разных БД — каждый файл может задать свою БД по умолчанию без необходимости прописывать `db:` в каждом отдельном запросе.

---

## Инклюды конфига

Конфиг можно разбить на несколько файлов — это критично важно для Kubernetes, где разные команды или окружения хотят управлять своим набором метрик независимо.

### Простой инклюд

```yaml
# config.yaml
includes:
  "oracle.yaml": true
  "mssql.yaml": true

databases:
  ...
```

**Важно**: `includes` — это **map**, а не список (`- "file.yaml"`). Причина в Helm: при объединении нескольких `values.yaml` файлов (multi-source ArgoCD) Helm делает глубокий мерж для map-полей, но **полностью заменяет** списки. Если бы `includes` был списком, второй `values` файл стирал бы инклюды первого. С map — оба набора объединяются автоматически.

Пути в `includes` относительны директории основного файла. Поддерживается рекурсия (инклюд может инклюдить другие файлы), глубина ограничена 10 уровнями для защиты от циклов.

Содержимое инклюд-файла мержится в основной конфиг — **инклюд имеет приоритет** и перезаписывает совпадающие ключи `databases`/`queries` основного файла. Порядок инклюдов в map не гарантирован Go, поэтому ключи обрабатываются в отсортированном порядке для детерминированности.

### include_defaults — переиспользуемые файлы метрик

Файл с метриками можно сделать полностью независимым от конкретной БД — без `databases` и без `default_db` внутри — а привязку к БД задать централизованно в основном конфиге:

```yaml
# config.yaml
include_defaults:
  oracle-metrics.yaml: "azdh_oracle"
  mssql-metrics.yaml: "main_mssql"

includes:
  "oracle-metrics.yaml": true
  "mssql-metrics.yaml": true

databases:
  azdh_oracle:
    driver: "oracle"
    url: "oracle://user:${ORACLE_PASS}@host:1521/service"
  main_mssql:
    driver: "sqlserver"
    url: "sqlserver://user:${MSSQL_PASS}@host:1433?database=mydb"
```

```yaml
# oracle-metrics.yaml — только запросы, никаких databases
queries:
  active_sessions:
    sql: "SELECT count(1) FROM v$session WHERE status = 'ACTIVE'"
    timeout: "10s"
    interval: "1m"
```

При загрузке `oracle-metrics.yaml` все его запросы без явного `db:` автоматически получат `db: "azdh_oracle"`. Файл `oracle-metrics.yaml` можно переиспользовать для любой Oracle БД — просто меняя значение в `include_defaults`, без редактирования самого файла метрик.

**Если для инклюда нет записи в `include_defaults`, файл полностью игнорируется** (его `databases`/`queries` не попадают в итоговый конфиг). Это логируется при каждом reload:

```
INFO include skipped (no include_defaults entry) detail="\"forgotten.yaml\": no entry in include_defaults"
```

Это защищает от ситуации, когда файл метрик добавлен в `includes`, но забыли указать для него БД — вместо непонятной ошибки валидации файл просто не подключается, и это видно в логах.

### Один файл метрик — несколько БД

`include_defaults` принимает не только строку, но и список БД — тогда один файл с метриками выполнится для каждой указанной БД:

```yaml
include_defaults:
  oracle-metrics.yaml:
    - "azdh_oracle"
    - "ir_test_oracle"
```

Запросы из `oracle-metrics.yaml` запустятся дважды — один раз с подключением к `azdh_oracle`, другой раз к `ir_test_oracle`. Внутренние имена воркеров получают служебный суффикс (`active_sessions__azdh_oracle`, `active_sessions__ir_test_oracle`) для уникальности, но **имя метрики в Prometheus остаётся чистым** — без суффикса. Различать данные по БД позволяет лейбл `db`, который присутствует на каждой метрике всегда:

```
active_sessions{db="azdh_oracle"}    42
active_sessions{db="ir_test_oracle"} 17
```

---

## Подключение к Oracle

### Стандартный URL

```yaml
databases:
  myoracle:
    driver: "oracle"
    url: "oracle://user:${ORACLE_PASS}@host:1521/service_name"
```

### TNS-дескриптор

Если у вас есть TNS-дескриптор (например из Vault или `tnsnames.ora`), используйте префикс `oracle+tns://`:

```yaml
databases:
  myoracle:
    driver: "oracle"
    url: "oracle+tns://user:${ORACLE_PASS}@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=host)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=service)))"
```

Также поддерживается вариант с `/?CONNSTR=` перед дескриптором (распространённый формат при экспорте из некоторых систем):

```yaml
url: "oracle+tns://user:${ORACLE_PASS}@/?CONNSTR=(DESCRIPTION=...)"
```

Оба варианта нормализуются одинаково. Пробелы внутри TNS-дескриптора убираются автоматически, так как драйвер `go-ora` не всегда корректно их обрабатывает.

**Важно при работе с переменными окружения**: если TNS-дескриптор целиком приходит из переменной окружения (например из Vault), он не URL-кодируется — экспортёр распознаёт значения похожие на TNS-дескриптор (начинаются с `(` и содержат `DESCRIPTION=`/`ADDRESS=`) и подставляет их как есть. Иначе скобки `(` `)` и знак `=` превратились бы в `%28 %29 %3D`, что сломало бы парсинг дескриптора.

### Кодировка сообщений об ошибках

Экспортёр автоматически задаёт `client charset=UTF8` при подключении к Oracle — это нужно чтобы сообщения об ошибках с кириллицей (частая ситуация для русскоязычных инсталляций Oracle) отображались корректно, а не как `\ufffd\ufffd\ufffd`. Дополнительно устанавливается `nls_language=AMERICAN` для сессии — большинство системных сообщений Oracle приходят на английском, что упрощает диагностику.

---

## Переменные окружения в конфиге

Поле `url` у каждой БД поддерживает подстановку переменных окружения через синтаксис `${VAR_NAME}`:

```yaml
databases:
  main:
    driver: "pgx"
    url: "postgres://user:${DB_PASSWORD}@host:5432/mydb"
```

**Важно**: подстановка переменных применяется **только к полю `url`**, не ко всему YAML-файлу. Это специально, потому что `$` широко встречается в SQL Oracle (`v$session`, `gv$instance`) — если бы подстановка шла по всему файлу, `v$session` превратилось бы в `v` (так как `$session` интерпретировалось бы как несуществующая переменная окружения и заменялось пустой строкой).

Значения переменных URL-кодируются автоматически (через `url.PathEscape`) — это значит пароль со спецсимволами (`@`, `/`, `#`, `%` и др.) не сломает парсинг URL:

```yaml
# DB_PASSWORD=p@ss/w0rd! в окружении
url: "postgres://user:${DB_PASSWORD}@host:5432/mydb"
# Подставится как postgres://user:p%40ss%2Fw0rd%21@host:5432/mydb
```

Исключение — значения похожие на TNS-дескриптор Oracle (см. выше) не кодируются, иначе сломается структура дескриптора.

В Kubernetes пароли обычно приходят через `envFrom` → `Secret`, созданный External Secrets Operator из Vault.

---

## Health-метрики и мониторинг состояния

Для каждого запроса автоматически публикуются три служебные метрики, удобные для алертинга:

```
app_query_up{query, db}                              # 1 = последнее выполнение успешно, 0 = ошибка
app_query_last_success_timestamp_seconds{query, db}  # unix-время последнего успеха
app_query_errors_total{query, db, reason}            # счётчик ошибок, reason: timeout|db_error|cancelled
app_query_duration_seconds{query, db}                # histogram длительности выполнения
```

Типичный алерт на зависшую метрику:

```promql
time() - app_query_last_success_timestamp_seconds > 300
```

Дополнительно публикуются метрики состояния пулов соединений:

```
app_db_pool_acquired_connections{db}   # сколько соединений сейчас занято
app_db_pool_idle_connections{db}       # сколько простаивает
app_db_pool_total_connections{db}      # всего открыто
app_db_connection_errors_total{db}     # счётчик ошибок подключения
```

---

## Hot-reload

Экспортёр следит за изменением файлов конфига через `fsnotify` и применяет изменения без перезапуска процесса. Это работает как при прямом редактировании файла, так и при обновлении ConfigMap в Kubernetes.

В Kubernetes kubelet обновляет файлы в Pod через атомарное переключение симлинка `..data` — это специфика которую экспортёр учитывает: при получении события удаления (а не модификации, как было бы при обычной записи) watcher переподписывается на директорию и инициирует reload.

При reload:

- Изменённые/новые запросы — воркеры пересоздаются.
- Удалённые запросы — воркер останавливается, метрика убирается из Prometheus registry (не остаётся "замороженной" на графиках).
- Изменённые БД — пул пересоздаётся **только после успешного подключения к новому**; если новое подключение не удалось, старый пул продолжает работать, и ошибка не обрывает существующие метрики.
- Невалидные отдельные запросы (нет `db` и нет `default_db`, несуществующая БД, плохой `timeout`) — пропускаются с логированием, но не блокируют загрузку остальных корректных запросов (см. [Мягкая валидация](#мягкая-валидация-конфига)).

Debounce 150мс защищает от множественных событий файловой системы при одном логическом изменении.

---

## Helm chart

### Структура values.yaml

```yaml
config:
  settings:
    db_reconnect_interval: "5m"
    default_db: "main"

  include_defaults: {}     # маппинг файл → БД
  includes: {}              # map, не список!
  databases: {}
  queries: {}

configIncludes: {}          # содержимое инклюд-файлов прямо в values
```

### Способ 1: папка config/ в chart

Удобно для локальной разработки, когда chart лежит рядом с кодом:

```
helm/sqlx-exporter/
└── config/
    ├── oracle.yaml
    └── mssql.yaml
```

Файлы из `config/*.yaml` автоматически читаются через `Files.Glob` в шаблоне `configmap.yaml` и попадают в ConfigMap. Достаточно перечислить их в `config.includes` values.

### Способ 2: configIncludes в values

Нужен когда chart хранится в артефактном репозитории (Nexus, ChartMuseum) и физически недоступен для редактирования — тогда содержимое инклюд-файлов передаётся прямо через values:

```yaml
# values-oracle.yaml
config:
  includes:
    "oracle-metrics.yaml": true

configIncludes:
  oracle-metrics.yaml: |
    queries:
      active_sessions:
        sql: "SELECT count(1) FROM v$session WHERE status = 'ACTIVE'"
        timeout: "10s"
        interval: "1m"
```

Оба способа работают одновременно — `configIncludes` из values имеет приоритет при совпадении имён файлов с тем, что лежит в `config/`.

### ArgoCD multi-source

Типичная конфигурация когда chart в Nexus, а конфигурация метрик и инфраструктурные values — в отдельных Git-репозиториях:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
spec:
  sources:
    - repoURL: https://nexus.company.com/repository/helm
      chart: sqlx-exporter
      targetRevision: 0.1.0
      helm:
        valueFiles:
          - $values/values.yaml
          - $values/values-oracle.yaml
          - $default/default_metrics/default_postgres.yaml
    - repoURL: https://gitlab.company.com/team/values-repo.git
      targetRevision: main
      ref: values
    - repoURL: https://gitlab.company.com/team/default-metrics.git
      targetRevision: main
      ref: default
```

Каждый `valueFiles` путь — это отдельный набор `config.includes`/`configIncludes`/`databases`, которые Helm мержит между собой. Благодаря тому что `includes` — map, набор инклюдов из разных репозиториев не перезатирает друг друга.

ExternalSecret для паролей деплоится отдельным ArgoCD Application с `directory.include`, так как Helm chart игнорирует файлы не входящие в его `templates/`:

```yaml
spec:
  source:
    repoURL: https://gitlab.company.com/team/values-repo.git
    path: monitoring/sqlx-exporter
    directory:
      include: "externalsecret.yaml"
```

---

## Переподключение к недоступным БД

Если при старте или reload подключение к БД не удалось — приложение **не падает и не блокирует остальные запросы**. Конфиг недоступной БД сохраняется отдельно, и фоновый процесс (`poolHealthChecker`) пытается переподключиться с заданным интервалом:

```yaml
settings:
  db_reconnect_interval: "5m"   # default: 5m, можно поставить и "30s" для быстрого восстановления
```

При успешном переподключении воркеры для этой БД автоматически стартуют без необходимости делать reload конфига. Интервал можно менять в конфиге на горячую — следующий тик подхватит новое значение.

---

## Мягкая валидация конфига

Валидация разделена на два уровня:

**Критичные проверки** (обрывают весь reload, если не пройдены) — структура секции `databases`: отсутствующий `driver`/`url`, некорректные durations, `max_idle_conns > max_conns`, `settings.default_db` указывает на несуществующую БД.

**Проверка отдельных запросов** (не критична) — каждый запрос в `queries` проверяется независимо. Если у одного конкретного запроса проблема (нет `db` и нет фолбэка, несуществующая БД, плохой `timeout`/`interval`/`schedule`) — этот запрос исключается из конфига с явным логом, но все остальные корректные запросы продолжают загружаться и работать:

```
ERROR skipping invalid query reason="query \"my_query\" skipped: db is required (or set default_db / settings.default_db)"
```

Это намеренное архитектурное решение: одна опечатка в одном запросе не должна останавливать сбор сотен остальных метрик.

---

## Метрики самого приложения

Помимо кастомных метрик, на `/metrics` доступны стандартные метрики Go runtime и процесса (регистрируются автоматически библиотекой `client_golang`, без явного кода):

```
go_memstats_heap_inuse_bytes      # heap занятый живыми объектами
go_memstats_heap_idle_bytes       # heap зарезервированный но свободный
go_goroutines                     # количество живых горутин
go_gc_duration_seconds            # длительность пауз GC
process_resident_memory_bytes     # RSS процесса
process_open_fds                  # открытые файловые дескрипторы
```

Полезны для диагностики потребления ресурсов самого экспортёра, отдельно от метрик которые он собирает с БД.

---

## Память и производительность

Приложение настроено на минимальное и стабильное потребление памяти в Kubernetes:

```dockerfile
# GOGC=50 — GC запускается вдвое чаще дефолта, меньше пиковый heap
ENV GOGC=50
# GOMEMLIMIT — мягкий лимит heap, держи чуть ниже resources.limits.memory
ENV GOMEMLIMIT=55MiB
# GODEBUG=madvdontneed=1 — немедленно отдавать свободные страницы ОС (важно для RSS в k8s)
ENV GODEBUG=madvdontneed=1
```

Без `GODEBUG=madvdontneed=1` Go по умолчанию использует `MADV_FREE` на Linux — освобождённые страницы остаются в RSS процесса до тех пор, пока ядро не забирает их под давлением памяти, что выглядит как "утечка" на графиках Grafana, хотя реальный heap стабилен.

Типичное потребление для конфигурации с несколькими БД и десятками запросов — 15–20 МБ resident memory после прогрева runtime в первые минуты после старта Pod.