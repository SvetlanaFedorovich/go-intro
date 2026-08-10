# Тестовое задание
Реализовать запись данных, отправленных по HTTP протоколу, в базу через Kafka.

![image](doc/schema.gif)

Нужно написать HTTP API и Worker. Kafka и Postgres поднимаются как окружение через `podman compose`.

## Что понадобится?
- Podman версии 4.1 или выше (со встроенной командой `podman compose`) или `podman-compose`.
  На macOS сначала подними виртуальную машину: `podman machine init && podman machine start`.
- Go версии 1.21 или выше для локального запуска (для контейнерного варианта не нужен).
- [Vegeta](https://github.com/tsenart/vegeta) для нагрузочного тестирования.
- Привычная среда разработки, например [VSCode](https://code.visualstudio.com/).
- Утилита для отправки сообщений в Kafka, например консольная [kcat](https://github.com/edenhill/kcat) или [плагин](https://github.com/jlandersen/vscode-kafka) для VSCode.
- Утилита для работы с БД Postgres, например [dBeaver](https://dbeaver.io/download/).

## Чуть подробнее о функционале

### HTTP API
Это сервис, который принимает запросы в формате JSON, отправленные на `/data` методом `POST` с заголовком `Content-type: application/json`.

Формат JSON следующий:
```json
{
    "user": "Max",
    "age": 31, 
    "email": "max@mail.com"
}
```
Типы данных:
- user [текст] - Имя пользователя
- age [число больше 0] - Возраст пользователя
- email [текст] - E-mail пользователя

Каждый полученный запрос с JSON от пользователя отправляются в Kafka топик `data`, который создаётся автоматически при старте окружения.

### Worker
Каждую секунду получает все доступные сообщения (JSON) из Kafka топика `data`, преобразует их в структуру и сохраняет в таблицу в базе данных `public.data`.

Структуру таблицы:
```sql
CREATE TABLE public."data" (
    id      serial4     NOT NULL,
    "user"  text        NOT NULL,
    age     int2        NOT NULL,
    email   text        NOT NULL,

    PRIMARY KEY (id)
)
```

## Окружение
В репозитории есть файл `compose.yml`, в нём описано окружение, необходимое для разработки (Kafka и Postgres).
Для работы с окружением используйте команды:
```bash
# Запустить окружение.
podman compose up -d

# Просмотр логов в потоковом режиме.
podman compose logs

# Остановить окружение.
podman compose down
```
Все команды нужно запускать в директории проекта.

> Проект использует Podman как drop-in замену Docker. Файл `Dockerfile` работает
> без изменений (Podman распознаёт его автоматически). Если предпочитаешь Docker,
> замени `podman compose` на `docker compose`, либо в Makefile: `make compose-up COMPOSE="docker compose"`.

### Kafka
- Доступна локально: `localhost:9892`
- Доступна из сети контейнеров: `kafka:9092`
-Целевой топик: `data`
- SSL не используется.

### Postgres
- Доступна локально: `localhost:5432`
- Доступна из сети контейнеров: `postgres:5432`
- База данных: `test`
- Пользователь: `postgres`
- Пароль: `pass`
- Целевая таблица: `public.data`

## Критерии проверки
1. Результат выполнения нужно загрузить в публичный репозиторий на github.
2. В репозитории должна быть инструкция для запуска вашего решения.
3. Код должен запускаться и работать на окружение, описанном в данном репозитории.
4. В случае не корректных входных данных (запроса от пользователя) должны возвращаться ошибка.
5. Будет плюсом, если напишите тесты.

---

## Решение

Реализованы два сервиса:

- **HTTP API** (`cmd/api` -> `internal/app/api`) - принимает `POST /data`, валидирует JSON и публикует сообщение в Kafka-топик `data`.
- **Worker** (`cmd/worker` -> `internal/app/worker`) - непрерывно читает сообщения
  батчами до 1000, обрабатывает их с ограниченным параллелизмом и сохраняет в
  `public.data`.

Идемпотентность строится на стабильном `event_id`. API генерирует его для
обычного запроса или детерминированно получает из заголовка `Idempotency-Key`.
Worker записывает `event_id` и hash payload непосредственно в `public.data`,
где `UNIQUE (event_id)` обеспечивает постоянную дедупликацию. Повтор события
с тем же payload пропускается, а повтор того же `event_id` с другим payload
отправляется в DLQ.

`public.processed_events` хранит диагностическую Kafka metadata. По умолчанию
Worker раз в час удаляет записи старше 30 дней (`EVENT_RETENTION`,
`EVENT_CLEANUP_INTERVAL`). Cleanup этого журнала не ослабляет идемпотентность:
при replay ограничение `data.event_id` не позволяет создать вторую строку,
а metadata записывается заново. Kafka offset не участвует в дедупликации,
поэтому пересоздание topic не вызывает ложных совпадений.

Битые сообщения (невалидный JSON / валидация) уходят в DLQ-топик `data.dlq` (заголовок `error` с причиной), затем offset коммитится - очередь не блокируется poison message.

Offset коммитится один раз на батч (кумулятивно по последнему успешно обработанному сообщению): меньше round-trip в Kafka; при ретрае батча дубли режет `UNIQUE (data.event_id)`.

Хранение в БД - две реализации за одним интерфейсом (`STORE_DRIVER`):

- `pgx` (по умолчанию) - `database/sql`-подобный драйвер `jackc/pgx`
- `gorm` - ORM `gorm.io/gorm`

```bash
STORE_DRIVER=gorm go run ./cmd/worker
```

### Запуск

Два способа. Начни с `.env`:

```bash
cp .env.example .env
```

#### Вариант A - всё в контейнерах (рекомендуется)

`podman compose up` поднимает весь стек: Kafka, Postgres **и оба приложения** (`api`, `worker`). Ничего больше запускать не нужно.
Compose использует PostgreSQL 17.10 и Confluent Platform 7.9.8 (Kafka 3.9).
Kafka и ZooKeeper сохраняют согласованное состояние в именованных volumes
`kafka_data`, `zookeeper_data` и `zookeeper_log`. Обычный `podman compose down`
их сохраняет, а `podman compose down -v` удаляет вместе со всеми сообщениями.

```bash
podman compose up -d --build
```

Каталог данных PostgreSQL 11 нельзя напрямую открыть PostgreSQL 17. При
обновлении существующего окружения сначала сделай backup/restore или `pg_upgrade`.
Если локальные данные не нужны, останови compose и удали `data/postgres` перед
первым запуском PostgreSQL 17.

Приложения собираются одним multi-stage `Dockerfile` в общий образ `localhost/go-intro:latest`. У `api` и `worker` одинаковые `build`-context - образ собирается один раз, второй build берётся из кэша слоёв (Podman compose не гарантирует порядок сборки shared-образа между сервисами, поэтому явный build задан у обоих). Внутри compose-сети адреса - по именам сервисов (`kafka:9092`, `postgres:5432`), поэтому `KAFKA_BROKERS`/`POSTGRES_DSN` для контейнеров заданы в `compose.yml` и перекрывают `localhost`-дефолты из `.env`.

Схема находится в одном файле `migrations/0001_up_schema.sql`. Миграция
создаёт отсутствующие таблицы и индекс через `IF NOT EXISTS`, не удаляя
существующие данные. Она предназначена для bootstrap чистой БД; изменение уже
существующей устаревшей схемы нужно выполнять отдельной upgrade-миграцией.
После добавления `data.event_id` старый локальный volume нужно пересоздать:
предыдущая схема не содержит связи между строкой `data` и обработанным событием,
поэтому корректный автоматический backfill невозможен.

```bash
podman compose up -d postgres
podman exec -i postgres psql -v ON_ERROR_STOP=1 -U postgres -d test \
  < migrations/0001_up_schema.sql
```

#### Вариант B - инфраструктура в контейнерах, приложения локально

Для разработки удобнее гонять сервисы через `go run`, а в контейнерах держать только Kafka и Postgres.

```bash
# 1. Только инфраструктура (без api/worker)
podman compose up -d --build kafka postgres

# 2. Зависимости
go mod tidy

# 3. go run НЕ читает .env - экспортируй переменные в shell
set -a && source .env && set +a

# 4. Worker (терминал 1) и API (терминал 2)
go run ./cmd/worker
go run ./cmd/api
```

Здесь адреса берутся из `.env` (`localhost:9892` / `localhost:5432`) - приложения ходят в контейнеры через проброшенные на хост порты.

Через Makefile (вариант B):

```bash
make compose-up
make run-worker   # терминал 1
make run-api      # терминал 2
```

### Проверка

```bash
curl -X POST http://localhost:8080/data \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-request-1' \
  -d '{"user":"Max","age":31,"email":"max@mail.com"}'
```

Ожидаемый ответ: `202 Accepted` с телом
`{"event_id":"...","status":"accepted"}`. Повтор запроса с тем же
`Idempotency-Key` получает тот же `event_id` и не создаёт вторую строку.

Некорректные данные (например `"age": 0`) возвращают `400` с описанием ошибки.
Запрос обязан иметь `Content-Type: application/json`, содержать ровно один
JSON-объект и не превышать 1 MiB (`415`, `400` и `413` соответственно).

Проверка записи в БД:

```bash
podman exec -it postgres psql -U postgres -d test -c 'SELECT * FROM public."data";'
```

### Просмотр сообщений Kafka и проверка DLQ

Показать все сообщения основного топика `data` вместе с ключами, заголовками
и временем создания:

```bash
podman exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic data \
  --from-beginning \
  --property print.key=true \
  --property print.headers=true \
  --property print.timestamp=true
```

Без `--from-beginning` consumer будет показывать только новые сообщения.
Остановка просмотра - `Ctrl+C`.

Чтобы проверить DLQ, отправь в основной топик заведомо невалидный JSON:

```bash
printf 'not-json\n' | podman exec -i kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic data
```

Worker прочитает сообщение, не сможет декодировать его и опубликует исходное
значение в `data.dlq` с заголовком `error`. Посмотреть DLQ:

```bash
podman exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic data.dlq \
  --from-beginning \
  --property print.headers=true \
  --property print.timestamp=true
```

### Наблюдаемость

Стек включает Prometheus, Grafana и Tempo:

```bash
# Собрать/запустить приложения и весь observability-стек
make grafana-up

# Остановить только Grafana, Prometheus и Tempo
make observability-down
```

- Grafana: [http://localhost:3000](http://localhost:3000), по умолчанию `admin` / `admin`
- Prometheus: [http://localhost:9090](http://localhost:9090)
- Tempo API: [http://localhost:3200](http://localhost:3200)
- API metrics: [http://localhost:8080/metrics](http://localhost:8080/metrics)
- Worker metrics: [http://localhost:8081/metrics](http://localhost:8081/metrics)

Dashboard **Go Intro Observability** создаётся автоматически и показывает
request rate/latency, статусы HTTP, результаты Worker, consumer lag, DLQ,
retry, Kafka publish и длительность batch. Трейсы доступны в Grafana Explore
через datasource **Tempo**.

API создаёт или принимает `X-Request-ID`, возвращает его клиенту и передаёт
в Kafka вместе с W3C `traceparent`. Worker продолжает тот же trace и добавляет
`request_id`, `trace_id`, topic/partition/offset в JSON-логи. Пользовательские
`user` и `email` в логи не выводятся.

Kafka publish/commit/lag и операции PostgreSQL (`connect`, `insert`, `cleanup`,
`ping`) повторяются только ограниченное число раз с exponential backoff и full jitter.
Параметры `RETRY_MAX_ATTEMPTS`, `RETRY_BASE_DELAY` и `RETRY_MAX_DELAY`
настраиваются в `.env`; каждый фактический повтор виден в метрике
`go_intro_retries_total{operation=...}` и на панели retry в Grafana.

### Нагрузочное тестирование: 5000 RPS

Vegeta запускает open-loop нагрузку на API: отсутствие свободного worker не
снижает заданный rate молча, а проявляется как рост latency/ошибок. Запросы
намеренно отправляются без `Idempotency-Key`, поэтому каждое успешное обращение
создаёт отдельное событие.

#### Запуск платформы с нуля

```bash
# 1. Клонировать репозиторий и перейти в него
git clone <repository-url>
cd go-intro

# 2. Создать локальный env
cp .env.example .env

# В .env установи 1% tracing, иначе экспорт всех 300 000 spans
# сам станет заметной частью нагрузки:
python3 - <<'PY'
from pathlib import Path
path = Path(".env")
path.write_text(path.read_text().replace(
    "OTEL_TRACE_SAMPLE_RATIO=1",
    "OTEL_TRACE_SAMPLE_RATIO=0.01",
))
PY

# 3. Установить Vegeta (macOS)
brew install vegeta

# 4. Собрать и поднять API, Worker, Kafka, Postgres, Prometheus, Tempo, Grafana
make grafana-up

# 5. Проверить готовность
curl -fsS http://localhost:8080/readyz
curl -fsS http://localhost:8081/readyz
curl -fsS http://localhost:9090/-/ready
curl -fsS http://localhost:3200/ready
curl -fsS http://localhost:3000/api/health
```

Для полностью чистого повторного запуска сначала выполни `podman compose down`.
Каталоги `data/postgres` и `data/kafka` сохраняются намеренно; удаляй их только
если действительно нужно уничтожить локальную БД и Kafka.

Открой dashboard **Go Intro Observability**:
[http://localhost:3000/d/go-intro-observability/go-intro-observability](http://localhost:3000/d/go-intro-observability/go-intro-observability).

#### Smoke и полный тест

```bash
# Сначала 100 RPS в течение 5 секунд
make load-test-smoke

# Основной критерий: 5000 RPS в течение 1 минуты
make load-test
```

Значения можно переопределить:

```bash
RATE=5000/s DURATION=2m WORKERS=512 MAX_WORKERS=20000 make load-test
```

Результаты сохраняются в `loadtest/results/`:

- `.txt` - человекочитаемый Vegeta report;
- `.json` - машинный отчёт;
- `.html` - интерактивный latency plot;
- `.bin` - исходные результаты для повторного `vegeta report`.

Автоматический критерий успешности:

- offered rate не ниже `5000 RPS`;
- не менее `99%` успешных HTTP-ответов;
- successful throughput не ниже `4950 RPS`.

Короткий локальный verification burst (`5000 RPS`, 2 секунды) дал 10 000
ответов `202`, success `100%`, throughput `4984 RPS`, p95 `15.5 ms`,
p99 `40.8 ms`; Worker сохранил все 10 000 событий и вернул consumer lag к
нулю. Это проверка конфигурации, а не замена основного минутного прогона:
результат зависит от CPU, ВМ Podman (`podman machine`) и диска конкретной машины.

Порог latency намеренно не придуман: p95/p99 выводятся в отчёте и Grafana, но
SLA должен задаваться требованиями продукта. При необходимости пороги можно
переопределить через `MIN_SUCCESS_RATIO` и `MIN_THROUGHPUT_RPS`.

Минутный тест создаёт около 300 000 событий. После того как Worker обработал
очередь, тестовые записи можно удалить:

```bash
make load-test-clean
```

#### На что смотреть в Grafana

1. **API request rate** - должен удерживаться около `5000 RPS`.
2. **API success rate / HTTP responses by status** - не ниже `99%`, основной
   успешный код - `202`; рост `5xx` означает, что API/Kafka не держат нагрузку.
3. **API latency p95/p99** - не должны постоянно расти по ходу теста.
4. **API in-flight requests** - устойчивый рост означает saturation API или
   синхронного Kafka producer.
5. **Kafka publish latency p99 / publish results** - рост latency и `error`
   локализуют проблему между API и Kafka.
6. **Worker throughput / consumer lag** - lag может вырасти во время burst,
   но после теста должен снижаться и вернуться к исходному уровню. Если он
   продолжает расти при постоянной нагрузке, Worker не обеспечивает 5000
   событий/с end-to-end, даже если API принимает 5000 запросов/с.
7. **Worker message latency / end-to-end event latency** - показывают время
   обработки и полную задержку Kafka→Postgres.
8. **DLQ / retries** - в штатном тесте должны оставаться нулевыми.
9. **Process CPU, memory, goroutines** - ищи упор в CPU и монотонный рост
   памяти/goroutines после завершения теста.

Vegeta report - источник истины со стороны клиента, а Grafana нужна для
локализации bottleneck. Если генератор сам не выдаёт 5000 RPS, проверь его CPU
и лимит файловых дескрипторов (`ulimit -n`); при необходимости запускай Vegeta
с отдельной машины.

### Тесты

```bash
# Быстрые unit-тесты
go test ./...
# или
make test

# Unit-тесты с race detector
make test-race

# Изолированные integration + E2E:
# Kafka publish/consume/commit/lag, обе реализации store (pgx/GORM),
# concurrent idempotency, HTTP → Kafka → Worker → PostgreSQL
# и повтор Idempotency-Key.
make test-integration
```

`make test-integration` использует отдельные временные volumes. Локальные
данные из `data/postgres` и `data/kafka` не затрагиваются; после теста временные
контейнеры и volumes автоматически удаляются.

### Конфигурация (env)

Значения по умолчанию зашиты в `internal/config`. Шаблон - [`.env.example`](.env.example).

- `podman compose` читает `.env` сам (подстановка в `compose.yml`, в т.ч. создание топиков `KAFKA_TOPIC` / `KAFKA_DLQ_TOPIC`).
- `go run` **не** читает `.env` - экспортируй переменные в shell (`set -a && source .env && set +a`) либо передавай явно.

| Переменная | По умолчанию | Описание |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Адрес HTTP API |
| `KAFKA_BROKERS` | `localhost:9892` | Брокеры Kafka |
| `KAFKA_TOPIC` | `data` | Входной топик |
| `KAFKA_DLQ_TOPIC` | `data.dlq` | DLQ для битых сообщений |
| `KAFKA_GROUP` | `go-intro-worker` | Consumer group Worker |
| `POSTGRES_DSN` | `postgres://postgres:pass@localhost:5432/test?sslmode=disable` | DSN Postgres (worker) |
| `POSTGRES_USER` / `PASSWORD` / `DB` | `postgres` / `pass` / `test` | Только для контейнера Postgres (compose) |
| `STORE_DRIVER` | `pgx` | Драйвер БД worker: `pgx` или `gorm` |
| `EVENT_RETENTION` | `720h` | TTL записей `processed_events` |
| `EVENT_CLEANUP_INTERVAL` | `1h` | Период очистки ledger |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTLP/HTTP endpoint Tempo для локального запуска |
| `OTEL_TRACE_SAMPLE_RATIO` | `1` | Доля трассируемых запросов от 0 до 1 |
| `GRAFANA_ADMIN_USER` | `admin` | Локальный пользователь Grafana |
| `GRAFANA_ADMIN_PASSWORD` | `admin` | Локальный пароль Grafana |
