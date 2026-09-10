# Recording bot

`recording-bot` — приватный контур записи конференций на Go/Pion WebRTC.
Он не является участником комнаты, не отображается в списке участников и не
поддерживает комнату активной.

В одном репозитории и одном Docker-образе запускаются два независимых процесса:

- `recording-capture-*` — receive-only Go/Pion worker, который получает отдельные
  аудио- и видеодорожки из mediasoup, формирует закрытые WebM/Matroska-сегменты и
  загружает их прямо в private S3 по короткоживущим presigned URL от API;
- `recording-composition` — Node.js/FFmpeg worker, который получает задачу у API,
  скачивает сегменты, собирает и проверяет итоговый MP4, загружает его по
  presigned URL и сообщает API о результате.

Chromium, browser capture и старый plain-RTP recorder удалены. Единственная
production-реализация захвата — Go/Pion.

## Схема работы

```text
Integration backend
        |
        | создаёт комнату с recordingRequested=true
        v
Conference API ---- PostgreSQL
        |                 |
        | metadata        | recording/job/artifact state
        v                 |
      Redis <-------------+
        ^
        |
Signaling/media -- private control --> Go/Pion capture worker
        |                                  |
        | receive-only WebRTC              | presigned PUT
        +---------------------------------> private S3
                                               ^
                                               |
                                    composition worker
                                               |
                                               +--> verified MP4
```

Порядок запуска записи:

1. Integration backend создаёт комнату с `recordingRequested: true`.
2. Перед входом первого live-участника signaling резервирует свободный capture
   slot. Если slot отсутствует, записываемая комната не запускается молча без
   записи, а получает явную ошибку admission.
3. Signaling выдаёт выбранному worker одноразовый bot admission и отправляет
   generation-fenced команду запуска.
4. Go/Pion worker подключается к приватному WebSocket signaling как receive-only
   recorder и получает snapshot участников и дорожек.
5. Каждая дорожка записывается независимо. Opus и VP8 упаковываются в WebM,
   H.264 — в Matroska без перекодирования capture-потока.
6. Закрытые сегменты сначала фиксируются в durable spool, затем загружаются в S3
   по выданным API presigned URL и подтверждаются через private API.
7. После закрытия комнаты capture worker запечатывает входные данные. Composition
   worker получает lease, собирает MP4, проверяет результат и только после этого
   API публикует состояние `READY`.

## Границы безопасности

- Capture и composition workers не получают `DATABASE_URL`.
- Workers не получают постоянные S3 access key и secret key.
- Capture и composition используют разные service secrets.
- Команды signaling → capture подписываются отдельным короткоживущим JWT.
- Media bytes не проходят через API и не сохраняются в Redis.
- Private S3 bucket не публикуется напрямую для браузеров.
- Незавершённые, повреждённые или конфликтующие данные не могут опубликовать
  готовый артефакт.
- Raw participant/producer IDs остаются в памяти capture-процесса; durable
  timeline использует recording-scoped opaque IDs.

## Production-топология

Production Compose запускает:

- пять независимо адресуемых контейнеров `recording-capture-1` …
  `recording-capture-5`;
- один контейнер `recording-composition`;
- отдельный named volume для каждого процесса.

Один capture slot соответствует одной одновременно записываемой комнате.
При `RECORDING_BOT_MAX_SLOTS=10` каждый capture-контейнер обслуживает до десяти
комнат, а суммарная настроенная capacity пяти worker-ов равна 50 записям.
Это инфраструктурный лимит, а не продуктовый лимит комнат или участников.

Для VM в одной приватной сети рекомендуется прямой межхостовый доступ:

```text
media     -> http://<private-recorder-ip>:8091..8095
recorder  -> http://<private-api-ip>:3000
recorder  -> ws://<private-media-ip>:4000/internal/recording-bot/v1
recorder  -> https://storage.yandexcloud.net
```

Порты `8091`–`8095` должны быть разрешены только с private IP media-хоста.
Публичный recorder nginx не требуется, если control plane полностью работает по
private IP. HTTP/WS внутри приватной сети остаётся защищён service credentials;
для дополнительной защиты можно использовать WireGuard или mTLS.

## Переменные окружения

Production pipeline принимает конфигурацию через защищённую GitLab File Variable
`DEPLOY_ENV_PROD`. Production `.env` не создаётся в репозитории и не коммитится.

### Обязательные переменные

| Переменная | Назначение |
| --- | --- |
| `RECORDING_RUNTIME_ENV` | Режим security validation; в production — `production`. |
| `RECORDING_API_INTERNAL_URL` | Базовый URL API без `/internal/v1`, например `http://10.0.0.10:3000`. |
| `RECORDING_CAPTURE_SERVICE_SECRET` | Bearer credential capture worker для private API; совпадает с API. |
| `RECORDING_COMPOSITION_SERVICE_SECRET` | Независимый credential composition worker; совпадает с API. |
| `RECORDING_BOT_CONTROL_JWT_HS256_SECRET` | Секрет control JWT; совпадает с media. |
| `RECORDING_S3_MAX_IN_FLIGHT_PUT_SECONDS` | Общий hard deadline одного S3 PUT; совпадает с API. |

Каждый secret должен содержать не менее 32 байт и не должен быть placeholder.
Production startup отклоняет значения вида `replace-with`, `change-me`,
`placeholder`, `example` и `local`.

### Capture runtime

| Переменная | Значение по умолчанию | Назначение |
| --- | ---: | --- |
| `RECORDING_BOT_BIND_HOST` | `0.0.0.0` | Адрес HTTP control server внутри контейнера. |
| `RECORDING_BOT_PORT` | `8090` | Control-порт внутри контейнера. |
| `RECORDING_BOT_BIND_ADDR` | `127.0.0.1` | Интерфейс host port publishing; для разных VM задаётся private IP recorder. |
| `RECORDING_CAPTURE_PORT_1` … `_5` | `8091` … `8095` | Host control-порты пяти capture worker-ов. |
| `RECORDING_BOT_MAX_SLOTS` | `10` в production Compose | Число одновременных записей одного worker, допустимый диапазон `1..32`. |
| `RECORDING_BOT_START_TIMEOUT_MS` | `30000` | Максимальное время запуска attempt. |
| `RECORDING_BOT_STOP_TIMEOUT_MS` | `30000` в production example | Максимальное время остановки attempt. |
| `RECORDING_BOT_RECOVERY_TIMEOUT_MS` | `30000` | Максимальное время recovery. |
| `RECORDING_STOP_GRACE_PERIOD` | `5m20s` | Grace period контейнера для корректного shutdown. |
| `RECORDING_STORAGE_ROOT` | `/var/lib/conference-recording` | Приватный durable spool. |
| `RECORDING_FFMPEG_TIMEOUT_MS` | `120000` | Таймаут capture FFmpeg subprocess. |
| `RECORDING_MAX_SOURCE_BYTES` | `4000000000` | Максимальный объём source media одной записи. |
| `RECORDING_MAX_DURATION_SECONDS` | `14400` | Максимальная продолжительность записи, 4 часа. |
| `RECORDING_BOT_LOG_LEVEL` | `info` | `debug`, `info`, `warn` или `error`. |

`RECORDING_BOT_WORKER_ID` и generation ID назначаются каждому контейнеру самим
Compose/runtime. Их не нужно вручную дублировать в общем env-файле.

### Composition runtime

| Переменная | Значение по умолчанию | Назначение |
| --- | ---: | --- |
| `RECORDING_COMPOSITION_FFMPEG_TIMEOUT_MS` | `21600000` | Максимальное время FFmpeg composition, 6 часов. |
| `RECORDING_COMPOSITION_HTTP_TIMEOUT_MS` | `120000` | Таймаут private API request. |
| `RECORDING_COMPOSITION_STOP_GRACE_PERIOD` | `5m20s` | Grace period для завершения текущей операции. |
| `RECORDING_FINALIZE_MAX_ATTEMPTS` | `3` | Число попыток финализации; совпадает с API. |
| `RECORDING_FINALIZE_RETRY_DELAY_MS` | `250` | Задержка между попытками финализации. |
| `RECORDING_COMPOSITION_PIDS_LIMIT` | `128` | Ограничение процессов composition container. |

### Ресурсы

| Переменная | Production preset | Область действия |
| --- | ---: | --- |
| `RECORDING_BOT_CPUS` | `6.0` | Каждый capture worker. |
| `RECORDING_BOT_MEMORY_LIMIT` | `8g` | Каждый capture worker. |
| `RECORDING_BOT_PIDS_LIMIT` | `512` | Каждый capture worker. |
| `RECORDING_COMPOSITION_CPUS` | `2.0` | Composition worker. |
| `RECORDING_COMPOSITION_MEMORY_LIMIT` | `4g` | Composition worker. |
| `RECORDING_COMPOSITION_PIDS_LIMIT` | `128` | Composition worker. |

При пяти capture worker-ах production preset допускает суммарно до 30 CPU и
40 GB RAM для capture плюс 2 CPU и 4 GB RAM для composition. Фактическое число
slot-ов должно подтверждаться нагрузочным тестом purchased host.

## Совпадение секретов между сервисами

| Recorder | Второй сервис | Правило |
| --- | --- | --- |
| `RECORDING_CAPTURE_SERVICE_SECRET` | одноимённая переменная API | Значения должны совпадать. |
| `RECORDING_COMPOSITION_SERVICE_SECRET` | одноимённая переменная API | Значения должны совпадать. |
| `RECORDING_BOT_CONTROL_JWT_HS256_SECRET` | одноимённая переменная media | Значения должны совпадать. |

Эти три строки используют три разных независимых секрета. Redis, PostgreSQL,
permanent S3 credentials, `JOIN_TOKEN_SECRET`, `ROOM_SESSION_TOKEN_SECRET` и
`RECORDING_CONTROL_JWT_HS256_SECRET` recorder-контейнерам не передаются.

## Health и control endpoints

Capture worker предоставляет:

- `GET /health/live` — liveness процесса без credentials;
- `GET /health/ready` — authenticated readiness, capacity, generation и
  recovery state;
- `PUT /internal/v1/attempts/{recordingId}` — generation-fenced запуск;
- `DELETE /internal/v1/attempts/{recordingId}` — bounded idempotent stop;
- `PUT /internal/v1/finalizations/{recordingId}` — fenced финализация/recovery.

Все endpoints, кроме liveness, требуют control JWT. Дополнительно worker
проверяет `X-Recording-Worker-Id` и `X-Recording-Worker-Generation-Id` для
операций изменения состояния.

Docker healthcheck capture-контейнера использует только `/health/live`: он
проверяет, что процесс отвечает, но не заявляет о готовности принимать запись.
Signaling опрашивает authenticated `/health/ready` и выдаёт новые attempts только
worker-у с доступной capacity.

Composition worker предоставляет внутренние identity-free `/health/live` и
`/health/ready`; контейнерный healthcheck использует readiness.

## Сегменты и восстановление

Capture worker хранит незавершённое состояние в отдельном private volume:

```text
/var/lib/conference-recording/
└── capture-v2/
    └── <recordingId>/
        ├── state.json
        ├── segments/
        └── staging/
```

Durable spool:

- ограничивает общий объём source media;
- не допускает абсолютные пути, `..`, symlink escape и выход из recording
  directory;
- сохраняет checksum и timeline checkpoint до подтверждения сегмента;
- после restart повторяет загрузку закрытых, но ещё не committed сегментов;
- сверяет локальное состояние с paginated recovery projection API;
- при конфликте или повреждении прекращает обработку fail-closed.

Чистое завершение запечатывает запись как `complete`. После crash или
неустранимого разрыва допустим только честный `partial` с зафиксированными gaps;
угадывать потерянную timeline или публиковать непроверенный результат нельзя.

## Docker deployment

Проверка конфигурации:

```bash
docker compose --env-file .env.example config --quiet
```

Production pipeline экспортирует путь GitLab File Variable как
`RECORDING_ENV_FILE`, обновляет пять capture worker-ов, затем composition worker
и только после этого выполняет общий `up`.

Ручной запуск с отдельным env-файлом:

```bash
export RECORDING_ENV_FILE=/secure/path/recording.prod.env
docker compose \
  --env-file "$RECORDING_ENV_FILE" \
  -f docker-compose.yml \
  up -d --build
```

Не помещайте `DATABASE_URL`, `RECORDING_S3_ACCESS_KEY_ID` или
`RECORDING_S3_SECRET_ACCESS_KEY` в recorder env. Compose намеренно очищает эти
значения в worker-контейнерах, а CI отклоняет production env с API-only
credentials.

## Локальная разработка

Полный локальный runtime запускается только root Compose из корня Conference:

```bash
npm run dev
```

Он поднимает PostgreSQL, Redis, MinIO, API, signaling, Go capture и Node
composition, применяет Prisma migrations и использует безопасные локальные
credentials из корневого `.env.example`.

Проверки Go capture:

```bash
cd recording-bot
go test ./...
go vet ./...
```

Проверки composition:

```bash
cd recording-bot/composition
npm ci
npm run typecheck
npm run lint
npm test
npm run build
```

Сборка production-образа:

```bash
docker build --target production -t conference-recording-bot:local .
```

Статическая проверка cutover из корня Conference:

```bash
npm run check:recording-cutover
docker compose --env-file .env.example config --quiet
docker compose \
  --env-file recording-bot/.env.example \
  -f recording-bot/docker-compose.yml \
  config --quiet
```

## Диагностика

Статус контейнеров:

```bash
docker compose --env-file "$RECORDING_ENV_FILE" ps
```

Логи конкретного worker-а:

```bash
docker compose --env-file "$RECORDING_ENV_FILE" logs -f recording-capture-1
```

Логи composition:

```bash
docker compose --env-file "$RECORDING_ENV_FILE" logs -f recording-composition
```

Liveness worker-а через private-интерфейс recording host:

```bash
curl -fsS http://<private-recorder-ip>:8091/health/live
```

Типовые причины недоступной записи:

- signaling не видит worker из-за неверного private IP, порта или security
  group;
- `RECORDING_BOT_CONTROL_JWT_HS256_SECRET` отличается у media и recorder;
- API недоступен по `RECORDING_API_INTERNAL_URL`;
- capture/composition service secret не совпадает с API;
- отсутствует FFmpeg/FFprobe или spool volume недоступен для записи;
- исчерпана capacity всех worker-ов;
- presigned PUT истёк или превышен общий S3 PUT deadline;
- S3/provider или private API временно недоступен.

## Ограничения и обязательные проверки перед production

- Реальная capacity определяется нагрузочным тестом host, а не только
  `RECORDING_BOT_MAX_SLOTS`.
- До production cutover требуется end-to-end smoke test с настоящим Yandex
  Object Storage.
- Нужно проверить live camera, mic, screen share, VP8 и H.264.
- Нужно проверить restart capture, restart composition, временную недоступность
  API/S3, stale lease, partial recording и отсутствие публикации повреждённого
  MP4.
- Bucket должен оставаться private; download выдаётся только через
  integration-authorized API flow.

## Git

`.gitignore` исключает `.env`, временные сегменты, MP4/WebM/Matroska,
`node_modules`, `dist`, spool data и локальные артефакты. В Git добавляются
только исходники, Docker-конфигурация и `.env.example` с безопасными
placeholder-значениями.

Никогда не коммитьте production secrets, S3 credentials, private URLs,
записанные медиафайлы или browser/network traces.
