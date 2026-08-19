# Recording bot

`recording-bot` — отдельный приватный сервис записи конференций. Он не является
участником комнаты и не влияет на её жизненный цикл.

В одном репозитории и Docker-образе запускаются два разных контейнера:

- `recording-bot` — receive-only WebRTC-клиент: получает потоки комнаты,
  формирует WebM-сегменты и загружает их в S3 по короткоживущим URL от API;
- `recording-composition` — забирает закрытую задачу композиции у API,
  скачивает сегменты, собирает MP4 и сообщает API о результате.

Контейнеры не получают `DATABASE_URL`, S3 access key или S3 secret key. Эти
данные принадлежат только API. Бот и composition worker используют независимые
service secrets только для внутренних API-маршрутов.

## Что должно быть запущено

Контур записи разворачивается на том же хосте, что и media/signaling. Перед его
запуском должны быть доступны:

1. signaling, которому заданы пять capture worker-ов в
   `RECORDING_BOT_WORKERS_JSON`;
2. API в `RECORDING_API_INTERNAL_URL`;
3. media и recording host, доступные друг другу по control URL worker-ов;
4. Redis media server, доступный API.

Публичный трафик пользователей не проходит через recording-bot:

```text
browser -> wss://media.victoryspace.ru/ws -> signaling
signaling -> https://recorder.victoryspace.ru/workers/1..5 (nginx, TLS) -> capture workers
capture/composition workers -> API -> presigned S3 URLs
```

## Nginx (TLS-терминация)

`docker-compose.nginx.prod.yml` — edge-оверлей по образцу
`victory-fabric-front`: `cert-bootstrap` (self-signed placeholder) → `nginx`
(:80/:443, редирект на HTTPS) → `certbot` (Let's Encrypt webroot, продление
каждые 12ч, SIGHUP reload nginx через docker.sock).

Маршрутизация: `https://recorder.victoryspace.ru/workers/N/...` →
`recording-capture-N:8090/...` (префикс `/workers/N` срезается). Воркеры stateful
и обнесены fence-заголовками, поэтому round-robin запрещён — путь жёстко
закреплён за своим воркером. `controlUrl` в `RECORDING_BOT_WORKERS_JSON` у
signaling = `https://recorder.victoryspace.ru/workers/N`.

Запуск (нужны поднятый базовый стек `conference-recording` — nginx подключается
к его внешней сети `conference-recording_default` — A-запись
`recorder.victoryspace.ru` на этот хост и `LETSENCRYPT_EMAIL` в `.env`):

```bash
docker compose -f docker-compose.nginx.prod.yml up -d --build
```

Прямые host-порты `8091..8095` (`RECORDING_BOT_BIND_ADDR=127.0.0.1`) остаются
как debug/fallback-доступ к воркерам в обход nginx.

## Production

Production-конфигурация передаётся через **GitLab CI/CD Variables**. Не
создавайте и не коммитьте production `.env`; `.env.example` содержит только
локальные безопасные заглушки.

Pipeline/deployment должен передать контейнерам как минимум:

```text
RECORDING_API_INTERNAL_URL
RECORDING_CAPTURE_SERVICE_SECRET
RECORDING_COMPOSITION_SERVICE_SECRET
RECORDING_BOT_CONTROL_JWT_HS256_SECRET
```

Дополнительно обычно задаются лимиты ресурсов:

```text
RECORDING_BOT_CPUS
RECORDING_BOT_MEMORY_LIMIT
RECORDING_BOT_PIDS_LIMIT
RECORDING_COMPOSITION_CPUS
RECORDING_COMPOSITION_MEMORY_LIMIT
```

Текущий production preset: пять capture worker-ов по `6 CPU`, `8 GB` RAM и
`512` PID каждый; composition worker — `2 CPU` и `4 GB` RAM. Суммарный CPU
limit равен `32`.

S3-переменные (`RECORDING_S3_*`), PostgreSQL и callback-настройки задаются
только у API. В recording-bot их быть не должно.

Фиксированные параметры внутреннего JWT не требуют env-переменных:

```text
issuer:   conference-signaling
audience: conference-recording-bot-control
ttl:      300 seconds
```

## Переменные `.env.example`

Файл предназначен только для local runtime. Перед локальной проверкой замените
заглушки в своей рабочей копии на значения локального API и service secrets.
В Git остаётся шаблон без секретов.

| Переменная | Назначение |
| --- | --- |
| `RECORDING_API_INTERNAL_URL` | URL API, доступный с recording host. |
| `RECORDING_CAPTURE_SERVICE_SECRET` | Секрет capture worker для внутренних API записи. |
| `RECORDING_COMPOSITION_SERVICE_SECRET` | Независимый секрет composition worker. |
| `RECORDING_BOT_CONTROL_JWT_HS256_SECRET` | Секрет команд signaling → capture bot. |
| `RECORDING_BOT_PUBLIC_ORIGIN` | Runtime URL Chromium внутри capture container; Compose фиксирует `http://127.0.0.1:8090`. |
| `RECORDING_BOT_BIND_ADDR` | Host bind address для control-портов capture worker-ов. |
| `RECORDING_CAPTURE_PORT_1` … `RECORDING_CAPTURE_PORT_5` | Control-порты пяти capture worker-ов (`8091` … `8095`). |
| `RECORDING_BOT_MAX_SLOTS` | Одновременное число записываемых комнат на один capture worker. |
| `RECORDING_BOT_CPUS` / `RECORDING_BOT_MEMORY_LIMIT` / `RECORDING_BOT_PIDS_LIMIT` | Лимиты каждого capture worker (`6.0` / `8g` / `512`). |
| `RECORDING_COMPOSITION_CPUS` / `RECORDING_COMPOSITION_MEMORY_LIMIT` | Лимиты composition worker (`2.0` / `4g`). |
| `LETSENCRYPT_EMAIL` | Email для сертификата Let's Encrypt домена `recorder.victoryspace.ru` (`docker-compose.nginx.prod.yml`). |

Один слот — одна одновременно записываемая комната. Production Compose запускает
пять capture worker-ов с разными `RECORDING_BOT_WORKER_ID`; при
`RECORDING_BOT_MAX_SLOTS=10` суммарная capacity равна 50 записям.

## Git

Репозиторий самостоятельный:

```bash
git clone git@git.victory-crm.ru:conference/recording-bot.git
cd recording-bot
```

Если папка уже содержит рабочий код, не выполняйте `git clone` поверх неё.
Привяжите существующую ветку к remote:

```bash
git remote add origin git@git.victory-crm.ru:conference/recording-bot.git
git push -u origin <branch-name>
```

Если `origin` уже существует, используйте `git remote set-url origin <url>`.

`.gitignore` исключает `.env`, временные сегменты, MP4/WebM, browser traces,
`node_modules` и `dist`. В Git добавляются исходный код, Docker-файлы и
`.env.example` без реальных секретов.
