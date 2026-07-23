# Recording bot

Сервис записи конференций. В одном репозитории и одном Docker-образе работают два независимых процесса:

- `recording-bot` — захватывает WebRTC-поток комнаты, режет его на WebM-сегменты и отправляет их через API в объектное хранилище;
- `recording-composition` — получает очередь из API, скачивает сегменты по presigned URL, собирает и проверяет итоговый MP4, затем сообщает API о готовом артефакте.

S3-ключей в контейнерах нет: ими владеет только API. Бот и composition worker используют разные service secrets для закрытых API-маршрутов.

## Развёртывание

Recording-контур запускается на **media server**, рядом с `media/docker-compose.yml`. Он подключается к внешней Docker-сети `conference-media_default`, созданной media Compose. Поэтому сначала должен быть запущен media stack.

API находится на app server. Его private URL указывается в `RECORDING_API_INTERNAL_URL`; API по private сети обращается к Redis на media server. Публичный WebSocket браузеров идёт отдельно: `wss://…/ws` → Nginx → signaling.

Перед запуском заполните `.env.example` фактическими private URL и секретами либо передайте свой env-файл с теми же ключами. В production не используйте значения-заглушки из примера.

```bash
cd recording-bot
docker compose --env-file .env.example up -d --build
```

Контейнеры не публикуют порты наружу. Signaling обращается к capture-боту внутри media Docker-сети по адресу `http://recording-bot:8090`.

## Переменные окружения

Минимально необходимы:

```env
RECORDING_API_INTERNAL_URL=http://<private-api-host>:3000
RECORDING_CAPTURE_SERVICE_SECRET=<не менее 32 символов>
RECORDING_COMPOSITION_SERVICE_SECRET=<не менее 32 символов>
RECORDING_BOT_CONTROL_JWT_HS256_SECRET=<не менее 32 символов>
RECORDING_MEDIA_NETWORK_NAME=conference-media_default
```

`RECORDING_BOT_CONTROL_JWT_ISSUER`, `RECORDING_BOT_CONTROL_JWT_AUDIENCE` и TTL не задаются через env: это фиксированные значения внутреннего протокола (`conference-signaling`, `conference-recording-bot-control`, 300 секунд).

Для signaling на media server должен быть задан один worker в `RECORDING_BOT_WORKERS_JSON` с адресом `http://recording-bot:8090`. API должен иметь E12-конфигурацию PostgreSQL, Yandex Object Storage и secrets capture/composition.

## Локальная разработка

Полный локальный сценарий запускается root Compose из корня репозитория. Он использует корневой `.env.example`, поднимает API, signaling, Redis, PostgreSQL, MinIO, один capture-бот и composition worker.

Отдельный Compose этого каталога предназначен для topology media server и ожидает уже существующую внешнюю сеть media Compose.

## Git

`.env`, временные сегменты, локальные артефакты, зависимости и результаты сборки игнорируются в `recording-bot/.gitignore`. В Git добавляется только `.env.example` с безопасными значениями-заглушками. Не коммитьте S3 credentials, service secrets, готовые MP4/WebM и browser traces.