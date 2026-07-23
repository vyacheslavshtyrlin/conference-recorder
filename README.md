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

1. media Compose и внешняя Docker-сеть `conference-media_default`;
2. signaling, которому указан worker `http://recording-bot:8090` в
   `RECORDING_BOT_WORKERS_JSON`;
3. API по private-сети в `RECORDING_API_INTERNAL_URL`;
4. Redis media server, доступный API по private-сети.

Публичный трафик пользователей не проходит через recording-bot:

```text
browser -> wss://<public-host>/ws -> nginx -> signaling
signaling -> http://recording-bot:8090 -> WebRTC receive-only capture
recording workers -> private API -> presigned S3 URLs
```

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
RECORDING_MEDIA_NETWORK_NAME
```

Дополнительно обычно задаются лимиты ресурсов:

```text
RECORDING_BOT_CPUS
RECORDING_BOT_MEMORY_LIMIT
RECORDING_BOT_PIDS_LIMIT
RECORDING_COMPOSITION_CPUS
RECORDING_COMPOSITION_MEMORY_LIMIT
```

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
| `RECORDING_API_INTERNAL_URL` | Private URL API, доступный из Docker-сети media server. |
| `RECORDING_CAPTURE_SERVICE_SECRET` | Секрет capture worker для внутренних API записи. |
| `RECORDING_COMPOSITION_SERVICE_SECRET` | Независимый секрет composition worker. |
| `RECORDING_BOT_CONTROL_JWT_HS256_SECRET` | Секрет команд signaling → capture bot. |
| `RECORDING_MEDIA_NETWORK_NAME` | Имя внешней Docker-сети, созданной media Compose. |
| `RECORDING_BOT_MAX_SLOTS` | Одновременное число записываемых комнат на capture worker. |

Один слот — одна одновременно записываемая комната. Увеличивать его можно
только вместе с измеренным запасом CPU/RAM для Chromium и FFmpeg.

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
