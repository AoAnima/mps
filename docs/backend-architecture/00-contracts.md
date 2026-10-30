# 00. Контракты и общий словарь (v1, заморожен)

> Статус: **FROZEN**. Все главы набора обязаны использовать типы, имена и форматы из этого файла
> без изменений. Расширять можно (новые поля/коды), противоречить — нельзя.
> Версия контракта: `1`. Ломающие изменения → `2` + миграция клиентов.

---

## 1. Границы: сервисы и их ответственность

Продукт: PhytoLedger — маркетплейс редких растений с живыми аукционами, чатом, мульти-магазинами,
реестром селекции, escrow-кошельком. Требуется real-time (`< 100 мс` на обновление стакана/чата),
SEO-индексируемые страницы, частичные обновления блоков без перезагрузки.

| Компонент | Имя в тексте | Ответственность | Состояние |
|---|---|---|---|
| API-шлюз | `gateway` | TLS, HTTP/1.1+2, WS, SSE; аутентификация; rate limit; сборка `Message`; маршрутизация; сборка ответа | stateless |
| Исполнитель маршрутов | `composer` | резолв манифеста маршрута из БД, план DAG, батчевые запросы, политики доступа, кэш данных, сборка `ViewDoc` | stateless (кэш в Redis) |
| Рендер | `renderer` | fragment registry, Go-шаблоны, HTML фрагментов и страниц, стриминг, экранирование | stateless |
| Real-time хаб | `hub` | WS/SSE соединения, подписки, ACL тем, `seq`, backpressure, resume | соединения в памяти + Redis cursor |
| Воркеры | `workers` | аукционы (таймеры/расчёт), escrow, outbox→события, уведомления, индексация, AI-описания, анкоринг в блокчейн | stateful через БД |

**Правило физического развёртывания:** на старте `gateway`, `composer` и `renderer` — один бинарник
(`cmd/app`) с внутренними портами-интерфейсами; `hub` и `workers` — отдельные процессы.
Физическое разделение `renderer`/\`composer` — вопрос масштабирования, а не дизайна:
границы заданы интерфейсами из §7, поэтому сплит не требует переписывания.

**Инфраструктура:** PostgreSQL 16 (данные, маршруты, outbox), Redis 7 (кэш, rate limit, hot-state
аукционов, presence, ticket-ы), NATS JetStream (события и fan-out), S3/MinIO (медиа), OpenSearch
(поиск/каталог), внешние: Vision-LLM (описания сортов), blockchain anchor (Polygon/Arbitrum).

---

## 2. Словарь

| Термин | Значение |
|---|---|
| **Route (маршрут)** | Декларативный манифест в БД: какой шаблон URL, какие шаги сбора данных, какие блоки, какие права. Версионируется. |
| **Fragment (блок)** | Адресуемая часть страницы с собственным шаблоном, данными и ETag. Идентификатор `<area>.<name>`. |
| **DataSet** | Результат исполнения маршрута: `map[stepID]any` + метаданные. Внутренний тип `composer`. |
| **ViewDoc** | Контракт ответа `composer → renderer/gateway → клиент` (§5). |
| **Envelope** | Единый конверт ввода/вывода для всех транспортов (§4, §6). |
| **Topic** | Адресуемый поток событий, напр. `auction:9f1c…`, `chat:room:9f1c…`, `user:7a2b…`. |
| **Seq** | Монотонный номер события внутри топика. Клиент применяет строго по возрастанию. |
| **Ticket** | Одноразовый короткоживущий токен для апгрейда HTTP→WS/SSE. |
| **Degraded** | Режим, когда часть фрагментов не собрана, но страница отдана. |

---

## 3. Инварианты (нарушение = блокирующий баг)

1. `Message` неизменяем после сборки в `gateway`. Все шаги получают копию по значению.
2. **Маршрут не содержит SQL.** Только ссылки на именованные запросы из реестра (`query: catalog.get_product_by_slug`).
3. `ViewDoc` не содержит секретов: e-mail, телефоны, хэши паролей, чужие балансы, внутренние флаги модерации.
4. Каждый фрагмент рендерится **только** из полей, объявленных в его data-контракте (§6.3).
5. Денежные операции (ставка, escrow, перевод) идут **только** через HTTP-команду с `Idempotency-Key`
   и транзакцию PostgreSQL + outbox. WS/SSE — доставка, не источник истины.
6. Любой внешний вызов и шаг маршрута ограничен дедлайном `Message.Meta.Deadline`. Fan-out без лимита запрещён.
7. Ответы анонимов кэшируемы; ответы с деньгами/персональными данными — `Cache-Control: private, no-store`.
8. Клиент никогда не считает цену, рейтинг, права или итог аукциона — только отображает серверное значение.

---

## 4. Входящий конверт: `Message`

```go
package message // internal/message

type Transport string

const (
	TransportHTTP Transport = "http"
	TransportWS   Transport = "ws"
	TransportSSE  Transport = "sse"
)

type Kind string

const (
	KindPage     Kind = "page"              // GET полной страницы
	KindFragment Kind = "fragment"          // GET набора блоков (AJAX)
	KindCommand  Kind = "command"           // POST/PATCH/DELETE
	KindSub      Kind = "stream.subscribe"  // подписка в WS
	KindSubSSE   Kind = "sse.subscribe"     // подписка в SSE
)

type Message struct {
	Meta   Meta              `json:"meta"`
	Auth   AuthContext       `json:"auth"`
	Kind   Kind              `json:"kind"`
	Route  RouteRef          `json:"route"`
	Path   string            `json:"path"`             // "/p/monstera-mint"
	Params map[string]string `json:"params"`           // path-параметры
	Query  map[string]string `json:"query"`            // нормализованный query (значения уже провалидированы)
	Body   json.RawMessage   `json:"body,omitempty"`   // только для KindCommand
	Hints  Hints             `json:"hints"`
}

type Meta struct {
	SchemaVersion  string    `json:"schema_version"` // "1"
	RequestID      string    `json:"request_id"`     // ULID, генерируется в gateway
	TraceID        string    `json:"trace_id"`       // W3C traceparent
	ClientID       string    `json:"client_id"`      // анонимный id устройства (cookie pl_cid)
	SessionID      string    `json:"session_id"`     // pl_sid, может быть пустым
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Transport      Transport `json:"transport"`
	ReceivedAt     time.Time `json:"received_at"`
	Deadline       time.Time `json:"deadline"`       // ReceivedAt + бюджет маршрута
	UAClass        string    `json:"ua_class"`       // desktop|mobile|tablet|bot
}

type RouteRef struct {
	ID      string `json:"id"`      // "product.page"
	Version int    `json:"version"` // применённая версия манифеста
	Pattern string `json:"pattern"` // "/p/{slug}"
}

type Hints struct {
	Locale    string   `json:"locale"`            // "ru-RU"
	Currency  string   `json:"currency"`          // "RUB"
	Fragments []string `json:"fragments,omitempty"` // какие блоки просит AJAX
	NoCache   bool     `json:"no_cache"`
	LastSeq   int64    `json:"last_seq,omitempty"` // Last-Event-ID для догона
	Viewport  string   `json:"viewport,omitempty"` // mobile|desktop
}

type AuthMethod string

const (
	AuthAnonymous AuthMethod = "anonymous"
	AuthSession   AuthMethod = "session"   // HttpOnly cookie pl_sid
	AuthBearer    AuthMethod = "bearer"    // API-ключ для мобильных/серверов
	AuthSigned    AuthMethod = "signed"    // HMAC-подпись сервис-сервис
	AuthTicket    AuthMethod = "ticket"    // одноразовый ticket для WS/SSE
)

type AuthContext struct {
	Method  AuthMethod `json:"method"`
	Subject Subject    `json:"subject"`
	Scopes  []string   `json:"scopes"` // вычисленные permissions: "catalog.read", "auction.bid", "wallet.read"
}

type Subject struct {
	ID       string   `json:"id"`        // UUID или ""
	Roles    []string `json:"roles"`     // user|seller|moderator|admin
	ShopIDs  []string `json:"shop_ids"`  // магазины владельца
	Level    string   `json:"level"`     // anonymous|user|seller|admin
	Verified bool     `json:"verified"`  // e-mail подтверждён
}
```

**Нормализация (gateway, до сборки Message):** путь канонизируется (trailing slash, %-декодирование,
lowercase для slug), query приводится к allowlist-типам из манифеста, неизвестные параметры
отбрасываются (не 400), `locale`/`currency` разрешаются по cookie → заголовку → дефолту.

---

## 5. Исходящий конверт: `ViewDoc`

```go
type ViewDoc struct {
	RequestID string         `json:"request_id"`
	Route     RouteRef       `json:"route"`
	Page      *Page          `json:"page,omitempty"`    // nil для чистого AJAX-ответа фрагментов
	Fragments []Fragment     `json:"fragments"`
	State     map[string]any `json:"state,omitempty"`  // мелкое состояние: "cart.count", "wallet.balance"
	Meta      ViewMeta       `json:"meta"`
}

type Page struct {
	ID        string   `json:"id"`         // "product.page"
	Template  string   `json:"template"`   // "pages/product"
	Fragments []string `json:"fragments"`  // порядок слотов в документе
	Title     string   `json:"title"`
	Status    int      `json:"status"`      // 200|404|410
}

type Fragment struct {
	ID      string          `json:"id"`            // "product.gallery"
	Version int             `json:"v"`             // версия контракта блока
	Status  FragmentStatus  `json:"status"`        // ok|empty|degraded|error
	HTML    string          `json:"html,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"` // для JSON-клиентов и тестов
	ETag    string          `json:"etag"`
	TTL     int             `json:"ttl"`            // секунды; 0 = не кэшировать
	Seq     int64           `json:"seq,omitempty"`  // только для потоковых обновлений
	Error   *FragmentError  `json:"error,omitempty"`
}

type FragmentStatus string

const (
	FragOK       FragmentStatus = "ok"
	FragEmpty    FragmentStatus = "empty"     // данных нет, рендерим пустое состояние
	FragDegraded FragmentStatus = "degraded"  // шаг упал/таймаут, рендерим skeleton
	FragError    FragmentStatus = "error"     // блок не рендерится, клиент оставляет старый
)

type FragmentError struct {
	Code    string `json:"code"`    // из §8
	Message string `json:"message"` // безопасный для показа текст
}

type ViewMeta struct {
	RouteVersion int      `json:"route_version"`
	DurationMS   int      `json:"duration_ms"`
	Cache        string   `json:"cache"`   // hit|miss|bypass
	Degraded     []string `json:"degraded,omitempty"`
	NextSeq      int64    `json:"next_seq,omitempty"`
}
```

Для обычного (не-AJAX) запроса `renderer` собирает HTML-документ из `Page` + `Fragments`;
`ViewDoc` всё равно формируется целиком — это единственный источник данных для шаблонов.

---

## 6. Маршрут (manifest) — v1

### 6.1. Пример: `product.page`

```yaml
id: product.page
version: 12
status: published            # draft|published|retired
match:
  methods: [GET]
  path: /p/{slug}
  # вариант по уровню доступа выбирается при резолве
  variants:
    - when: {auth_level: anonymous}
      steps: [load.product.public, load.seller, load.similar, load.care]
    - when: {auth_level: ">=user"}
      steps: [load.product.full, load.seller, load.similar, load.care, load.viewer]
defaults:
  budget_ms: 400
  cache: {ttl: 30s, swr: 120s, vary: [locale, currency, auth_level]}
steps:
  - id: load.product.public
    kind: sql
    query: catalog.get_product_public     # именованный запрос из реестра, НЕ строка SQL
    args: {slug: "$params.slug"}
    out: product
    require: not_null                    # not_null => 404, если пусто
    policy: catalog.read
    row_filter: "visibility = 'public'"
  - id: load.seller
    kind: sql
    query: sellers.get_public_by_id
    args: {id: "$steps.product.owner_id"}
    out: seller
    optional: true
  - id: load.similar
    kind: search
    index: products
    query: similar_by_variety
    args: {variety_id: "$steps.product.variety_id", limit: 8}
    out: similar
    optional: true
    timeout_ms: 80
  - id: load.care
    kind: const
    out: care
    value: {source: "$steps.product.care_json"}
  - id: load.viewer
    kind: sql
    query: users.get_viewer_flags
    args: {user_id: "$auth.subject.id", product_id: "$params.slug"}
    out: viewer
    optional: true
fragments:
  - id: product.gallery
    template: fragments/product/gallery
    bind: {product: "$steps.product"}
    cache: {ttl: 60s, vary: [locale], tag: "product:$steps.product.id"}
  - id: product.price
    template: fragments/product/price
    bind: {product: "$steps.product", viewer: "$steps.viewer"}
    cache: {ttl: 10s, tag: "product:$steps.product.id"}
  - id: product.origin_tree
    template: fragments/product/origin_tree
    bind: {product: "$steps.product", seller: "$steps.seller"}
    optional: true
  - id: product.care
    template: fragments/product/care
    bind: {care: "$steps.care"}
    optional: true
  - id: product.similar
    template: fragments/product/similar
    bind: {items: "$steps.similar"}
    optional: true
page:
  template: pages/product
  slots: [product.gallery, product.price, product.origin_tree, product.care, product.similar]
```

### 6.2. Правила манифеста

- `steps` — DAG: аргумент `$steps.X.field` создаёт ребро `X → текущий`. Цикл = ошибка публикации.
- `kind`: `sql | sql_batch | cache_get | search | blob | rpc | const | computed`.
- `optional: true` — падение шага не валит страницу, фрагмент уходит в `degraded`.
- `require: not_null` — пустой результат даёт 404 на маршрут.
- `budget_ms` — бюджет маршрута, режет `Meta.Deadline`.
- Публикация проходит валидатор: схема, существование запросов/индексов, проекции полей, права, отсутствие циклов.

### 6.3. Контракт фрагмента

```yaml
id: product.price
version: 3
template: fragments/product/price
data_schema: schemas/fragments/product.price.v3.json   # JSON Schema (draft 2020-12)
cache: {ttl: 10s, vary: [locale, currency]}
updates: [signal]      # какие типы потоковых событий блок принимает
```

Рендер падает в тесте, если `bind` отдаёт данные, не проходящие `data_schema`. Это защита от
расхождений «шаблон ждёт одно — запрос вернул другое».

---

## 7. Внутренние порты (интерфейсы между компонентами)

```go
// gateway → composer
type RouteExecutor interface {
	Execute(ctx context.Context, msg message.Message) (*view.ViewDoc, error)
}

// composer → renderer
type Renderer interface {
	RenderPage(ctx context.Context, doc *view.ViewDoc) (html []byte, err error)
	RenderFragments(ctx context.Context, doc *view.ViewDoc, ids []string) (map[string][]byte, error)
}

// hub публикует в топики; gateway подписан для SSE
type EventBus interface {
	Publish(ctx context.Context, topic string, ev view.Event) (seq int64, err error)
	Subscribe(ctx context.Context, pattern string, from int64) (<-chan view.Event, error)
}
```

---

## 8. Ошибки

Формат: RFC 9457 Problem Details (`application/problem+json`).

```json
{"type":"https://phyto.dev/errors/forbidden","title":"Forbidden","status":403,
 "code":"auction.bid.own_lot","detail":"Нельзя делать ставку на свой лот",
 "request_id":"01J...","trace_id":"4bf9..."}
```

| code | HTTP | Смысл | Клиент делает |
|---|---|---|---|
| `validation.failed` | 400 | невалидные параметры (есть `errors[]`) | показывает у полей |
| `auth.unauthenticated` | 401 | нет/просрочен ключ | редирект на вход / refresh |
| `auth.ticket_invalid` | 401 | ticket просрочен/использован | новый ticket через HTTP |
| `auth.forbidden` | 403 | нет scope | скрывает действие |
| `resource.not_found` | 404 | нет объекта | страница 404 |
| `resource.gone` | 410 | лот снят/аукцион удалён | плейсхолдер |
| `conflict.version` | 409 | устаревшая версия (ETag/If-Match) | перезагрузка блока |
| `conflict.bid_too_low` | 409 | ставка ниже текущей + шаг | обновляет стакан |
| `rate.limited` | 429 | лимит (+ `Retry-After`) | backoff |
| `idempotency.replay` | 200/409 | повтор с тем же ключом | берёт прошлый ответ |
| `upstream.timeout` | 504 | внешний сервис | деградация |
| `internal` | 500 | прочее | «попробуйте позже» |

**Деградация:** если фрагмент не собрался — страница всё равно 200, `Fragment.Status = degraded`,
заголовок `X-PL-Degraded: product.similar,product.care`. Ошибка в обязательном фрагменте
(`optional: false`) → 503 с сохранённым `request_id`, клиент показывает retry-состояние.

---

## 9. Потоковые события (WS/SSE)

```go
type Event struct {
	Type  string          `json:"t"`               // fragment.replace|state.patch|signal|ack|error|ping
	Topic string          `json:"topic,omitempty"`
	Seq   int64           `json:"seq"`
	ID    string          `json:"id,omitempty"`    // fragment id / ключ сигнала
	HTML  string          `json:"html,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
	ETag  string          `json:"etag,omitempty"`
	Mode  string          `json:"mode,omitempty"`  // outer|inner|append|prepend
}
```

| Type | Назначение | Клиент |
|---|---|---|
| `fragment.replace` | новый HTML блока | морфит блок по `id` |
| `state.patch` | JSON Merge Patch в `ViewDoc.State` | обновляет счётчики/баланс |
| `signal` | мелкое значение (таймер, текущая цена, online) | обновляет `[data-pl-signal]` без HTML |
| `ack` | подтверждение команды клиента (`{t:"ack", id:<opId>, data:{...}}`) | снимает оптимистичное состояние |
| `error` | ошибка стрима/подписки | backoff / переподписка |
| `ping` | keepalive каждые 15 с | молча игнорирует |

Топики: `auction:<id>`, `chat:room:<id>`, `user:<id>`, `shop:<id>`, `catalog:feed`.
Топик `user:<id>` — только для владельца (проверка в `hub`).

---

## 10. Имена: заголовки, префиксы, метрики

**HTTP-заголовки:** `X-Request-Id`, `X-Trace-Id`, `X-PL-Client-Id`, `X-PL-Mode: fragment`,
`X-PL-Fragments`, `X-PL-Route`, `X-PL-Route-Version`, `X-PL-Degraded`, `X-PL-Seq`,
`X-PL-Ticket`, `X-PL-Key-Id`, `X-PL-Signature`, `Idempotency-Key`, `Last-Event-ID`.

**Cookie:** `pl_sid` (HttpOnly, Secure, SameSite=Lax, 30 дней), `pl_rt` (refresh, Path=/auth, 90 дней),
`pl_cid` (анонимный id, 1 год), `pl_csrf` (double-submit, читаемый).

**HTTP API:** `/v1/*` для команд и JSON; страницы — без префикса; `/ws/v1`, `/sse/v1`.
Команды: `POST /v1/auctions/{id}/bids`, `POST /v1/cart/items`, `POST /v1/registry/applications`.

**Redis:** `pl:sess:<sid>`, `pl:tkt:<id>`, `pl:rl:<scope>:<key>`, `pl:cache:step:<hash>`,
`pl:cache:frag:<id>:<hash>`, `pl:auction:<id>:state`, `pl:seq:<topic>`, `pl:presence:<topic>`.

**NATS:** `ev.<domain>.<entity>.<action>` (`ev.auction.9f1c.bid`, `ev.chat.room.9f1c.msg`),
`ev.cache.purge`, `rpc.render`, `ev.outbox`.

**Метрики:** `pl_http_request_duration_seconds{route,status}`, `pl_route_step_duration_seconds{route,step}`,
`pl_fragment_render_duration_seconds{fragment}`, `pl_fragment_degraded_total{fragment}`,
`pl_ws_connections{transport}`, `pl_stream_seq_lag{topic}`, `pl_cache_hits_total{layer}`,
`pl_bid_command_duration_seconds`, `pl_outbox_lag_seconds`.

**ETag:** фрагмент — `W/"<id>-<ver>-<sha256(data|locale|currency)[:16]>"`;
страница — `W/"<route>-<ver>-<sha256(отсортированные etag фрагментов)[:16]>"`.

---

## 11. Кэш-матрица (кратко; детали — в главах 02/06)

| Слой | Что | Ключ | Инвалидация |
|---|---|---|---|
| CDN/HTTP | HTML анонимных страниц | URL + `Vary: Accept-Language, X-PL-Mode` | `Cache-Tag` purge |
| HTML фрагментов | `renderer` | `frag:<id>:<hash(data,locale,currency)>` | tag из манифеста (`product:<id>`) |
| Данные шага | `composer` | `step:<query>:<hash(args,roles)>` | row version / tag |
| Hot-state | Redis | `auction:<id>:state` | запись при ставке (Lua CAS) |

---

## 12. Что изменять нельзя без версии контракта

- Поля `Message`, `ViewDoc`, `Fragment`, `Event` (только добавление, не переименование).
- Семантику `seq` внутри топика (строго монотонно, без дыр в доставленном окне).
- Коды ошибок из §8.
- Формат ETag и правило «клиент не считает бизнес-значения».

---

## 13. Правки после состязательного ревью (v1.1)

Раздел нормативный: при конфликте с текстом глав приоритет у него. Каждый пункт закрывает
конкретное расхождение из [08-review.md](./08-review.md).

**13.1. `seq` и `epoch`.** `seq` монотонен в топике, но **непрерывность номеров не гарантируется**:
номер выдаёт outbox-relay, откат транзакции оставляет неиспользованный номер. Клиент применяет событие
при `seq > last`; пропуск в доставке объявляется только явным `error` с кодом `stream.seq_gap`.
Смена поколения топика (failover Redis, рестарт relay) несёт `epoch`; при росте `epoch` клиент
перезагружает блок (snapshot), а не догоняет поток.

```go
type Event struct {
	Type  string          `json:"t"`               // fragment.replace|state.patch|signal|ack|error|ping
	Topic string          `json:"topic,omitempty"`
	Seq   int64           `json:"seq"`
	Epoch int64           `json:"epoch,omitempty"` // поколение топика
	Ts    int64           `json:"ts"`              // unix ms, для SLI лага
	ID    string          `json:"id,omitempty"`
	HTML  string          `json:"html,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
	ETag  string          `json:"etag,omitempty"`
	Mode  string          `json:"mode,omitempty"`
}
```

**13.2. Владелец рендера.** `composer` возвращает `ViewDoc` **без HTML**; `RenderPage`/`RenderFragments`
вызывает `gateway` (§7). Никаких «composer отрендерил фрагменты»: composer не знает HTML, renderer не знает SQL.

**13.3. Ticket.** Один путь выпуска: `POST /v1/realtime/ticket` → `{ticket, expires_in: 30}`. Ticket —
opaque id в Redis (`pl:tkt:<id>`, `GETDEL`, single-use). Доставка: WS — сабпротокол
`Sec-WebSocket-Protocol: pl.v1, pl.ticket.<t>`; SSE — `?ticket=` (единственный фолбэк, параметр
редактируется в логах). Ed25519 для ticket не используется (только для подписи событий/вебхуков).

**13.4. Идемпотентность.** Единственная DDL `idempotency_keys` — в [02-data-routes.md](./02-data-routes.md) §2
(владелец — платформа). Повтор с тем же ключом и другим телом: `422 idempotency.key_reused` (внесён в §8).
TTL — свойство таблицы, не манифеста маршрута.

**13.5. Заголовки и media-type.** Единое имя CSRF-заголовка — `X-CSRF-Token` (`X-PL-CSRF` — устаревший
алиас). Добавлены `X-PL-Timestamp`, `X-PL-Nonce`. Media-type фрагментного ответа — один:
`application/vnd.pl.viewdoc+json`.

**13.6. Кэш.** `Vary: Cookie` запрещён (убивает CDN и бесполезен при `no-store`). Анонимная страница:
`public, s-maxage=30, stale-while-revalidate=120`, `Vary: Accept-Language, X-PL-Mode`. Персональная:
`private, no-store`. Одно значение `s-maxage` во всех главах.

**13.7. Потолок бюджета.** `budget_ms ≤ 400` для любого маршрута; валидатор публикации отклоняет больше.

**13.8. Обязательный фрагмент.** Если `optional: false` и шаг упал — `503 upstream.timeout` **до** первого
байта тела; стриминг не начинается, пока не определён статус обязательных фрагментов. «200 + degraded» —
только для `optional: true`.

**13.9. ETag.** `sha256(canonical_json(data) || 0x00 || locale || 0x00 || currency)`, слабый тег `W/`.
Разделитель обязателен: без него две реализации дают разные теги.

**13.10. `ViewDoc.State`.** Ключи: `ui.locale`, `ui.currency` (кладёт `composer` из `Message.Hints`),
`cart.count`, `wallet.balance`. Presentation-поля (`amount_text`, `now_ms`, `min_amount_text`) формирует
`composer` computed-шагом; `renderer` деньги не форматирует и локаль не резолвит.

**13.11. CSP и Trusted Types.** Клиент обязан работать под `require-trusted-types-for 'script'; trusted-types pl`:
вставка HTML идёт только через `trustedTypes.createPolicy('pl', ...)`. Без этого морф блоков запрещён.

**13.12. Чат.** `chat_messages` не хранит `seq`: порядок истории — `(created_at, msg_id)`; `seq` живёт
только в транспорте топика.
