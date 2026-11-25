# Практична №3 — Оптимізація Cassandra: Simple Wide Row vs Hourly Bucketing

**Мета:** продемонструвати зростання партиції в наївній моделі `PRIMARY KEY(device_id,recorded_at)`, показати переваги `PRIMARY KEY((device_id,bucket_hour),recorded_at)` і коректно виміряти час читання, кількість партицій та обсяг даних. Джерело завдання: `Практика 3 Оптимізація Cassandra.docx`.

## Етап 1. Підготувати дві схеми з однаковими колонками

Переконайтеся, що Cassandra зі №2 доступна:

```powershell
docker compose up -d cassandra
docker compose exec -T cassandra nodetool status
docker compose exec -T cassandra cqlsh -f /workspace/practical3_optimization/schema.cql
docker compose exec -T cassandra cqlsh -e "DESCRIBE KEYSPACE demo_practice;"
```

У новому `demo_practice` створено дві таблиці:

| Схема | Partition key | Clustering key | Очікувана кількість партицій за 30 діб, 1 пристрій |
|---|---|---|---|
| `telemetry_bad` | `device_id` | `recorded_at` | 1 |
| `telemetry_hourly` | `(device_id,bucket_hour)` | `recorded_at` | Приблизно 720 (30 × 24) |

За одного показання за хвилину за 30 днів буде `30 × 24 × 60 = 43 200` показань для кожної схеми. У `bad` це одна партиція на 43 200 рядків, у `hourly` — приблизно 60 рядків на партицію. **Це передбачувані математичні кількості**, а не вже виміряні байти Cassandra.

В оригінальному матеріалі є непослідовна назва `telemetry_hourly1` під час CREATE та `telemetry_hourly` під час INSERT. У цій реалізації усюди використано **`telemetry_hourly`**.

## Етап 2. Генерувати однаковий набір вимірювань

Код: `practical3_optimization/generate.py`, чиста генерація значень — `practical3_optimization/core.py`. Спочатку проведіть невеликий тест:

```powershell
docker compose run --rm lab python -m practical3_optimization.generate --days 1
```

Для демонстрації більшої партиції, як у матеріалах, виконайте **основний** запуск:

```powershell
docker compose run --rm lab python -m practical3_optimization.generate --days 30 --step-minutes 1
```

Генератор створює один і той самий детермінований набір вимірювань (час, потужність, напруга) й записує кожне значення до обох таблиць. Такий A/B підхід кращий за два незалежні випадкові генератори: обидві моделі порівнюються на однакових рядках. Запис ведеться асинхронними запитами Cassandra driver із обмеженням кількості незавершених операцій (`--inflight 80`); це зменшує час завантаження без нескінченного накопичення futures.

Якщо на ПК не вистачає ОЗП або Cassandra дає `OverloadedException`, зменшіть `--days 3 --inflight 20`, зафіксуйте менший сценарій у звіті й повторіть при можливості. Можна також запустити `--days 180` чи `300`, але це сотні тисяч вставок в одну широку партицію та потенційно суттєве навантаження — не рекомендується без достатніх ресурсів.

Після завершення має з'явитися `Completed: ... rows/table`, а також `results/p3_generation_metadata.json` із датою кінця генерації. Benchmark бере часовий інтервал саме з цього файла; повторний запуск не змістить інтервал тестування випадково на іншу годину.

## Етап 3. Перевірити кількість рядків та партицій

```powershell
docker compose exec -T cassandra cqlsh -e "SELECT COUNT(*) FROM demo_practice.telemetry_bad WHERE device_id='DEMO_SOLAR_001';"
docker compose exec -T cassandra nodetool flush demo_practice telemetry_bad
docker compose exec -T cassandra nodetool flush demo_practice telemetry_hourly
docker compose exec -T cassandra nodetool tablestats demo_practice.telemetry_bad
docker compose exec -T cassandra nodetool tablestats demo_practice.telemetry_hourly
```

Для порівняння збережіть повний `nodetool tablestats` у текстові файли:

```powershell
docker compose exec -T cassandra nodetool tablestats demo_practice.telemetry_bad | Out-File -FilePath results/p3_bad_tablestats.txt -Encoding utf8
docker compose exec -T cassandra nodetool tablestats demo_practice.telemetry_hourly | Out-File -FilePath results/p3_good_tablestats.txt -Encoding utf8
docker compose run --rm lab python -m practical3_optimization.analyze_partitions
```

У підсумковій таблиці дивіться: `Number of partitions (estimate)`, `Compacted partition mean/min/max bytes`, `SSTable count`, `Space used (live)`. Відомості про кількість партицій та фізичні розміри — **оцінки та статистика вузла**; вони можуть змінюватися після flush і compaction. Не плутайте `Space used (live)` (вся таблиця) з середнім розміром однієї партиції. Порівнюйте саме такі величини, а не загальний розмір файлів двох таблиць без контексту.

Скриншоти: два виводи `nodetool`, розрахунок `43 200` / `720` / `60`, таблиця порівняння партицій.

## Етап 4. Ручні запити та TRACING

Підключіться до Cassandra:

```powershell
docker compose exec cassandra cqlsh
```

Виконайте:

```sql
USE demo_practice;
TRACING ON;
SELECT recorded_at, power_output, voltage FROM telemetry_bad
WHERE device_id='DEMO_SOLAR_001'
LIMIT 60;
TRACING OFF;
```

Для погодинної схеми використайте **реальний** `bucket_hour` з вашої генерації. Його можна дізнатися з `results/p3_generation_metadata.json`: остання повна година — на одну годину раніше від `end_hour_utc`. Наприклад, якщо `end_hour_utc=2026-11-22T12:00:00+00:00`, то запит:

```sql
TRACING ON;
SELECT recorded_at, power_output, voltage FROM telemetry_hourly
WHERE device_id='DEMO_SOLAR_001'
AND bucket_hour='2026-11-22 11:00:00+0000'
LIMIT 60;
TRACING OFF;
```

Для **справедливого** порівняння `TRACING` вказуйте також однакові часові межі у схемі `bad`:

```sql
SELECT recorded_at,power_output,voltage FROM telemetry_bad
WHERE device_id='DEMO_SOLAR_001'
AND recorded_at >= '2026-11-22 11:00:00+0000'
AND recorded_at < '2026-11-22 12:00:00+0000'
LIMIT 60;
```

Замініть дати на фактичний інтервал з metadata. Зауважте, `TRACING ON` додає витрати на діагностику, тому для основного benchmark виконання запитів зі tracing вимкнено.

## Етап 5. Benchmark (avg, p50, p95, p99)

Код: `practical3_optimization/benchmark.py`. Запускає підготовлені CQL-запити по одному часовому інтервалу й пристрою; кожен запит читає до 60 рядків, тричі прогріває кеш і проводить 50 незалежних вимірювань `time.perf_counter()`:

```powershell
docker compose run --rm lab python -m practical3_optimization.benchmark --iterations 50
```

Вивід містить для `bad` і `hourly`: avg, p50, p95, p99, min/max. Файл `results/p3_benchmark.csv` містить **справжні заміри**. Показники з методичних матеріалів (наприклад, 5.77 мс і 1.82 мс) **не слід переносити до власного звіту як результати**: вони залежать від конкретного комп'ютера, розміру набору, кешу та стану Cassandra. Малий локальний тест іноді не показує істотної переваги hourly — це допустимий результат; архітектурна перевага проявляється при довготривалому накопиченні та масштабуванні.

Перцентилі: p50 — медіана; p95 — час, швидший за який виконується приблизно 95% запитів; p99 відображає довгі хвости. `avg` чутливий до піків. Benchmark виконує *bounded reads*, однак сервер усе одно має різні фізичні структури партицій — це і є досліджувана незалежна змінна.

## Етап 6. Оформлення порівняння

Підготуйте таблицю (без вигаданих чисел):

| Метрика | `telemetry_bad` | `telemetry_hourly` |
|---|---|---|
| Partition key | `(device_id)` | `(device_id,bucket_hour)` |
| Записів у одній типовій партиції | ~43 200 при 30 днях | ~60 при 1 записі/хв |
| Кількість партицій, estimate | з `nodetool` | з `nodetool` |
| Mean partition bytes | з `nodetool` | з `nodetool` |
| p95 / p99, ms | з `p3_benchmark.csv` | з `p3_benchmark.csv` |
| Умова вибірки | 1 повна година | та сама повна година |

**Пояснення:** без bucketing записи одного пристрою невпинно збільшують partition; з погодинним ключем кожна година утворює нову обмежену партицію. Це стабілізує розмір, спрощує роботи з часовими діапазонами й покращує масштабування. Натомість великий запит за багато днів повинен звертатися до кількох погодинних партицій — це свідомий trade-off.

Скриншоти: schema обох таблиць, генератор, flush, `tablestats`, TRACING обох схем, benchmark із перцентилями. Для звіту використайте `results/REPORT_03.md`.
