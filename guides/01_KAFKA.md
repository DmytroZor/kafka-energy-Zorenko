# Практична №1 — Основи Apache Kafka в розподілених базах даних

**Тема:** збирання та передавання телеметрії електростанцій. **Мета:** налаштувати Kafka 3.7.x із ZooKeeper; створити топіки, Producer і Consumer; вивчити partitions, replication, offsets, consumer groups; провести моніторинг і тест пропускної здатності.

## Етап 1. Робоче середовище

Запустіть Docker Desktop, відкрийте VS Code в папці `energy_labs_zorenko`, відкрийте PowerShell (`Terminal → New Terminal`). Після одноразового `docker compose build lab` виконайте:

```powershell
docker compose up -d zookeeper kafka
docker compose ps
docker compose logs --tail 70 kafka
docker compose exec -T kafka kafka-topics --version
docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server kafka:29092
```

Ознаки успіху: `zookeeper` і `kafka` перебувають у стані `Up`, `kafka-topics --version` повідомляє версію, команда API versions виводить відомості про брокер. У документі використовується Kafka 3.7.1; образ `cp-kafka:7.7.1` належить лінійці Confluent 7.7 на основі Kafka 3.7. Фактичну версію вашого контейнера збережіть зі виводу команди, не підміняйте цифру прикладом.

Де ZooKeeper? Це окремий контейнер `zookeeper`, який координує кластер. Зберігайте скриншот `docker compose ps` та журналу запуску.

## Етап 2. Створення і дослідження топіків

Вимога методички: **додати власне прізвище до назви основного топіка**. Тому використано `power-station-data-zorenko` і папку `energy_labs_zorenko`.

```powershell
docker compose run --rm lab python create_topics.py
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic power-station-data-zorenko
docker compose exec -T kafka kafka-configs --bootstrap-server kafka:29092 --entity-type topics --entity-name power-station-data-zorenko --describe
```

Основний топік має `PartitionCount: 3`, `ReplicationFactor: 1`. В одному брокері реплікація 3 неможлива; replication=1 відповідає одно-вузловій навчальній конфігурації. Додаткові топіки: `power-telemetry` (3), `grid-alerts` (2), `energy-consumption` (4), `weather-data` (2). Скрипт також створює топіки для майбутньої роботи №4 — це нормально. Офсети є окремими **для кожної партиції**.

Скриншоти: список топіків і `--describe` вашого основного топіка.

## Етап 3. Консольні Producer та Consumer

**Термінал A — Producer.** Відкрийте новий PowerShell, виконайте:

```powershell
docker compose exec kafka kafka-console-producer --bootstrap-server kafka:29092 --topic power-station-data-zorenko
```

Уведіть вручну **окремими рядками**:

```text
Київська ТЕС: 1200 МВт
Дніпровська ГЕС: 800 МВт
Сонячна станція: 200 МВт
```

Не закривайте Producer, доки не перевірите споживання. **Термінал B — Consumer**:

```powershell
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic power-station-data-zorenko --from-beginning --max-messages 3
```

Мають з'явитися три введені рядки. Завершіть консольний Producer натисканням Ctrl+C. Зробіть скриншот обох консолей з назвами топіка та введеними повідомленнями. Якщо цей топік уже містить дані з попередніх запусків, `--from-beginning` відобразить старі повідомлення; для чистого тесту почніть із нової назви тестового топіка або фіксуйте offset.

## Етап 4. Python Producer

Код: `practical1_kafka/producer.py`. Програма генерує JSON за трьома станціями, надсилає ключ `station_id` (показники одного джерела потрапляють до тієї самої партиції), а також `power_output_mw`, `voltage_kv`, `frequency_hz`, час UTC та `sequence`.

```powershell
docker compose run --rm lab python -m practical1_kafka.producer --count 150 --interval 0.03
```

Очікування: вивід кількох JSON-подій та останній рядок `Producer finished: 150 records...`. Слово `finished` підтверджує виклик `producer.flush()`. Якщо з'являється `NoBrokersAvailable`, перевірте, чи запущений Kafka (`docker compose ps`) та правильний порт `kafka:29092` усередині контейнера.

## Етап 5. Python Consumer

Код: `practical1_kafka/consumer.py`. Обробляє JSON-повідомлення, друкує `partition`, `offset`, потужність і частоту, визначає відхилення частоти та робить ручний commit позиції після оброблення.

```powershell
docker compose run --rm lab python -m practical1_kafka.consumer --max-messages 20 --group energy-monitor-zorenko-demo1
```

Очікується 20 повідомлень, якщо вони вже були відправлені на етапі 4. Запустіть consumer із **новою групою** (`energy-monitor-zorenko-demo2`), щоб прочитати старі події з початку ще раз. При повторному запуску **тієї самої групи** consumer продовжить після останнього зафіксованого offset. Це наочна демонстрація offset management.

Після завершення запитайте стан групи:

```powershell
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --list
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group energy-monitor-zorenko-demo1
```

Дивіться поля `CURRENT-OFFSET`, `LOG-END-OFFSET`, `LAG`. `LAG` — різниця між кінцевим і зафіксованим offset, а не число повідомлень у самому топіку. Для вже завершеного consumer можлива відсутність стовпців активного CLIENT-ID — це нормально. Збережіть скриншот Producer, Consumer та групи.

## Етап 6. Моніторинг і benchmark Kafka

Команди виконуйте у PowerShell із кореня проєкту:

```powershell
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --describe
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --list
docker compose exec -T kafka kafka-log-dirs --bootstrap-server kafka:29092 --describe
docker compose logs --tail 80 kafka
```

**Benchmark Producer:**

```powershell
docker compose exec -T kafka kafka-producer-perf-test --topic power-station-data-zorenko --num-records 10000 --record-size 1024 --throughput 1000 --producer-props bootstrap.servers=kafka:29092 compression.type=gzip
```

Вивід містить `records/sec`, `MB/sec`, середню, p50, p95 та p99 latency. Аргумент `--throughput 1000` **обмежує** генератор до ~1000 подій/с, тому результат не визначає максимальну продуктивність брокера. Збережіть фактичні цифри.

**Benchmark Consumer:**

```powershell
docker compose exec -T kafka kafka-consumer-perf-test --bootstrap-server kafka:29092 --topic power-station-data-zorenko --messages 10000
```

Вивід містить `data.consumed.in.nMsg`, `nMsg.sec`, `MB.sec`. Producer benchmark має бути виконаний раніше, щоб у топіку були дані для читання. Якщо консюмер прочитав більше ніж 10 000 повідомлень, врахуйте початкові тестові дані та пакетну обробку.

**Міні-дослід:** якщо потрібно, зменшіть швидкість генератора або змініть число consumers у спільній групі, спостерігаючи, як розподіляються три партиції. Один consumer у групі може обслуговувати кілька партицій; четвертий consumer у групі з трьома партиціями залишиться без призначеної партиції.

## Етап 7. Завершення й звіт

```powershell
docker compose stop kafka zookeeper
# Для наступної роботи знову: docker compose up -d zookeeper kafka
```

Не видаляйте томи та топіки, поки не зробили скриншоти. Звіт для №1 за методичкою: титульний аркуш, тема і мета, скриншоти консольного Producer/Consumer і Python Producer/Consumer (із власним прізвищем у папці/топіку), посилання на листинг GitHub, що треба зберігати 3 роки. Є редаговані шаблони `results/REPORT_01.md` і `results/REPORT_01.docx`. Посилання на GitHub впишіть після справжньої публікації репозиторію, не вигадуйте його.

### Публікація проєкту на особистому GitHub (виконати власноруч)

1. Встановіть Git для Windows: https://git-scm.com/downloads/win . Перезапустіть PowerShell і перевірте `git --version`.
2. Зайдіть у свій акаунт https://github.com/ . Натисніть **New repository**, назвіть `energy_labs_zorenko` (за потреби замініть прізвище), виберіть Public або Private відповідно до правил курсу. **Не** створюйте README чи `.gitignore` на GitHub, адже ці файли вже є в проєкті.
3. У PowerShell у корені розпакованого проєкту виконайте:

```powershell
git init
git add .
git commit -m "Implement Kafka and Cassandra practical works 1-4"
git branch -M main
git remote add origin https://github.com/ВАШ_ЛОГІН/energy_labs_zorenko.git
git push -u origin main
```

Замініть `ВАШ_ЛОГІН` фактичним іменем акаунту. GitHub може попросити виконати вхід через браузер; звичайний пароль у `git push` не використовується. Якщо Git повідомляє про невстановлені `user.name`/`user.email`, задайте власні значення командами `git config --global user.name "Ваше ім'я"` і `git config --global user.email "email, прив'язаний до вашого GitHub"`, потім повторіть `git commit`.

4. Відкрийте репозиторій у браузері, переконайтеся, що видно `practical1_kafka/producer.py`, `practical1_kafka/consumer.py` і всі подальші папки. Скопіюйте **реальне** URL зі строки адреси у Word-звіт №1. За вимогою матеріалу підтримуйте доступність репозиторію протягом трьох років.
5. Знімки екрана у Windows зручно робити `Win + Shift + S`. Збережіть кожен у локальній папці `screenshots/` з описовою назвою (`01_kafka_topics.png`, `02_python_producer.png`, `03_python_consumer.png` тощо), вставте у Word-звіт під підписами рисунків. `.gitignore` не забороняє `screenshots/`, тому перед публікацією перевірте, чи немає в них токенів, паролів, приватних повідомлень або особистих даних.


## Відповіді на контрольні питання

**Topic vs partition.** Topic — логічна категорія повідомлень; partition — упорядкований append-only журнал частини цього топіка. Порядок гарантується в межах однієї партиції, а не всього топіка.

**Replication.** Кожна партиція має лідера й визначену кількість реплік на різних брокерах; виробник пише лідеру. В одно-брокерному стенді replication factor = 1 — відмовостійкості при втраті цього вузла немає.

**Kafka для IoT.** Витримує масовий прийом подій, буферизує дані, незалежно масштабує producers і consumers, підтримує повторне відтворення подій та сегментацію за ключами пристроїв.

**Offset management.** Consumer group зберігає позицію для кожної партиції; можна виконувати автоматичний або ручний commit. У нашому прикладі commit виконується після успішного оброблення події. Нова група з `earliest` починає з найстарішого наявного offset.

**Schema Registry.** Дає централізовану перевірку формату подій (Avro, Protobuf, JSON Schema), правила сумісності версій, зменшує ризик поломки Consumer при зміні схеми лічильника. У практичній №1 показано JSON без Schema Registry, тому не слід писати у звіті, що Registry було встановлено.
