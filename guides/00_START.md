# Загальна інструкція: послідовне виконання чотирьох практичних

## 1. Призначення

У цьому репозиторії об’єднано чотири самостійні роботи в порядок, визначений методичками: 1) Kafka; 2) Cassandra; 3) оптимізація Cassandra; 4) Kafka + Bytewax + Cassandra. У кожній папці є виконувані файли, а в `guides/` — послідовні команди, очікувані результати, перевірки та перелік скриншотів. Не слід одночасно виконувати всі роботи: це ускладнить діагностику.

**Команди наведено для PowerShell у Windows 10/11 з Docker Desktop (WSL 2).** Працюйте в терміналі PowerShell у кореневій папці `energy_labs_zorenko` (самій папці, де знаходиться `docker-compose.yml`). `docker compose` виконується на хості; `python` запускається в контейнері `lab`, тому не потрібно встановлювати Bytewax, Cassandra чи Java безпосередньо у Windows.

## 2. Що треба зробити на власному комп’ютері

1. Установіть Docker Desktop: https://docs.docker.com/desktop/setup/install/windows-install/ . Увімкніть backend WSL2 та Linux containers.
2. Запустіть Docker Desktop; у PowerShell виконайте `docker version`, `docker compose version`, `docker info`. Якщо `docker` не розпізнається, перезапустіть термінал; якщо daemon недоступний, відкрийте Docker Desktop.
3. Рекомендовано мати 12–16 ГБ ОЗП, 4 ядра та 15+ ГБ вільного SSD. Для Docker із WSL2 виділіть достатньо ресурсів. На ПК з 8 ГБ зменшуйте навантаження практичної №3 (`--days 1`) і №4 (`--meters 5`).
4. Розпакуйте архів до звичайної папки без системних обмежень, наприклад `C:\Projects\energy_labs_zorenko`. У VS Code відкрийте саме цю папку (`File → Open Folder`).
5. Запустіть команди:

```powershell
cd C:\Projects\energy_labs_zorenko
docker compose config
docker compose pull zookeeper kafka cassandra
docker compose build lab
```

`docker compose config` має перевірити YAML без помилок. `pull` потребує інтернет-з'єднання. Перший `build` завантажує Python-залежності. Увесь навчальний стенд — локальний: брокер доступний з Windows за `localhost:9092`, Cassandra за `localhost:9042`, а всередині контейнерів — `kafka:29092` і `cassandra:9042`.

## 3. Загальні команди

```powershell
# Початковий запуск усіх серверних компонентів:
docker compose up -d zookeeper kafka cassandra
# Показати стан контейнерів:
docker compose ps
# Показати журнали Kafka:
docker compose logs --tail 100 kafka
# Зайти у CQLSH:
docker compose exec cassandra cqlsh
# Показати топіки:
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list
# Запустити тести незалежної від серверів логіки:
docker compose run --rm lab python -m pytest -q
# Без втрати даних зупинити сервери:
docker compose stop
# Підняти повторно:
docker compose up -d
```

Зупинка `docker compose stop` зберігає томи даних. **Не виконуйте `docker compose down -v`, доки не збережете результати:** ключ `-v` знищує локальні дані Kafka й Cassandra.

## 4. Принципи перевірки

- Повідомлення `Created topic...`, `SELECT ...`, вивід Producer/Consumer та `nodetool` — це результати вашого власного запуску. Їх треба зберігати реальними скриншотами.
- Цільові швидкості з методичних матеріалів не слід видавати за свої виміри. Локальний кластер з одним брокером/вузлом не є розподіленим відмовостійким стендом.
- Підсумки виконання, які потребують сервісів, стануть доступні після запуску на вашому ПК. Показники часу/продуктивності змінюються від обладнання та навантаження.
- На першому запуску Cassandra завантажується помітно довше, ніж Kafka. Якщо `cqlsh` повертає `Connection refused`, перевірте `docker compose ps` і `docker compose logs --tail 120 cassandra`, а потім повторіть команду після готовності вузла.

## 5. Порядок переходу до наступної практичної

1. Виконайте **усі пункти** `guides/01_KAFKA.md`, зробіть скриншоти і заповніть звіт №1.
2. Не видаляючи Kafka/Cassandra томи, виконайте `guides/02_CASSANDRA.md` та звіт №2.
3. На тій самій Cassandra створіть `demo_practice` і пройдіть `guides/03_OPTIMIZATION.md` та звіт №3.
4. Використайте працюючі сервіси для `guides/04_STREAMING.md` та звіту №4.

## 6. Команди лабораторії в документації

У прикладах завжди вказано `docker compose run --rm lab python -m <пакет.модуль>`. Це забезпечує однакові бібліотеки й адреси сервісів. Для виконання скриптів без контейнера потрібен Linux/WSL2 та окремо встановлені залежності з `requirements.txt`; потрібно виставити `KAFKA_BOOTSTRAP=localhost:9092`, `CASSANDRA_HOST=localhost`. Не змішуйте різні режими в межах одного запуску.
