# Лабораторная работа №3

Вариант 16 реализован на `Python 3.10+` и `MongoDB`.

Проект моделирует распределенный анализ социальных графов:
- воркеры атомарно захватывают задачи из `graph_tasks`;
- для обхода подграфа используется `$graphLookup`;
- зависшие задачи повторно подхватываются по `processing_until`;
- результаты сохраняются в `graph_results`;
- метрики выполнения пишутся в `metrics.log`.

## Структура

```text
lab_distributed/
├── docker-compose.yml
├── .env
├── init_db/
│   └── init-mongo.js
├── worker_py/
│   ├── Dockerfile
│   ├── main.py
│   ├── requirements.txt
│   └── worker.py
└── tests/
    ├── test_worker.py
    └── test_integration.py
```

## Запуск через Docker

```powershell
cd Task3\lab_distributed
docker compose up -d --build --scale worker=3
docker compose ps
docker compose logs -f worker
```

Ожидаемый результат:
- `mongo` в статусе `healthy`;
- `worker` запущен в 3 экземплярах;
- в логах видно, что задачи распределяются между разными `WORKER_ID`.

Для более наглядной демонстрации распределения в `.env` задан `PROCESSING_DELAY_MS=250`, а инициализация создает `32` задачи вместо `8`.

## Полезные MongoDB-команды

Подключение:

```powershell
mongosh "mongodb://labuser:labpass@127.0.0.1:27017/labdb?authSource=admin"
```

Проверка статусов задач:

```javascript
use labdb
db.graph_tasks.aggregate([
  { $group: { _id: "$status", count: { $sum: 1 } } },
  { $sort: { _id: 1 } }
])
```

Проверка результатов:

```javascript
db.graph_results.find({}, { root_node: 1, worker_id: 1, elapsed_ms: 1 }).pretty()
```

Проверка распределения задач по воркерам:

```javascript
db.graph_results.aggregate([
  { $group: { _id: "$worker_id", count: { $sum: 1 } } },
  { $sort: { count: -1, _id: 1 } }
])
```

Поиск зависших задач:

```javascript
db.graph_tasks.find({
  status: "processing",
  processing_until: { $lt: new Date() }
})
```

## Локальный запуск без Docker

```powershell
cd Task3\lab_distributed\worker_py
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
set DB_URL=mongodb://labuser:labpass@127.0.0.1:27017/labdb?authSource=admin
set WORKER_ID=local-w1
python main.py
```

## Тесты

Unit-тест:

```powershell
cd Task3\lab_distributed
pytest tests\test_worker.py
```

Интеграционный тест требует доступного MongoDB:

```powershell
set MONGO_TEST_URL=mongodb://labuser:labpass@127.0.0.1:27017/labdb_test?authSource=admin
pytest tests\test_integration.py
```
