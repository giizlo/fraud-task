# Анализ массовых регистраций юридических лиц: Fraud-Audit исследование

## 1. Бизнес-контекст и гипотеза

### Контекст
В условиях санкционного давления (2022-2024 гг.) наблюдается резкий рост регистрации юридических лиц. Ключевой риск-индикатор — массовая регистрация компаний по одним и тем же адресам (координатам) в критические периоды. Такие паттерны характерны для:
- Создания подставных структур для обхода санкций
- Фиктивных компаний для мошеннических схем
- "Прослоек" в цепочках поставщиков

### Гипотеза
**Компании, зарегистрированные в массовых кластерах (>17 фирм на одну координату) в периоды пиков санкционной активности, имеют повышенный риск быть фиктивными или подставными структурами.**

**Значимость для аудита и fraud-аналитики:**
- Раннее выявление потенциальных shell-компаний
- Приоритизация клиентов для углубленной проверки (EDD)
- Обнаружение географических аномалий в клиентской базе

---

## 2. Источники данных

| Источник | Тип | Описание | Ограничения |
|----------|-----|----------|-------------|
| [RFSD](https://tochno.st/datasets/rfsd) | Реестр (CSV) | Российский реестр юридических лиц, 2020-2024 гг. ~7.5M записей | Только действующие/ликвидированные компании РФ; нет данных о бенефициарах |
| [RuNSD ISIN](https://www.opensanctions.org/datasets/ru_nsd_isin/) | Санкционный список (JSON) | ISIN-эмитенты с признаками санкций | Только эмитенты ценных бумаг; неполное покрытие всех санкционных режимов |

**Типология источников:**
- RFSD — структурированные данные (реестровый тип)
- RuNSD — полуструктурированные данные (JSON с вложенными свойствами)

---


## 3. Результаты анализа

### Ключевые метрики
| Показатель | Значение |
|------------|----------|
| Всего кластеров координат | 46,922 |
| Аномальных кластеров (>17 фирм) | 2,068 (4.4%) |
| Максимум фирм на адрес | 1,575 (Краснодар) |
| ТОП-10 кластеров | 6,800 фирм (80% аномальной массы) |

### Географическая концентрация рисков
**"Горячие" регионы:** Краснодарский край, Свердловская область, Республика Бурятия, Саратовская область

### Аналитический модуль: Аномальный числовой анализ
**Метод:** IQR-выбросы по размеру кластера (Q3 + 1.5 × IQR)  
**Порог аномалии:** >17 фирм на координату  
**Вывод:** 4.4% географических координат являются системными точками массовой регистрации и требуют приоритетной проверки.

---

## 4. Использование LLM (концептуально)

**Применение LLM как вспомогательного инструмента:**

1. **Генерация пояснений к кластерам**
   - Вход: регион + размер кластера + распределение ОКВЭД
   - Выход: текстовое описание риск-профиля кластера

2. **Классификация ОКВЭД по уровню риска**
   - Автоматическая категоризация отраслей: high/medium/low risk
   - Примеры high-risk: консалтинг, оптовая торговля, строительство

3. **Контекстуализация временных пиков**
   - Связь пиков регистрации с событиями санкционной политики
   - Объяснение аномалий через внешние факторы

**Ограничения:**
- Галлюцинации при интерпретации — требуется валидация аналитиком
- Контроль качества — человек в контуре принятия решений
- Стоимость API для массовой обработки

---

## 5. Инструкция по запуску

### 5.1 Предварительные требования
- Python 3.9+
- PostgreSQL 13+ (или SQLite для тестирования)
- ~8GB свободного места на диске

### 5.2 Установка зависимостей
```bash
pip install -r requirements.txt
```

### 5.3 Запуск Python-конвейера

**ВАЖНО:** Перед запуском основного конвейера необходимо скачать исходные данные RFSD. Скрипт автоматически проверит наличие файлов и скачает только отсутствующие. Это связано с ограничением размера загружаемых в  Git файлов в 100 Мб - вы не можете установить репозиторий вместе с датасетами, их там попросту нет.

```bash
# Шаг 0: Скачивание исходных данных RFSD (2020-2024)
python scripts/download_rfsd_data.py
```

**Пошаговый запуск (рекомендуется для отладки):**

```bash
# Шаг 1: Предобработка RFSD данных
python scripts/rfsd_setting.py

# Шаг 2: EDA и кластеризация с анализом санкций
python scripts/rfsd_nsd_eda.py

# Шаг 3: Анализ аномалий и визуализация
python scripts/cluster_analysing.py

# Шаг 4: Дополнительные визуализации (опционально)
python scripts/advanced_visualization.py
```

**ВАЖНО:** На каждом этапе дождитесь полного завершения скрипта и появления сообщения о успешном выполнении перед запуском следующего. Не прерывайте выполнение скриптов.

**Автоматический запуск всех этапов (одной командой):**

Создайте файл `run_pipeline.sh` и выполните его:

```bash
#!/bin/bash
set -e

echo "=========================================="
echo "FRAUD-AUDIT ANALYSIS PIPELINE"
echo "=========================================="

echo "[1/5] Downloading RFSD data..."
python scripts/download_rfsd_data.py

echo ""
echo "[2/5] Preprocessing RFSD data..."
python scripts/rfsd_setting.py

echo ""
echo "[3/5] EDA and clustering..."
python scripts/rfsd_nsd_eda.py

echo ""
echo "[4/5] Anomaly analysis..."
python scripts/cluster_analysing.py

echo ""
echo "[5/5] Additional visualizations..."
python scripts/advanced_visualization.py

echo ""
echo "=========================================="
echo "PIPELINE COMPLETED SUCCESSFULLY"
echo "=========================================="
echo ""
echo "Results available in:"
echo "  - data/ (processed data)"
echo "  - results/ (visualizations and CSV)"
```

Или выполните последовательно в PowerShell (Windows):

```powershell
python scripts/download_rfsd_data.py; 
python scripts/rfsd_setting.py; 
python scripts/rfsd_nsd_eda.py; 
python scripts/cluster_analysing.py; 
python scripts/advanced_visualization.py
```

**Результаты выполнения:**
- `data/rfsd_2020_2024.parquet` — очищенный реестр
- `data/companies_for_sql.csv` — данные компаний для БД
- `data/clusters_for_sql.csv` — кластеры для БД
- `results/top_mass_addresses.csv` — ТОП-10 массовых адресов
- `results/anomalous_clusters.csv` — 50 аномальных кластеров
- `results/cluster_distribution.png` — гистограмма распределения

### 5.4 Работа с SQL (PostgreSQL)

#### Вариант A: Полная установка PostgreSQL

**Шаг 1: Установка PostgreSQL**
```bash
# Windows (через winget)
winget install PostgreSQL.PostgreSQL

# Или скачайте установщик с https://www.postgresql.org/download/
```

**Шаг 2: Создание базы данных и загрузка схемы**
```bash
# Подключение к PostgreSQL (замените username на вашего пользователя)
psql -U postgres -f sql/schema.sql

# Или пошагово:
psql -U postgres -c "CREATE DATABASE sanctions_analysis;"
psql -U postgres -d sanctions_analysis -f sql/schema.sql
```

**Шаг 3: Загрузка данных**
```bash
# Важно: выполняйте из корневой директории проекта
psql -U postgres -d sanctions_analysis -f sql/load_data.sql
```

**Шаг 4: Выполнение аудиторских запросов**
```bash
psql -U postgres -d sanctions_analysis -f sql/auditor_queries.sql
```

#### Вариант B: Docker (рекомендуется)
```bash
# Запуск PostgreSQL в контейнере
docker run -d \
  --name fraud-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=sanctions_analysis \
  -p 5432:5432 \
  -v "$(pwd)/data:/data" \
  postgres:15

# Подключение и выполнение скриптов
docker exec -i fraud-postgres psql -U postgres -d sanctions_analysis < sql/schema.sql
docker exec -i fraud-postgres psql -U postgres -d sanctions_analysis < sql/load_data.sql
docker exec -i fraud-postgres psql -U postgres -d sanctions_analysis < sql/auditor_queries.sql
```

#### Вариант C: SQLite (для быстрого тестирования)
```bash
# SQLite не требует отдельной установки сервера
python -c "
import sqlite3
import pandas as pd

conn = sqlite3.connect('sanctions_analysis.db')
df_companies = pd.read_csv('data/companies_for_sql.csv')
df_clusters = pd.read_csv('data/clusters_for_sql.csv')
df_companies.to_sql('companies', conn, if_exists='replace', index=False)
df_clusters.to_sql('clusters', conn, if_exists='replace', index=False)
print('SQLite database created successfully')
"
```

### 5.5 Примеры SQL-запросов для аналитиков

**Запрос 1: Кластеры с наибольшей долей high-risk компаний**
```sql
SELECT region, oktmo, companies_count, high_risk_companies,
       round(high_risk_companies::float/companies_count*100, 1) AS risk_pct
FROM clusters 
WHERE high_risk_companies > 5
ORDER BY risk_pct DESC 
LIMIT 20;
```

**Запрос 2: High-risk компании для детальной проверки**
```sql
SELECT inn, region, okved, cluster_size
FROM companies 
WHERE high_risk = true
ORDER BY cluster_size DESC 
LIMIT 100;
```

**Запрос 3: Распределение рисков по регионам**
```sql
SELECT region, count(*) as suspicious,
       round(count(*)::float/sum(count(*)) over ()*100, 2) as pct
FROM companies 
WHERE high_risk = true
GROUP BY region
HAVING count(*) > 10
ORDER BY suspicious DESC;
```

---

## 7. Ограничения и дальнейшее развитие

### Текущие ограничения
1. **Данные:** Нет информации о бенефициарах и связанных лицах
2. **Санкции:** Только ISIN-эмитенты, неполное покрытие
3. **Геокодирование:** Округление координат может давать погрешности
4. **Временной лаг:** Данные RFSD с задержкой обновления

### Возможные улучшения
1. **Graph Analysis:** Построение сетей связей через ОКВЭД, регионы, даты регистрации
2. **Text Mining:** Анализ наименований компаний на шаблонность
3. **ML-модель:** Классификация риска на основе признаков
4. **Интеграция:** Подключение дополнительных источников (ЕГРЮЛ, арбитражы)

---

## 8. Данные:
- RFSD — [tochno.st](https://tochno.st/datasets/rfsd) (открытые данные)
- RuNSD — [OpenSanctions](https://www.opensanctions.org/) (CC-BY-4.0)
