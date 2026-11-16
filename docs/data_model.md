# Модель данных PM2.5 Бишкека

В PostgreSQL aiacademy схемы korgonbaeva_stg, korgonbaeva_ods и korgonbaeva_datamart соответствуют приёму, нормализации и аналитике. STG хранит поля источников в text и добавляет _ds, _source_file и _loaded_at. Журнал korgonbaeva_stg.etl_load_log содержит статус, число строк и время каждого запуска задачи.

| Таблица | Зерно и ключ |
|---|---|
| ods_sensors_history | Одно исходное измерение, PK event_id. |
| ods_locations_dictionary | Одна станция, PK location_id. |
| ods_api_hourly | Одно почасовое значение для датчика и параметра, PK sensor_id + measured_at + parameter. |
| ods_sensor_status | Текущий статус датчика, PK sensor_id. |
| dm_daily_air_quality | Один локальный день, PK ds. |
| dm_monthly_by_station | Месяц × станция, PK month + location_id. Название и город берутся из ods_locations_dictionary. |

Представление v_measurements_hourly усредняет архивные наблюдения по датчику и часу и объединяет их с часовыми значениями API от других датчиков. measurement_count показывает число датчико-часов с данными, api_measurement_count показывает их подмножество из API, mean_value содержит среднее по наблюдаемым датчико-часам. Пропуски не учитываются в среднем.

Дневная витрина копируется в ClickHouse korgonbaeva.dm_daily_air_quality. Проверка задачи сравнивает её строки с PostgreSQL за ту же дату.

## Вопрос для защиты

Сколько часов наблюдений было за сентябрь по каждой станции и сколько из них получено из API? Строка витрины отражает доступные загруженные наблюдения за месяц. Контрольный прогон загружает один день, 10 сентября.

```sql
SELECT location_name, measurement_count, api_measurement_count
FROM korgonbaeva_datamart.dm_monthly_by_station
WHERE month = DATE '2026-09-01'
ORDER BY measurement_count DESC, location_id;
```
