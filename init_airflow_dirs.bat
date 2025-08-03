@echo off
REM 🔁 ลบแล้วสร้าง airflow ใหม่หมด
rd /s /q airflow
mkdir airflow
mkdir airflow\logs
mkdir airflow\logs\scheduler
mkdir airflow\nc
mkdir airflow\csv
mkdir airflow\db
