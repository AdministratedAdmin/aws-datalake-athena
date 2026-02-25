-- =========================================
-- Data Lake Serverless - AWS Athena
-- Autor: Pedro Henrique
-- =========================================

-- Criar database
CREATE DATABASE IF NOT EXISTS pedro_lab;

-- Criar tabela externa apontando para S3
CREATE EXTERNAL TABLE IF NOT EXISTS pedro_lab.dados_brutos (
  id INT,
  day_time STRING,
  work_hours DECIMAL(5,2),
  screen_time_hours DECIMAL(5,2),
  meetings_count INT,
  breaks_taken INT,
  after_hours_work INT,
  sleep_hours DECIMAL(4,2),
  task_completion_rate DECIMAL(5,2),
  burnout_score DECIMAL(5,2),
  burnout_risk STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
LOCATION 's3://.../'
TBLPROPERTIES ('classification' = 'csv');

-- =========================================
-- Consultas Analíticas
-- =========================================

-- Média de burnout por período
SELECT 
    day_time,
    AVG(burnout_score) AS avg_burnout
FROM dados_brutos
GROUP BY day_time
ORDER BY avg_burnout DESC;

-- Horas trabalhadas e risco de burnout
SELECT 
    burnout_risk,
    AVG(work_hours) AS avg_work_hours
FROM dados_brutos
GROUP BY burnout_risk
ORDER BY avg_work_hours DESC;