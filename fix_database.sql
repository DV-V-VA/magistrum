-- Подключаемся к базе данных
\c genes_database;

-- Добавляем недостающие уникальные ограничения
ALTER TABLE ortholog_summary 
ADD CONSTRAINT unique_ortholog_summary 
UNIQUE (ortholog_id, description);

ALTER TABLE mane_transcript 
ADD CONSTRAINT unique_mane_transcript_gene 
UNIQUE (gene_id, transcript_id);

ALTER TABLE ccds_reference 
ADD CONSTRAINT unique_ccds_gene 
UNIQUE (gene_id, ccds_id);

-- Добавляем индексы для улучшения производительности поиска
CREATE INDEX IF NOT EXISTS idx_gene_alias_value ON gene_alias(alias_value);
CREATE INDEX IF NOT EXISTS idx_gene_alias_type ON gene_alias(alias_type);
CREATE INDEX IF NOT EXISTS idx_ortholog_summary_desc ON ortholog_summary(description);