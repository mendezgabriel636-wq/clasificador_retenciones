-- =============================================================================
-- Tabla de reglas de retención IR (Impuesto a la Renta)
--
-- Claves de join:
--   clase_contribuyente, categoria, contribuyente_especial,
--   tipo_contribuyente, tipo_concepto_ir
--
-- Columnas de salida:
--   nro_campo, porcentaje_retencion_renta, campo_formulario_103_ir, codigo_anexo_ir
--
-- REQUIERE que base_rucs_sri y ciiu_clasificado ya estén cargadas en MySQL.
-- =============================================================================

DROP TABLE IF EXISTS reglas_retencion_renta;

CREATE TABLE reglas_retencion_renta (
  clase_contribuyente        VARCHAR(100)  NOT NULL,
  categoria                  VARCHAR(100)  NOT NULL,
  contribuyente_especial     TINYINT(1)    NOT NULL DEFAULT 0,
  tipo_contribuyente         VARCHAR(100)  NOT NULL,
  tipo_concepto_ir           VARCHAR(100)  NOT NULL,
  nro_campo                  INT           NOT NULL,
  porcentaje_retencion_renta DECIMAL(5,2)  NOT NULL,
  campo_formulario_103_ir    INT           NOT NULL,
  codigo_anexo_ir            INT           NOT NULL,
  PRIMARY KEY (
    clase_contribuyente,
    categoria,
    contribuyente_especial,
    tipo_contribuyente,
    tipo_concepto_ir
  )
);

-- Tabla auxiliar con los 23 tipos de concepto IR
CREATE TEMPORARY TABLE tmp_tipos_ir (tipo_concepto_ir VARCHAR(100));
INSERT INTO tmp_tipos_ir VALUES
  ('BIEN_MUEBLE'),('BIEN_AGROPECUARIO'),('MINERALES'),('ENERGIA'),
  ('SERVICIO_MANO_OBRA'),('SERVICIO_PROFESIONAL'),('SERVICIO_INTELECTO'),
  ('CONSTRUCCION'),('TRANSPORTE'),('MEDIOS_COMUNICACION'),('COMISIONES'),
  ('EDUCACION'),('DOMESTICO'),('IMAGEN_RENOMBRE'),('FINANCIERO_BANCO'),
  ('FINANCIERO_OTROS'),('SEGUROS'),('ARRENDAMIENTO_INMUEBLE'),
  ('ARRENDAMIENTO_MERCANTIL'),('SECTOR_PUBLICO'),('LOTERIAS'),
  ('EXTRATERRITORIAL'),('RESIDUAL');

-- =============================================================================
-- REGLA 1: contribuyente_especial = 1  →  0% · campo 332 · cód 3321
-- Aplica a TODAS las combinaciones de clase/categoria/tipo_contribuyente
-- y a TODOS los tipos de concepto IR.
-- =============================================================================
INSERT IGNORE INTO reglas_retencion_renta
SELECT
    b.clase_contribuyente,
    b.categoria,
    1                    AS contribuyente_especial,
    b.tipo_contribuyente,
    c.tipo_concepto_ir,
    1                    AS nro_campo,
    0.00                 AS porcentaje_retencion_renta,
    332                  AS campo_formulario_103_ir,
    3321                 AS codigo_anexo_ir
FROM (SELECT DISTINCT clase_contribuyente, categoria, tipo_contribuyente FROM base_rucs_sri) b
CROSS JOIN tmp_tipos_ir c;

-- =============================================================================
-- REGLA 2: RIMPE Negocio Popular  →  0% · campo 332 · cód 3321
-- =============================================================================
INSERT IGNORE INTO reglas_retencion_renta
SELECT
    'RIMPE', 'NEGOCIO POPULAR', 0,
    t.tipo_contribuyente,
    c.tipo_concepto_ir,
    5, 0.00, 332, 3321
FROM (SELECT DISTINCT tipo_contribuyente FROM base_rucs_sri) t
CROSS JOIN tmp_tipos_ir c;

-- =============================================================================
-- REGLA 3: RIMPE Emprendedor  →  1% · campo 343 · cód 3431
-- =============================================================================
INSERT IGNORE INTO reglas_retencion_renta
SELECT
    'RIMPE', 'EMPRENDEDOR', 0,
    t.tipo_contribuyente,
    c.tipo_concepto_ir,
    7, 1.00, 343, 3431
FROM (SELECT DISTINCT tipo_contribuyente FROM base_rucs_sri) t
CROSS JOIN tmp_tipos_ir c;

-- =============================================================================
-- REGLA 4: Régimen General — lookup por tipo_concepto_ir y tipo_contribuyente
-- Aplica a todas las clases que NO son RIMPE y con contribuyente_especial = 0.
-- =============================================================================

-- Helper: combinaciones reales de clase/categoria que no son RIMPE ni especial
CREATE TEMPORARY TABLE tmp_clases_general AS
SELECT DISTINCT clase_contribuyente, categoria, tipo_contribuyente
FROM base_rucs_sri
WHERE clase_contribuyente NOT IN ('RIMPE')
  AND clase_contribuyente NOT LIKE '%ESPECIAL%';

-- SERVICIO_PROFESIONAL: PN 10%/303, SOC 5%/303
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'SERVICIO_PROFESIONAL',
  9,
  CASE g.tipo_contribuyente WHEN 'PERSONA NATURAL' THEN 10.00 ELSE 5.00 END,
  303, 3031
FROM tmp_clases_general g;

-- SERVICIO_INTELECTO: PN 10%/304, SOC 5%/303
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'SERVICIO_INTELECTO',
  11,
  CASE g.tipo_contribuyente WHEN 'PERSONA NATURAL' THEN 10.00 ELSE 5.00 END,
  CASE g.tipo_contribuyente WHEN 'PERSONA NATURAL' THEN 304 ELSE 303 END,
  CASE g.tipo_contribuyente WHEN 'PERSONA NATURAL' THEN 3041 ELSE 3031 END
FROM tmp_clases_general g;

-- COMISIONES: PN 10%/304, SOC 5%/3482
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'COMISIONES',
  13,
  CASE g.tipo_contribuyente WHEN 'PERSONA NATURAL' THEN 10.00 ELSE 5.00 END,
  CASE g.tipo_contribuyente WHEN 'PERSONA NATURAL' THEN 304 ELSE 3482 END,
  CASE g.tipo_contribuyente WHEN 'PERSONA NATURAL' THEN 3042 ELSE 34821 END
FROM tmp_clases_general g;

-- EDUCACION: PN 10%/304, SOC 5%/303
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'EDUCACION',
  15,
  CASE g.tipo_contribuyente WHEN 'PERSONA NATURAL' THEN 10.00 ELSE 5.00 END,
  CASE g.tipo_contribuyente WHEN 'PERSONA NATURAL' THEN 304 ELSE 303 END,
  CASE g.tipo_contribuyente WHEN 'PERSONA NATURAL' THEN 3043 ELSE 3034 END
FROM tmp_clases_general g;

-- ARRENDAMIENTO_MERCANTIL: PN 3%/3440, SOC 2%/319
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'ARRENDAMIENTO_MERCANTIL',
  17,
  CASE g.tipo_contribuyente WHEN 'PERSONA NATURAL' THEN 3.00 ELSE 2.00 END,
  CASE g.tipo_contribuyente WHEN 'PERSONA NATURAL' THEN 3440 ELSE 319 END,
  CASE g.tipo_contribuyente WHEN 'PERSONA NATURAL' THEN 34401 ELSE 3191 END
FROM tmp_clases_general g;

-- Conceptos simples (mismo % para PN y SOC) ----------

-- BIEN_AGROPECUARIO: 1.75% / 312
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'BIEN_AGROPECUARIO', 19, 1.75, 312, 3121 FROM tmp_clases_general g;

-- SERVICIO_MANO_OBRA: 3% / 307
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'SERVICIO_MANO_OBRA', 21, 3.00, 307, 3071 FROM tmp_clases_general g;

-- MINERALES: 2% / 344
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'MINERALES', 23, 2.00, 344, 3441 FROM tmp_clases_general g;

-- BIEN_MUEBLE: 2% / 312
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'BIEN_MUEBLE', 25, 2.00, 312, 3122 FROM tmp_clases_general g;

-- ENERGIA: 2% / 343
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'ENERGIA', 27, 2.00, 343, 3432 FROM tmp_clases_general g;

-- RESIDUAL: 3% / 3440
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'RESIDUAL', 29, 3.00, 3440, 34402 FROM tmp_clases_general g;

-- CONSTRUCCION: 2% / 343
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'CONSTRUCCION', 31, 2.00, 343, 3433 FROM tmp_clases_general g;

-- TRANSPORTE: 1% / 310
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'TRANSPORTE', 33, 1.00, 310, 3101 FROM tmp_clases_general g;

-- MEDIOS_COMUNICACION: 3% / 309
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'MEDIOS_COMUNICACION', 35, 3.00, 309, 3091 FROM tmp_clases_general g;

-- FINANCIERO_BANCO: 0% / 323
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'FINANCIERO_BANCO', 37, 0.00, 323, 3231 FROM tmp_clases_general g;

-- FINANCIERO_OTROS: 3% / 3440
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'FINANCIERO_OTROS', 39, 3.00, 3440, 34403 FROM tmp_clases_general g;

-- SEGUROS: 2% / 322
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'SEGUROS', 41, 2.00, 322, 3221 FROM tmp_clases_general g;

-- ARRENDAMIENTO_INMUEBLE: 10% / 320
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'ARRENDAMIENTO_INMUEBLE', 43, 10.00, 320, 3201 FROM tmp_clases_general g;

-- SECTOR_PUBLICO: 3% / 3440
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'SECTOR_PUBLICO', 45, 3.00, 3440, 34404 FROM tmp_clases_general g;

-- IMAGEN_RENOMBRE: 10% / 308
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'IMAGEN_RENOMBRE', 47, 10.00, 308, 3081 FROM tmp_clases_general g;

-- LOTERIAS: 15% / 335
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'LOTERIAS', 49, 15.00, 335, 3351 FROM tmp_clases_general g;

-- DOMESTICO: 3% / 3440
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'DOMESTICO', 51, 3.00, 3440, 34405 FROM tmp_clases_general g;

-- EXTRATERRITORIAL: 0% / 332
INSERT IGNORE INTO reglas_retencion_renta
SELECT g.clase_contribuyente, g.categoria, 0, g.tipo_contribuyente,
  'EXTRATERRITORIAL', 53, 0.00, 332, 3322 FROM tmp_clases_general g;

DROP TEMPORARY TABLE tmp_tipos_ir;
DROP TEMPORARY TABLE tmp_clases_general;

SELECT CONCAT('reglas_retencion_renta: ', COUNT(*), ' filas') AS resultado
FROM reglas_retencion_renta;
