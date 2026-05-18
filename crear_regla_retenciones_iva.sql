-- =============================================================================
-- Tabla de reglas de retención del IVA
--
-- Claves de join (deben existir en base_rucs_sri + ciiu_clasificado):
--   tipo_concepto_iva, tipo_contribuyente, clase_contribuyente, categoria,
--   obligado_llevar_contabilidad, contribuyente_especial
--
-- Columnas de salida (se añaden al resultado por el join):
--   porcentaje_retencion_iva, campo_formulario_104_iva, codigo_anexo_iva
-- =============================================================================

CREATE TABLE reglas_retencion_iva (
  -- claves de clasificación
  tipo_concepto_iva              VARCHAR(200)  NOT NULL,
  tipo_contribuyente             VARCHAR(50)   NOT NULL,
  clase_contribuyente            VARCHAR(50)   NOT NULL,
  categoria                      VARCHAR(100)  NOT NULL,
  obligado_llevar_contabilidad   TINYINT(1)    NOT NULL DEFAULT 0,
  contribuyente_especial         TINYINT(1)    NOT NULL DEFAULT 0,
  -- columnas de salida
  porcentaje_retencion_iva       DECIMAL(5,2)  NOT NULL,
  campo_formulario_104_iva       INT,
  codigo_anexo_iva               INT,
  PRIMARY KEY (
    tipo_concepto_iva,
    tipo_contribuyente,
    clase_contribuyente,
    categoria,
    obligado_llevar_contabilidad,
    contribuyente_especial
  )
);

-- =============================================================================
-- Datos: todas las combinaciones posibles
-- Generadas cruzando conceptos × tipos de contribuyente × flags
-- =============================================================================

-- ---- NEGOCIO POPULAR: sin retención ----------------------------------------
INSERT INTO reglas_retencion_iva (tipo_concepto_iva, tipo_contribuyente, clase_contribuyente, categoria, obligado_llevar_contabilidad, contribuyente_especial, porcentaje_retencion_iva, campo_formulario_104_iva, codigo_anexo_iva)
SELECT c.tipo_concepto_iva, t.tipo_contribuyente, t.clase_contribuyente, 'NEGOCIO POPULAR', f.obligado, f.especial, 0, NULL, NULL
FROM (
  SELECT 'bienes gravados con iva'                                                                                                               AS tipo_concepto_iva UNION ALL
  SELECT 'servicios y derechos, comisiones por intermediacion, contratos de consultoria'                                                         UNION ALL
  SELECT 'servicios profesionales personas naturales con titulo universitario'                                                                    UNION ALL
  SELECT 'arrendamiento de inmuebles de personas naturales o sucesiones indivisas no obligadas a llevar contabilidad'                            UNION ALL
  SELECT 'dietas, honorarios a miembros de directorios y cuerpos colegiados'                                                                     UNION ALL
  SELECT 'servicios de construccion'                                                                                                              UNION ALL
  SELECT 'servicios digitales importados'                                                                                                         UNION ALL
  SELECT 'Importación de servicios'                                                                                                               UNION ALL
  SELECT 'servicios realizados por instituciones del estado y empresas publicas'                                                                  UNION ALL
  SELECT 'servicios y bienes de aviacion'
) c
CROSS JOIN (
  SELECT DISTINCT tipo_contribuyente, clase_contribuyente FROM base_rucs_sri WHERE categoria = 'NEGOCIO POPULAR'
) t
CROSS JOIN (SELECT 0 AS obligado, 0 AS especial UNION ALL SELECT 0,1 UNION ALL SELECT 1,0 UNION ALL SELECT 1,1) f;

-- ---- Exentos: instituciones del estado y aviación → 0%, sin campo ----------
INSERT INTO reglas_retencion_iva (tipo_concepto_iva, tipo_contribuyente, clase_contribuyente, categoria, obligado_llevar_contabilidad, contribuyente_especial, porcentaje_retencion_iva, campo_formulario_104_iva, codigo_anexo_iva)
SELECT c.tipo_concepto_iva, t.tipo_contribuyente, t.clase_contribuyente, t.categoria, f.obligado, f.especial, 0, NULL, NULL
FROM (
  SELECT 'servicios realizados por instituciones del estado y empresas publicas' AS tipo_concepto_iva
  UNION ALL
  SELECT 'servicios y bienes de aviacion'
) c
CROSS JOIN (
  SELECT DISTINCT tipo_contribuyente, clase_contribuyente, categoria FROM base_rucs_sri WHERE categoria <> 'NEGOCIO POPULAR'
) t
CROSS JOIN (SELECT 0 AS obligado, 0 AS especial UNION ALL SELECT 0,1 UNION ALL SELECT 1,0 UNION ALL SELECT 1,1) f;

-- ---- Siempre 100%: digitales, importación, profesionales, dietas -----------
INSERT INTO reglas_retencion_iva (tipo_concepto_iva, tipo_contribuyente, clase_contribuyente, categoria, obligado_llevar_contabilidad, contribuyente_especial, porcentaje_retencion_iva, campo_formulario_104_iva, codigo_anexo_iva)
SELECT c.tipo_concepto_iva, t.tipo_contribuyente, t.clase_contribuyente, t.categoria, f.obligado, f.especial, 100, 731, 7311
FROM (
  SELECT 'servicios digitales importados'                                                                        AS tipo_concepto_iva
  UNION ALL SELECT 'Importación de servicios'
  UNION ALL SELECT 'servicios profesionales personas naturales con titulo universitario'
  UNION ALL SELECT 'dietas, honorarios a miembros de directorios y cuerpos colegiados'
) c
CROSS JOIN (
  SELECT DISTINCT tipo_contribuyente, clase_contribuyente, categoria FROM base_rucs_sri WHERE categoria <> 'NEGOCIO POPULAR'
) t
CROSS JOIN (SELECT 0 AS obligado, 0 AS especial UNION ALL SELECT 0,1 UNION ALL SELECT 1,0 UNION ALL SELECT 1,1) f;

-- ---- Arrendamiento: 100% solo si NO obligado a contabilidad ----------------
INSERT INTO reglas_retencion_iva (tipo_concepto_iva, tipo_contribuyente, clase_contribuyente, categoria, obligado_llevar_contabilidad, contribuyente_especial, porcentaje_retencion_iva, campo_formulario_104_iva, codigo_anexo_iva)
SELECT 'arrendamiento de inmuebles de personas naturales o sucesiones indivisas no obligadas a llevar contabilidad',
       t.tipo_contribuyente, t.clase_contribuyente, t.categoria, 0, f.especial, 100, 731, 7312
FROM (SELECT DISTINCT tipo_contribuyente, clase_contribuyente, categoria FROM base_rucs_sri WHERE categoria <> 'NEGOCIO POPULAR') t
CROSS JOIN (SELECT 0 AS especial UNION ALL SELECT 1) f;

-- Arrendamiento: obligado a contabilidad → aplica regla de servicios (no retiene por este concepto)
INSERT INTO reglas_retencion_iva (tipo_concepto_iva, tipo_contribuyente, clase_contribuyente, categoria, obligado_llevar_contabilidad, contribuyente_especial, porcentaje_retencion_iva, campo_formulario_104_iva, codigo_anexo_iva)
SELECT 'arrendamiento de inmuebles de personas naturales o sucesiones indivisas no obligadas a llevar contabilidad',
       t.tipo_contribuyente, t.clase_contribuyente, t.categoria, 1, f.especial, 70, 729, 7291
FROM (SELECT DISTINCT tipo_contribuyente, clase_contribuyente, categoria FROM base_rucs_sri WHERE categoria <> 'NEGOCIO POPULAR') t
CROSS JOIN (SELECT 0 AS especial UNION ALL SELECT 1) f;

-- ---- Construcción: siempre 30% --------------------------------------------
INSERT INTO reglas_retencion_iva (tipo_concepto_iva, tipo_contribuyente, clase_contribuyente, categoria, obligado_llevar_contabilidad, contribuyente_especial, porcentaje_retencion_iva, campo_formulario_104_iva, codigo_anexo_iva)
SELECT 'servicios de construccion', t.tipo_contribuyente, t.clase_contribuyente, t.categoria, f.obligado, f.especial, 30, 725, 7251
FROM (SELECT DISTINCT tipo_contribuyente, clase_contribuyente, categoria FROM base_rucs_sri WHERE categoria <> 'NEGOCIO POPULAR') t
CROSS JOIN (SELECT 0 AS obligado, 0 AS especial UNION ALL SELECT 0,1 UNION ALL SELECT 1,0 UNION ALL SELECT 1,1) f;

-- ---- Bienes: contribuyente especial → 10%, otros → 30% --------------------
INSERT INTO reglas_retencion_iva (tipo_concepto_iva, tipo_contribuyente, clase_contribuyente, categoria, obligado_llevar_contabilidad, contribuyente_especial, porcentaje_retencion_iva, campo_formulario_104_iva, codigo_anexo_iva)
SELECT 'bienes gravados con iva', t.tipo_contribuyente, t.clase_contribuyente, t.categoria, f.obligado, 1, 10, 721, 7211
FROM (SELECT DISTINCT tipo_contribuyente, clase_contribuyente, categoria FROM base_rucs_sri WHERE categoria <> 'NEGOCIO POPULAR') t
CROSS JOIN (SELECT 0 AS obligado UNION ALL SELECT 1) f;

INSERT INTO reglas_retencion_iva (tipo_concepto_iva, tipo_contribuyente, clase_contribuyente, categoria, obligado_llevar_contabilidad, contribuyente_especial, porcentaje_retencion_iva, campo_formulario_104_iva, codigo_anexo_iva)
SELECT 'bienes gravados con iva', t.tipo_contribuyente, t.clase_contribuyente, t.categoria, f.obligado, 0, 30, 725, 7252
FROM (SELECT DISTINCT tipo_contribuyente, clase_contribuyente, categoria FROM base_rucs_sri WHERE categoria <> 'NEGOCIO POPULAR') t
CROSS JOIN (SELECT 0 AS obligado UNION ALL SELECT 1) f;

-- ---- Servicios: contribuyente especial → 20%, otros → 70% -----------------
INSERT INTO reglas_retencion_iva (tipo_concepto_iva, tipo_contribuyente, clase_contribuyente, categoria, obligado_llevar_contabilidad, contribuyente_especial, porcentaje_retencion_iva, campo_formulario_104_iva, codigo_anexo_iva)
SELECT 'servicios y derechos, comisiones por intermediacion, contratos de consultoria',
       t.tipo_contribuyente, t.clase_contribuyente, t.categoria, f.obligado, 1, 20, 723, 7231
FROM (SELECT DISTINCT tipo_contribuyente, clase_contribuyente, categoria FROM base_rucs_sri WHERE categoria <> 'NEGOCIO POPULAR') t
CROSS JOIN (SELECT 0 AS obligado UNION ALL SELECT 1) f;

INSERT INTO reglas_retencion_iva (tipo_concepto_iva, tipo_contribuyente, clase_contribuyente, categoria, obligado_llevar_contabilidad, contribuyente_especial, porcentaje_retencion_iva, campo_formulario_104_iva, codigo_anexo_iva)
SELECT 'servicios y derechos, comisiones por intermediacion, contratos de consultoria',
       t.tipo_contribuyente, t.clase_contribuyente, t.categoria, f.obligado, 0, 70, 729, 7292
FROM (SELECT DISTINCT tipo_contribuyente, clase_contribuyente, categoria FROM base_rucs_sri WHERE categoria <> 'NEGOCIO POPULAR') t
CROSS JOIN (SELECT 0 AS obligado UNION ALL SELECT 1) f;
