CREATE TABLE conceptos_iva (
  concepto VARCHAR(600),
  PRIMARY KEY (concepto)
);
-- Catálogo de conceptos IVA
INSERT INTO conceptos_iva (concepto)
VALUES
  ('bienes gravados con iva'),
  ('servicios y derechos, comisiones por intermediacion, contratos de consultoria'),
  ('servicios profesionales personas naturales con titulo universitario'),
  ('arrendamiento de inmuebles de personas naturales o sucesiones indivisas no obligadas a llevar contabilidad'),
  ('dietas, honorarios a miembros de directorios y cuerpos colegiados'),
  ('servicios de construccion'),
  ('servicios digitales importados'),
  ('Importación de servicios'),
  ('servicios realizados por instituciones del estado y empresas publicas'),
  ('servicios y bienes de aviacion')


CREATE TABLE reglas_retencion_iva (
  tipo_concepto_iva VARCHAR(600) NOT NULL,
  tipo_contribuyente VARCHAR(400) NOT NULL,
  clase_contribuyente VARCHAR(300) NOT NULL,
  categoria VARCHAR(200) NOT NULL,
  obligado_contabilidad TINYINT(1) DEFAULT 0,
  contribuyente_especial TINYINT(0) DEFAULT 0,
  excepcion VARCHAR(100),
  descripcion_excepcion,
  porcentaje_retencion NOT NULL,
  codigo_formulario NOT NULL,
  PRIMARY KEY (
    tipo_concepto_iva, 
    tipo_contribuyente, 
    clase_contribuyente, 
    categoria, 
    obligado_contabilidad, 
    contribuyente_especial, 
    excepcion
  ),
  FOREIGN KEY (descripcion_excepcion) 
    REFERENCES tabla_excepciones_retenciones(descripcion_excepcion),
  FOREIGN KEY (tipo_concepto_iva)
    REFERENCES tipo_concepto_iva_formulario(tipo_concepto_iva)
);

INSERT INTO reglas_retencion_iva
WITH
conceptos AS (
  SELECT tipo_concepto_iva FROM tipo_concepto_iva_formulario
),
contribuyentes AS (
  SELECT DISTINCT tipo_contribuyente, clase_contribuyente, categoria
  FROM base_rucs_sri
),
flags AS (
  SELECT *
  FROM (VALUES (TRUE), (FALSE)) t1(contribuyente_especial)
  CROSS JOIN (VALUES (TRUE), (FALSE)) t2(obligado_contabilidad)
),
combinaciones AS (
  SELECT
    c.tipo_concepto_iva,
    t.tipo_contribuyente,
    t.clase_contribuyente,
    t.categoria,
    f.obligado_contabilidad,
    f.contribuyente_especial,
    '' AS excepcion,
    NULL AS descripcion_excepcion
  FROM conceptos c
  CROSS JOIN contribuyentes t
  CROSS JOIN flags f
)
SELECT
  *,
  CASE
    -- Sin IVA
    WHEN categoria = 'NEGOCIO POPULAR'
      THEN 0
    -- Exentos de retención
    WHEN tipo_concepto_iva IN (
      'servicios realizados por instituciones del estado y empresas publicas',
      'servicios y bienes de aviacion'
    ) THEN 0
    -- Siempre 100%
    WHEN tipo_concepto_iva IN (
      'servicios digitales importados',
      'Importación de servicios',
      'servicios profesionales personas naturales con titulo universitario',
      'dietas, honorarios a miembros de directorios y cuerpos colegiados'
    ) THEN 100
    -- Arrendamiento: 100% solo si no obligado a contabilidad
    WHEN tipo_concepto_iva = 'arrendamiento de inmuebles de personas naturales o sucesiones indivisas no obligadas a llevar contabilidad'
      AND obligado_contabilidad = FALSE
      THEN 100
    -- Construcción: siempre 30%
    WHEN tipo_concepto_iva = 'servicios de construccion'
      THEN 30
    -- Bienes: depende de contribuyente especial
    WHEN tipo_concepto_iva = 'bienes gravados con iva' AND contribuyente_especial = TRUE  THEN 10
    WHEN tipo_concepto_iva = 'bienes gravados con iva' AND contribuyente_especial = FALSE THEN 30
    -- Servicios: depende de contribuyente especial
    WHEN tipo_concepto_iva = 'servicios y derechos, comisiones por intermediacion, contratos de consultoria' AND contribuyente_especial = TRUE  THEN 20
    WHEN tipo_concepto_iva = 'servicios y derechos, comisiones por intermediacion, contratos de consultoria' AND contribuyente_especial = FALSE THEN 70
    ELSE NULL
  END AS porcentaje_retencion,
 
  CASE
    WHEN categoria = 'NEGOCIO POPULAR'                                                                              THEN NULL
    WHEN tipo_concepto_iva IN ('servicios realizados por instituciones del estado y empresas publicas','servicios y bienes de aviacion') THEN NULL
    WHEN tipo_concepto_iva IN ('servicios digitales importados','Importación de servicios','servicios profesionales personas naturales con titulo universitario','dietas, honorarios a miembros de directorios y cuerpos colegiados') THEN '731'
    WHEN tipo_concepto_iva = 'arrendamiento de inmuebles de personas naturales o sucesiones indivisas no obligadas a llevar contabilidad' AND obligado_contabilidad = FALSE THEN '731'
    WHEN tipo_concepto_iva = 'servicios de construccion'                                                            THEN '725'
    WHEN tipo_concepto_iva = 'bienes gravados con iva'        AND contribuyente_especial = TRUE                     THEN '721'
    WHEN tipo_concepto_iva = 'bienes gravados con iva'        AND contribuyente_especial = FALSE                    THEN '725'
    WHEN tipo_concepto_iva = 'servicios y derechos, comisiones por intermediacion, contratos de consultoria' AND contribuyente_especial = TRUE  THEN '723'
    WHEN tipo_concepto_iva = 'servicios y derechos, comisiones por intermediacion, contratos de consultoria' AND contribuyente_especial = FALSE THEN '729'
    ELSE NULL
  END AS codigo_formulario
 
FROM combinaciones;
