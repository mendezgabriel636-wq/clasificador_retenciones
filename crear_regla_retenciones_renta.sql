CREATE TABLE conceptos_renta (
  concepto VARCHAR(100),
  PRIMARY KEY (concepto)
);

INSERT INTO conceptos_renta (concepto) VALUES
  ('bien agropecuario'),
  ('servicio mano obra'),
  ('minerales'),
  ('bien mueble'),
  ('energia'),
  ('residual'),
  ('construccion'),
  ('transporte'),
  ('medios comunicacion'),
  ('servicio intelecto'),
  ('financiero banco'),
  ('financiero otros'),
  ('seguros'),
  ('arrendamiento inmueble'),
  ('comisiones'),
  ('servicio profesional'),
  ('arrendamiento mercantil'),
  ('sector publico'),
  ('educacion'),
  ('imagen renombre'),
  ('loterias'),
  ('domestico'),
  ('extraterritorial');

-- -----------------------------------------------------------------------------
-- TABLA PRINCIPAL: reglas_retencion_renta
-- Cada fila = un caso posible del motor Python calcular_retencion_renta()
-- -----------------------------------------------------------------------------
CREATE TABLE reglas_retencion_renta (
  contribuyente_especial    TINYINT(1)    NOT NULL DEFAULT 0,
  clase_contribuyente       VARCHAR(100)  NOT NULL,
  categoria                 VARCHAR(100)  NOT NULL,
  tipo_contribuyente        VARCHAR(100)  NOT NULL,
  tipo_concepto_ir          VARCHAR(100)  NOT NULL,
  regla                     VARCHAR(10)   NOT NULL,
  codigo_sri                VARCHAR(10)   NOT NULL,
  porcentaje_retencion      DECIMAL(5,2)  NOT NULL,
  descripcion               VARCHAR(300)  NOT NULL,
  base_calculo              VARCHAR(300)  NOT NULL,
  PRIMARY KEY (
    contribuyente_especial,
    clase_contribuyente,
    categoria,
    tipo_contribuyente,
    tipo_concepto_ir
  ),
  FOREIGN KEY (tipo_concepto_ir)
    REFERENCES tipo_concepto_ir_catalogo(tipo_concepto_ir)
);

-- =============================================================================
-- REGLA 1: Contribuyente Especial → 332, 0%, NO RETENER
-- =============================================================================
-- R1A: campo contribuyente_especial = SI/1/TRUE
INSERT INTO reglas_retencion_renta VALUES
  (1, '(cualquiera)', '(cualquiera)', 'PERSONA NATURAL', '(cualquiera)', 'R1', '332', 0.00, 'NO RETENER - Contribuyente Especial', 'Art.92 LORTI'),
  (1, '(cualquiera)', '(cualquiera)', 'SOCIEDAD',        '(cualquiera)', 'R1', '332', 0.00, 'NO RETENER - Contribuyente Especial', 'Art.92 LORTI');

-- R1B: clase_contribuyente = ESPECIAL
INSERT INTO reglas_retencion_renta VALUES
  (0, 'ESPECIAL', '(cualquiera)', 'PERSONA NATURAL', '(cualquiera)', 'R1', '332', 0.00, 'NO RETENER - Contribuyente Especial', 'Art.92 LORTI'),
  (0, 'ESPECIAL', '(cualquiera)', 'SOCIEDAD',        '(cualquiera)', 'R1', '332', 0.00, 'NO RETENER - Contribuyente Especial', 'Art.92 LORTI');

-- =============================================================================
-- REGLA 2: RIMPE Negocio Popular → 332, 0%
-- =============================================================================
INSERT INTO reglas_retencion_renta VALUES
  (0, 'RIMPE', 'NEGOCIO POPULAR', 'PERSONA NATURAL', '(cualquiera)', 'R2', '332', 0.00, 'No sujeto a retención / RIMPE Negocio Popular', 'RIMPE Negocio Popular'),
  (0, 'RIMPE', 'NEGOCIO POPULAR', 'SOCIEDAD',        '(cualquiera)', 'R2', '332', 0.00, 'No sujeto a retención / RIMPE Negocio Popular', 'RIMPE Negocio Popular');

-- =============================================================================
-- REGLA 3: RIMPE Emprendedor → 343, 1%
-- =============================================================================
INSERT INTO reglas_retencion_renta VALUES
  (0, 'RIMPE', 'EMPRENDEDOR', 'PERSONA NATURAL', '(cualquiera)', 'R3', '343', 1.00, 'RIMPE Emprendedor', 'RIMPE Emprendedor'),
  (0, 'RIMPE', 'EMPRENDEDOR', 'SOCIEDAD',        '(cualquiera)', 'R3', '343', 1.00, 'RIMPE Emprendedor', 'RIMPE Emprendedor');

-- =============================================================================
-- REGLA 4: Régimen General — Conceptos DIFERENCIADOS PN vs SOC
-- (del MAPEO_DIFERENCIADO: código distinto según tipo_contribuyente)
-- =============================================================================

-- SERVICIO_PROFESIONAL: PN → 303 (10%) / SOC → 303A (5%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'SERVICIO_PROFESIONAL', 'R4', '303',  10.00, 'Honorarios profesionales - personas naturales',  'SERVICIO_PROFESIONAL → 303'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'SERVICIO_PROFESIONAL', 'R4', '303A',  5.00, 'Servicios profesionales - SOCIEDADES',           'SERVICIO_PROFESIONAL → 303A');

-- SERVICIO_INTELECTO: PN → 304 (10%) / SOC → 303A (5%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'SERVICIO_INTELECTO', 'R4', '304',  10.00, 'Servicios prevalece intelecto - personas naturales', 'SERVICIO_INTELECTO → 304'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'SERVICIO_INTELECTO', 'R4', '303A',  5.00, 'Servicios profesionales - SOCIEDADES',               'SERVICIO_INTELECTO → 303A');

-- COMISIONES: PN → 304A (10%) / SOC → 3482 (5%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'COMISIONES', 'R4', '304A', 10.00, 'Comisiones a personas naturales',  'COMISIONES → 304A'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'COMISIONES', 'R4', '3482',  5.00, 'Comisiones a sociedades',          'COMISIONES → 3482');

-- EDUCACION: PN → 304E (10%) / SOC → 303A (5%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'EDUCACION', 'R4', '304E', 10.00, 'Docencia',                             'EDUCACION → 304E'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'EDUCACION', 'R4', '303A',  5.00, 'Servicios profesionales - SOCIEDADES', 'EDUCACION → 303A');

-- ARRENDAMIENTO_MERCANTIL: PN → 3440 (3%) / SOC → 319 (2%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'ARRENDAMIENTO_MERCANTIL', 'R4', '3440', 3.00, 'Otras retenciones 3% (residual)',                          'ARRENDAMIENTO_MERCANTIL → 3440'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'ARRENDAMIENTO_MERCANTIL', 'R4', '319',  2.00, 'Arrendamiento mercantil (leasing) - sociedades',          'ARRENDAMIENTO_MERCANTIL → 319');

-- =============================================================================
-- REGLA 4: Régimen General — Conceptos SIMPLES (mismo código PN y SOC)
-- (del MAPEO: código único independiente del tipo_contribuyente)
-- =============================================================================

-- BIEN_AGROPECUARIO → 312C (1.75%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'BIEN_AGROPECUARIO', 'R4', '312C', 1.75, 'Compras al COMERCIALIZADOR agropecuario', 'BIEN_AGROPECUARIO → 312C'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'BIEN_AGROPECUARIO', 'R4', '312C', 1.75, 'Compras al COMERCIALIZADOR agropecuario', 'BIEN_AGROPECUARIO → 312C');

-- SERVICIO_MANO_OBRA → 307 (3%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'SERVICIO_MANO_OBRA', 'R4', '307', 3.00, 'Servicios predomina mano de obra', 'SERVICIO_MANO_OBRA → 307'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'SERVICIO_MANO_OBRA', 'R4', '307', 3.00, 'Servicios predomina mano de obra', 'SERVICIO_MANO_OBRA → 307');

-- MINERALES → 344B (2%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'MINERALES', 'R4', '344B', 2.00, 'Sustancias minerales', 'MINERALES → 344B'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'MINERALES', 'R4', '344B', 2.00, 'Sustancias minerales', 'MINERALES → 344B');

-- BIEN_MUEBLE → 312 (2%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'BIEN_MUEBLE', 'R4', '312', 2.00, 'Transferencia de bienes muebles corporales', 'BIEN_MUEBLE → 312'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'BIEN_MUEBLE', 'R4', '312', 2.00, 'Transferencia de bienes muebles corporales', 'BIEN_MUEBLE → 312');

-- ENERGIA → 343A (2%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'ENERGIA', 'R4', '343A', 2.00, 'Energía eléctrica', 'ENERGIA → 343A'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'ENERGIA', 'R4', '343A', 2.00, 'Energía eléctrica', 'ENERGIA → 343A');

-- RESIDUAL → 3440 (3%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'RESIDUAL', 'R4', '3440', 3.00, 'Otras retenciones 3% (residual)', 'RESIDUAL → 3440'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'RESIDUAL', 'R4', '3440', 3.00, 'Otras retenciones 3% (residual)', 'RESIDUAL → 3440');

-- CONSTRUCCION → 343B (2%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'CONSTRUCCION', 'R4', '343B', 2.00, 'Construcción obra material inmueble', 'CONSTRUCCION → 343B'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'CONSTRUCCION', 'R4', '343B', 2.00, 'Construcción obra material inmueble', 'CONSTRUCCION → 343B');

-- TRANSPORTE → 310 (1%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'TRANSPORTE', 'R4', '310', 1.00, 'Transporte privado pasajeros / público y privado carga', 'TRANSPORTE → 310'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'TRANSPORTE', 'R4', '310', 1.00, 'Transporte privado pasajeros / público y privado carga', 'TRANSPORTE → 310');

-- MEDIOS_COMUNICACION → 309 (3%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'MEDIOS_COMUNICACION', 'R4', '309', 3.00, 'Medios comunicación y publicidad', 'MEDIOS_COMUNICACION → 309'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'MEDIOS_COMUNICACION', 'R4', '309', 3.00, 'Medios comunicación y publicidad', 'MEDIOS_COMUNICACION → 309');

-- FINANCIERO_BANCO → 323O (0%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'FINANCIERO_BANCO', 'R4', '323O', 0.00, 'Intereses a bancos y entidades financieras', 'FINANCIERO_BANCO → 323O'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'FINANCIERO_BANCO', 'R4', '323O', 0.00, 'Intereses a bancos y entidades financieras', 'FINANCIERO_BANCO → 323O');

-- FINANCIERO_OTROS → 3440 (3%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'FINANCIERO_OTROS', 'R4', '3440', 3.00, 'Otras retenciones 3% (residual)', 'FINANCIERO_OTROS → 3440'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'FINANCIERO_OTROS', 'R4', '3440', 3.00, 'Otras retenciones 3% (residual)', 'FINANCIERO_OTROS → 3440');

-- SEGUROS → 322 (2%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'SEGUROS', 'R4', '322', 2.00, 'Seguros y reaseguros (primas)', 'SEGUROS → 322'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'SEGUROS', 'R4', '322', 2.00, 'Seguros y reaseguros (primas)', 'SEGUROS → 322');

-- ARRENDAMIENTO_INMUEBLE → 320 (10%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'ARRENDAMIENTO_INMUEBLE', 'R4', '320', 10.00, 'Arrendamiento bienes inmuebles', 'ARRENDAMIENTO_INMUEBLE → 320'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'ARRENDAMIENTO_INMUEBLE', 'R4', '320', 10.00, 'Arrendamiento bienes inmuebles', 'ARRENDAMIENTO_INMUEBLE → 320');

-- SECTOR_PUBLICO → 3440 (3%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'SECTOR_PUBLICO', 'R4', '3440', 3.00, 'Otras retenciones 3% (residual)', 'SECTOR_PUBLICO → 3440'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'SECTOR_PUBLICO', 'R4', '3440', 3.00, 'Otras retenciones 3% (residual)', 'SECTOR_PUBLICO → 3440');

-- IMAGEN_RENOMBRE → 308 (10%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'IMAGEN_RENOMBRE', 'R4', '308', 10.00, 'Uso imagen/renombre (influencers)', 'IMAGEN_RENOMBRE → 308'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'IMAGEN_RENOMBRE', 'R4', '308', 10.00, 'Uso imagen/renombre (influencers)', 'IMAGEN_RENOMBRE → 308');

-- LOTERIAS → 335 (15%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'LOTERIAS', 'R4', '335', 15.00, 'Loterías, rifas, apuestas', 'LOTERIAS → 335'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'LOTERIAS', 'R4', '335', 15.00, 'Loterías, rifas, apuestas', 'LOTERIAS → 335');

-- DOMESTICO → 3440 (3%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'DOMESTICO', 'R4', '3440', 3.00, 'Otras retenciones 3% (residual)', 'DOMESTICO → 3440'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'DOMESTICO', 'R4', '3440', 3.00, 'Otras retenciones 3% (residual)', 'DOMESTICO → 3440');

-- EXTRATERRITORIAL → 332 (0%)
INSERT INTO reglas_retencion_renta VALUES
  (0, 'OTROS', 'REGIMEN GENERAL', 'PERSONA NATURAL', 'EXTRATERRITORIAL', 'R4', '332', 0.00, 'No sujeto a retención / RIMPE Negocio Popular', 'EXTRATERRITORIAL → 332'),
  (0, 'OTROS', 'REGIMEN GENERAL', 'SOCIEDAD',        'EXTRATERRITORIAL', 'R4', '332', 0.00, 'No sujeto a retención / RIMPE Negocio Popular', 'EXTRATERRITORIAL → 332');
