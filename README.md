# Corrección CIIU - base_rucs_sri

## Resumen

Este paquete corrige y enriquece `base_rucs_sri` con códigos y descripciones CIIU
estandarizadas según el catálogo INEC nivel 6. Genera dos columnas nuevas:

- `codigo_inec_corregido`: Código CIIU con formato INEC (ej: A0111.11)
- `desc_inec_corregido`: Descripción oficial INEC para ese código

## Archivos

| Archivo | Contenido | Registros |
|---------|-----------|-----------|
| `primera_correccion.parquet` | Mapeo codigo_ciiu (catastro, sin punto) → codigo_inec + desc_inec | 1,735 |
| `segunda_correccion.parquet` | Mapeo actividad_economica (SRI, texto exacto) → codigo_inec + desc_inec | 499 |
| `tercera_correccion.parquet` | Mapeo actividad_economica (SRI, semántico) → codigo_inec + desc_inec | 487 |
| `aplicar_correcciones.py` | Script que aplica las 3 etapas | — |

## Estrategia de corrección (3 etapas)

### Etapa 1: Por RUC → catastro → INEC
- Cruza `base_rucs_sri.numero_ruc` con `base_rucs_catastro.numero_ruc`
- Del catastro obtiene `codigo_ciiu` (formato sin punto, ej: A011111)
- Usa `primera_correccion` para traducir a formato INEC (A0111.11) + descripción oficial
- Cubre ~99% de los RUCs

### Etapa 2: Match directo por texto
- Para los RUCs sin catastro, compara `actividad_economica` de SRI contra descripciones INEC
- Normaliza: mayúsculas, acentos, espacios, puntuación
- Usa `segunda_correccion` (499 descripciones con match exacto)

### Etapa 3: Búsqueda semántica
- Para las descripciones restantes, usa coincidencias por TF-IDF y clasificación manual
- Usa `tercera_correccion` (480 por TF-IDF + 7 clasificadas manualmente)

## Uso

### Con base de datos MySQL:
```bash
pip install pymysql pandas pyarrow
python aplicar_correcciones.py \
    --host localhost --user root --password pass --database data_fact \
    --output base_rucs_sri_corregido.parquet
```

### Con archivos locales:
```bash
python aplicar_correcciones.py \
    --sri-parquet base_rucs_sri.parquet \
    --catastro-parquet base_rucs_catastro.parquet \
    --output base_rucs_sri_corregido.parquet
```

## Columna metodo_correccion

El script añade una columna `metodo_correccion` que indica cómo se obtuvo el código INEC:

| Valor | Significado |
|-------|-------------|
| `etapa1_catastro` | RUC encontrado en catastro, código CIIU mapeado a INEC |
| `etapa2_texto_directo` | Actividad económica coincide textualmente con INEC |
| `etapa3_semantica` | Actividad económica mapeada por búsqueda semántica o clasificación manual |
| `null` | Sin corrección posible (RUC sin catastro y actividad sin equivalente INEC) |
