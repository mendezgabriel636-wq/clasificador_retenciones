import polars as pl
import duckdb
from rapidfuzz import fuzz
from typing import List
from dataclasses import dataclass
import spacy, unicodedata

ruta_catalogo = "~/Proyectos/RETENCIONES/catalogo_retenciones.duckdb"

_STOPWORDS_ES = spacy.load("es_core_news_sm").Defaults.stop_words


def _catalogo() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(ruta_catalogo, read_only=True)


@dataclass
class MetadatosCiiu:
    total_ciiu: int
    actividades_sri: int
    actividades_con_match: int
    tabla_sin_match: pl.DataFrame
    tabla: pl.DataFrame

    def resumen(self) -> str:
        sin_match = self.actividades_sri - self.actividades_con_match
        return (
            f"📊 Cobertura CIIU Nivel 6 vs SRI\n"
            f"{'─' * 38}\n"
            f"  Total descripciones CIIU:         {self.total_ciiu:>6,}\n"
            f"  Actividades únicas en SRI:         {self.actividades_sri:>6,}\n"
            f"  Con match en CIIU:                 {self.actividades_con_match:>6,}\n"
            f"  Sin match en CIIU:                 {sin_match:>6,}\n"
            f"{'─' * 38}\n"
            f"  Cobertura:                         {self.actividades_con_match / self.actividades_sri * 100:>5.2f}%\n"
        )

    def to_csv(self, path: str):
        try:
            self.tabla.write_csv(path)
        except Exception as e:
            raise RuntimeError(f"{e}")

        print(f"Se escribio el resultado en {path}")


@dataclass
class MetadatosComparacion:
    registros_sri: int
    registros_catastro: int
    registros_comunes: int
    registros_sri_no_catastro: List[pl.Int64]
    columnas_cruce: List[str]
    columna_comparar: str
    registros_comunes_sin_columna: int
    promedio_cruce: float
    porcentaje_desactualizacion: float
    tabla: pl.DataFrame

    def __str__(self) -> str:
        return (
            f"📊 Reporte de Comparación\n"
            f"{'─' * 35}\n"
            f"  Registros SRI:           {self.registros_sri:>10,}\n"
            f"  Registros Catastro:      {self.registros_catastro:>10,}\n"
            f"  Registros comunes:       {self.registros_comunes:>10,}\n"
            f"  Sin columna comparable:  {self.registros_comunes_sin_columna:>10,}\n"
            f"{'─' * 35}\n"
            f"  Columnas de cruce:       {', '.join(self.columnas_cruce)}\n"
            f"  Columna comparada:       {self.columna_comparar}\n"
            f"{'─' * 35}\n"
            f"  Similitud promedio:      {self.promedio_cruce:>9.2f}%\n"
            f"  Desactualización:        {self.porcentaje_desactualizacion * 100:>9.2f}%\n"
            f"{'─' * 35}\n"
            f"{self.tabla}\n"
        )

    def resumen(self) -> str:
        return (
            f"📊 Resumen de Comparación\n"
            f"{'─' * 35}\n"
            f"  Registros SRI:           {self.registros_sri:>10,}\n"
            f"  Registros Catastro:      {self.registros_catastro:>10,}\n"
            f"  Registros comunes:       {self.registros_comunes:>10,}\n"
            f"  Sin columna comparable:  {self.registros_comunes_sin_columna:>10,}\n"
            f"{'─' * 35}\n"
            f"  Columnas de cruce:       {', '.join(self.columnas_cruce)}\n"
            f"  Columna comparada:       {self.columna_comparar}\n"
            f"{'─' * 35}\n"
            f"  Similitud promedio:      {self.promedio_cruce:>9.2f}%\n"
            f"  Desactualización:        {self.porcentaje_desactualizacion * 100:>9.2f}%\n"
        )

    def to_csv(self, path: str):
        try:
            # Crea un DataFrame con solo la columna id_establecimiento y el resto nulo
            filas_faltantes = pl.DataFrame(
                {"id_establecimiento": self.registros_sri_no_catastro}
            ).with_columns(
                # rellena el resto de columnas con null
                [
                    pl.lit(None).cast(self.tabla.schema[col]).alias(col)
                    for col in self.tabla.columns
                    if col != "id_establecimiento"
                ]
            )

            resultado = pl.concat([self.tabla, filas_faltantes])
            resultado.write_csv(path)
        except Exception as e:
            raise RuntimeError(f"{e}")

        print(f"Se escribio el resultado en {path}")


def comparar_fuzzy(
    tabla_a: pl.DataFrame,
    tabla_b: pl.DataFrame,
    col_name: str,
    subset_join: List[str],
    fuzzy_flag: bool = True,
) -> pl.DataFrame:
    tabla_a_analisis = (
        tabla_a.select(subset_join + [col_name])
        .with_columns(pl.col(col_name).alias(col_name + "_a"))
        .drop(col_name)
    )
    tabla_b_analisis = (
        tabla_b.select(subset_join + [col_name])
        .with_columns(pl.col(col_name).alias(col_name + "_b"))
        .drop(col_name)
    )
    tabla_compuesta_inner = tabla_a_analisis.join(
        tabla_b_analisis, on=subset_join, how="inner"
    ).with_columns(pl.lit(100.0).alias("score_join"))

    anti_a = tabla_a_analisis.join(tabla_b_analisis, on=subset_join, how="anti")
    tabla_compuesta_anti = anti_a.join(
        tabla_b_analisis.rename({c: c + "_b_join" for c in subset_join}),
        how="cross",
    )

    score_cols = []
    for col_join in subset_join:
        scores = [
            sum(
                sw := [
                    max(fuzz.token_set_ratio(wa, wb) for wb in str(b).split())
                    for wa in str(a).split()
                ]
            )
            / len(sw)
            for a, b in zip(
                tabla_compuesta_anti[col_join],
                tabla_compuesta_anti[col_join + "_b_join"],
            )
        ]
        score_col = f"_score_{col_join}"
        score_cols.append(score_col)
        tabla_compuesta_anti = tabla_compuesta_anti.with_columns(
            pl.Series(score_col, scores)
        )

    tabla_compuesta_anti = (
        tabla_compuesta_anti.with_columns(
            (
                pl.sum_horizontal([pl.col(c) for c in score_cols]) / len(score_cols)
            ).alias("score_join")
        )
        .drop(score_cols)
        .drop([c + "_b_join" for c in subset_join])
        .sort("score_join", descending=True)
        .unique(subset=anti_a.columns, keep="first")
    )

    tabla_compuesta = pl.concat(
        [tabla_compuesta_inner, tabla_compuesta_anti],
        how="diagonal",
    )

    scores_por_palabra = [
        [
            max(
                [
                    s if (s := fuzz.token_set_ratio(word_a, word_b)) >= 60.0 else 0.0
                    for word_b in limpiar(str(b)).split()
                ]
            )
            for word_a in limpiar(str(a)).split()
        ]
        for a, b in zip(
            tabla_compuesta[col_name + "_a"],
            tabla_compuesta[col_name + "_b"],
        )
    ]
    scores = [sum(lista) / len(lista) if lista else 0.0 for lista in scores_por_palabra]
    if fuzzy_flag:
        return tabla_compuesta.with_columns(
            pl.Series("score_similitud", scores),
        )
    else:
        return tabla_compuesta.with_columns(
            pl.Series("score_similitud", scores),
        ).drop("score_join")


def limpiar(texto: str) -> str:
    texto = "".join(
        c
        for c in unicodedata.normalize("NFD", texto)
        if unicodedata.category(c) != "Mn"
    )
    return " ".join(w for w in texto.lower().strip().split() if w not in _STOPWORDS_ES)


def comparacion_actividad_economica() -> MetadatosComparacion:
    con = _catalogo()
    tabla_sri = (
        con.execute("SELECT * FROM base_rucs_sri WHERE matriz = 1")
        .pl()
        .with_columns(pl.col("id_establecimiento").cast(pl.Int64))
    )
    con.close()

    con = _catalogo()
    tabla_catastro = (
        con.execute("SELECT * FROM base_rucs_catastro")
        .pl()
        .with_columns(pl.col("id_establecimiento").cast(pl.Int64))
    )
    con.close()

    id_establecimientos_bendo_analisis = tabla_sri["id_establecimiento"].to_list()

    tabla_catastro = tabla_catastro.filter(
        pl.col("id_establecimiento").is_in(id_establecimientos_bendo_analisis)
    )

    id_establecimientos_bendo_catastro = tabla_catastro["id_establecimiento"].to_list()

    tabla_resultados = comparar_fuzzy(
        tabla_sri,
        tabla_catastro,
        col_name="actividad_economica",
        subset_join=["id_establecimiento"],
        fuzzy_flag=False,
    )

    set_catastro = set(id_establecimientos_bendo_catastro)
    registros_sri_no_catastro = [
        id_ for id_ in id_establecimientos_bendo_analisis if id_ not in set_catastro
    ]

    return MetadatosComparacion(
        registros_sri=len(tabla_sri),
        registros_catastro=len(tabla_catastro),
        registros_comunes=len(tabla_resultados),
        registros_sri_no_catastro=registros_sri_no_catastro,
        columnas_cruce=["id_establecimiento"],
        columna_comparar="actividad_economica",
        registros_comunes_sin_columna=len(
            tabla_resultados.filter(pl.col("actividad_economica_b").is_null())
        ),
        promedio_cruce=tabla_resultados.select("score_similitud").mean().item(),
        porcentaje_desactualizacion=len(
            tabla_resultados.filter(pl.col("score_similitud") <= 70.0)
        )
        / len(tabla_resultados),
        tabla=tabla_resultados,
    )


def comparacion_ciiu_nivel6() -> MetadatosCiiu:
    con = _catalogo()

    # Buffer A: actividades de bendo sin clasificar
    buf_sri = (
        con.execute(
            "SELECT DISTINCT brs.actividad_economica FROM base_rucs_sri brs;"
            #             " JOIN rucs_bendo rb ON brs.numero_ruc = rb.numero_ruc"
            #             " WHERE brs.actividad_economica IS NOT NULL"
            #             " AND brs.actividad_economica NOT IN ("
            #             "   SELECT actividad_economica FROM actividades_economicas_clasificadas"
            #             " )"
            # "SELECT DISTINCT actividad_economica FROM base_rucs_sri"
            # " WHERE actividad_economica IS NOT NULL"
        )
        .pl()
        .rename({"actividad_economica": "descripcion"})
        .with_columns(pl.lit("_").alias("_k"))
    )

    # Buffer B: descripciones CIIU nivel 6
    buf_ciiu = (
        con.execute("SELECT descripcion FROM ciiu_clasificado WHERE nivel = 6")
        .pl()
        .with_columns(pl.lit("_").alias("_k"))
    )

    con.close()

    total_ciiu = len(buf_ciiu)
    n_actividades_sri = len(buf_sri)

    # La clave constante "_k" fuerza un cross-join en el inner join de comparar_fuzzy
    # (todos los registros comparten el mismo valor), anti queda vacío
    todos_pares = comparar_fuzzy(
        buf_sri,
        buf_ciiu,
        col_name="descripcion",
        subset_join=["_k"],
        fuzzy_flag=True,
    )

    # Mejor match CIIU por actividad única del SRI
    tabla_resultados = todos_pares.sort("score_similitud", descending=True).unique(
        subset=["descripcion_a"], keep="first"
    )

    umbral = 70.0
    tabla_sin_match = (
        tabla_resultados.filter(pl.col("score_similitud") < umbral)
        .select(["descripcion_a", "descripcion_b", "score_similitud"])
        .rename({"descripcion_a": "actividad_economica", "descripcion_b": "mejor_ciiu"})
    )

    tabla_completa = tabla_resultados.select(
        ["descripcion_a", "descripcion_b", "score_similitud"]
    ).rename({"descripcion_a": "actividad_economica", "descripcion_b": "mejor_ciiu"})

    return MetadatosCiiu(
        total_ciiu=total_ciiu,
        actividades_sri=n_actividades_sri,
        actividades_con_match=len(
            tabla_resultados.filter(pl.col("score_similitud") >= umbral)
        ),
        tabla_sin_match=tabla_sin_match,
        tabla=tabla_completa,
    )
