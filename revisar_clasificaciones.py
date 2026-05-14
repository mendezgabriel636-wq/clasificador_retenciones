import unicodedata
import duckdb
import polars as pl
from rapidfuzz import fuzz, process


def limpiar(texto: str) -> str:
    if not texto:
        return ""
    texto = unicodedata.normalize("NFD", texto.lower())
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto.strip()


def main():
    con = duckdb.connect("catalogo_retenciones.duckdb", read_only=True)
    act_class = con.execute(
        "SELECT actividad_economica, codigo_ciiu FROM actividades_economicas_clasificadas"
    ).pl()
    ciiu = con.execute("SELECT codigo, descripcion FROM ciiu_nivel6").pl()
    con.close()

    resultados = pl.read_csv("resultados.csv")

    # Joins
    df = act_class.join(resultados, on="actividad_economica", how="left")
    df = df.join(
        ciiu.rename({"codigo": "codigo_ciiu", "descripcion": "descripcion_ciiu_actual"}),
        on="codigo_ciiu",
        how="left",
    )

    # Filtrar score != 100
    df_review = df.filter(pl.col("score_similitud") != 100)
    print(f"Registros con score != 100: {len(df_review)}")

    # Preparar lookup de ciiu para búsqueda rápida
    ciiu_descs = ciiu["descripcion"].to_list()
    ciiu_codigos = ciiu["codigo"].to_list()
    ciiu_clean = [limpiar(d) for d in ciiu_descs]
    ciiu_map = dict(zip(ciiu_clean, ciiu_codigos))
    ciiu_clean_to_orig = dict(zip(ciiu_clean, ciiu_descs))

    resultados_rows = []

    for row in df_review.iter_rows(named=True):
        actividad = row["actividad_economica"] or ""
        codigo_actual = row["codigo_ciiu"] or ""
        desc_actual = row["descripcion_ciiu_actual"] or ""
        score_orig = row["score_similitud"]

        actividad_clean = limpiar(actividad)
        desc_actual_clean = limpiar(desc_actual)

        # Score entre la actividad y su CIIU actual
        score_vs_actual = fuzz.token_set_ratio(actividad_clean, desc_actual_clean) if desc_actual_clean else 0

        # Mejor match en todo ciiu_nivel6
        match = process.extractOne(actividad_clean, ciiu_clean, scorer=fuzz.token_set_ratio)
        if match is None:
            continue
        best_clean, score_sugerido, _ = match
        best_codigo = ciiu_map[best_clean]
        best_desc = ciiu_clean_to_orig[best_clean]

        # Criterio de discrepancia: score actual bajo Y sugerencia mejora claramente
        es_diferente = best_codigo != codigo_actual
        mejora_significativa = score_sugerido > score_vs_actual + 20
        match_actual_bajo = score_vs_actual < 50

        if match_actual_bajo and es_diferente and mejora_significativa:
            resultados_rows.append({
                "actividad_economica": actividad,
                "codigo_ciiu_actual": codigo_actual,
                "descripcion_ciiu_actual": desc_actual,
                "score_vs_actual": score_vs_actual,
                "codigo_ciiu_sugerido": best_codigo,
                "descripcion_ciiu_sugerida": best_desc,
                "score_sugerido": score_sugerido,
                "score_similitud_original": score_orig,
            })

    if not resultados_rows:
        print("No se encontraron discrepancias.")
        return

    df_out = pl.DataFrame(resultados_rows)
    df_out = df_out.sort("score_vs_actual")
    df_out.write_csv("revision_clasificaciones.csv")
    print(f"Discrepancias encontradas: {len(df_out)}")
    print(f"CSV generado: revision_clasificaciones.csv")
    print("\nMuestra (peores primeros):")
    print(df_out.head(10).select([
        "actividad_economica",
        "codigo_ciiu_actual",
        "score_vs_actual",
        "codigo_ciiu_sugerido",
        "score_sugerido",
    ]))


if __name__ == "__main__":
    main()
