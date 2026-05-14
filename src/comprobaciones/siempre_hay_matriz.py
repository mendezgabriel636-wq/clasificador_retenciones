import polars as pl
from typing import Tuple

ruta_sri = "~/Proyectos/BASES_COMERCIALES/projects/data_fact/extraer_data_fact/backups/base_rucs_sri_0.parquet"
rucs_bendo = "~/Proyectos/RETENCIONES/rucs_bendo.csv"


def comprobar_todo_ruc_tiene_matriz() -> Tuple[list[int], list[int]]:
    numeros_rucs = pl.read_csv(rucs_bendo).select("numero_ruc")["numero_ruc"].to_list()
    ruc_sin_matriz = (
        pl.read_parquet(ruta_sri)
        .select(["numero_ruc", "matriz"])
        .filter(pl.col("numero_ruc").is_in(numeros_rucs))
        .group_by("numero_ruc")
        .agg(pl.col("matriz").sum().alias("tiene_matriz"))
        .filter(pl.col("tiene_matriz") == 0)
        .select("numero_ruc")["numero_ruc"]
        .to_list()
    )

    return numeros_rucs, ruc_sin_matriz
