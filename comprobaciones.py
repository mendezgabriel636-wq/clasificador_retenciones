from src.comprobaciones.sri_catastro_comparacion import (
    #    comparacion_actividad_economica,
    comparacion_ciiu_nivel6,
)

if __name__ == "__main__":
    #     print("=" * 50)
    #     print("SRI vs CATASTRO — actividad_economica")
    #     print("=" * 50)
    #     sri_catastro = comparacion_actividad_economica()
    #     print(sri_catastro.resumen())
    #     print(sri_catastro.tabla)

    print("=" * 50)
    print("CIIU NIVEL 6 vs CATASTRO — descripción")
    print("=" * 50)
    ciiu = comparacion_ciiu_nivel6()
    print(ciiu.resumen())
    print(ciiu.tabla_sin_match)
    ciiu.to_csv("resultados.csv")
