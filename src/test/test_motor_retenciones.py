import sys
import os
import pytest
import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from asignacion_retenciones.calculo_retenciones import (
    aplicar_retencion_iva,
    aplicar_retencion_renta,
)


def test_falta_columna_tabla_retencion_iva():
    tb = pl.DataFrame({"tipo_proveedor": ["A", "B"]})
    reglas = pl.DataFrame({"tipo_proveedor": ["A", "B"]})  # sin porcentaje_retencion_iva
    with pytest.raises(ValueError, match="porcentaje_retencion_iva"):
        aplicar_retencion_iva(tb, reglas)


def test_columna_inexistente_en_tb_iva():
    tb = pl.DataFrame({"tipo_proveedor": ["A", "B"]})
    reglas = pl.DataFrame({
        "columna_inexistente": ["A", "B"],
        "porcentaje_retencion_iva": [10.0, 20.0],
    })
    with pytest.raises(ValueError, match="retencion_iva"):
        aplicar_retencion_iva(tb, reglas)


def test_falta_columna_tabla_retencion_renta():
    tb = pl.DataFrame({"tipo_proveedor": ["A", "B"]})
    reglas = pl.DataFrame({"tipo_proveedor": ["A", "B"]})  # sin porcentaje_retencion_renta
    with pytest.raises(ValueError, match="porcentaje_retencion_renta"):
        aplicar_retencion_renta(tb, reglas)


def test_columna_inexistente_en_tb_renta():
    tb = pl.DataFrame({"tipo_proveedor": ["A", "B"]})
    reglas = pl.DataFrame({
        "columna_inexistente": ["A", "B"],
        "porcentaje_retencion_renta": [10.0, 20.0],
    })
    with pytest.raises(ValueError, match="retencion_renta"):
        aplicar_retencion_renta(tb, reglas)
