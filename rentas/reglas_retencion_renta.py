# =============================================================================
# REGLAS DE RETENCIÓN DE RENTA - BENDO ECUADOR
# =============================================================================
# Resolución NAC-DGERCGC26-00000009 (vigente desde 01-marzo-2026)
# 
# ENTRADA: Tabla de proveedores con las siguientes columnas:
#   - ruc
#   - tipo_contribuyente (PERSONA NATURAL / SOCIEDAD)
#   - clase_contribuyente (ESPECIAL / OTROS / RIMPE)
#   - categoria (EMPRENDEDOR / NEGOCIO POPULAR / REGIMEN GENERAL)
#   - cod_ciiu / codigo_ciiu
#   - tipo_concepto_ir (ya clasificado desde tabla CIIU)
#   - obligado_llevar_contabilidad (SI / NO)
#   - agente_retencion (SI / NO)
#   - contribuyente_especial (SI / NO)
#
# SALIDA: codigo_sri_renta, porcentaje_renta, descripcion_renta
# =============================================================================

import pandas as pd
from typing import Tuple, Optional


# =============================================================================
# TABLA: CONCEPTOS_RETENCION_SRI (84 códigos residentes)
# ⚠️ ACTUALIZAR cuando cambie la normativa
# =============================================================================

CONCEPTOS_SRI = {
    # --- 0% ---
    "332": {"porcentaje": 0.0, "descripcion": "No sujeto a retención / RIMPE Negocio Popular"},
    "332B": {"porcentaje": 0.0, "descripcion": "Energía eléctrica (exento)"},
    "332C": {"porcentaje": 0.0, "descripcion": "Transporte público de pasajeros"},
    "332D": {"porcentaje": 0.0, "descripcion": "Transporte pasajeros/carga internacional"},
    "332E": {"porcentaje": 0.0, "descripcion": "Valores cooperativas transporte a socios"},
    "332F": {"porcentaje": 0.0, "descripcion": "Compraventa divisas distintas USD"},
    "332G": {"porcentaje": 0.0, "descripcion": "Pagos con tarjeta de crédito"},
    "332H": {"porcentaje": 0.0, "descripcion": "Pago exterior tarjeta crédito (RECAP)"},
    "332I": {"porcentaje": 0.0, "descripcion": "Pago convenio débito (Clientes IFIs)"},
    "323E2": {"porcentaje": 0.0, "descripcion": "Rendimientos financieros: depósito plazo fijo exentos"},
    "323N": {"porcentaje": 0.0, "descripcion": "Rendimientos financieros: títulos renta fija exentos"},
    "323O": {"porcentaje": 0.0, "descripcion": "Intereses a bancos y entidades financieras"},
    "323R": {"porcentaje": 0.0, "descripcion": "Otros intereses y rendimientos exentos"},
    "323T": {"porcentaje": 0.0, "descripcion": "Rendimientos deuda pública ecuatoriana"},
    "323U": {"porcentaje": 0.0, "descripcion": "Rendimientos títulos obligaciones exentos"},
    "328": {"porcentaje": 0.0, "descripcion": "Dividendos distribuidos a sociedades residentes"},
    "329": {"porcentaje": 0.0, "descripcion": "Dividendos distribuidos a fideicomisos residentes"},
    "331": {"porcentaje": 0.0, "descripcion": "Dividendos en acciones (capitalización)"},
    "3250": {"porcentaje": 0.0, "descripcion": "Dividendos exentos"},
    
    # --- 1% ---
    "310": {"porcentaje": 1.0, "descripcion": "Transporte privado pasajeros / público y privado carga"},
    "312A": {"porcentaje": 1.0, "descripcion": "Compras al PRODUCTOR agropecuario"},
    "343": {"porcentaje": 1.0, "descripcion": "RIMPE Emprendedor"},
    
    # --- 1.75% ---
    "312C": {"porcentaje": 1.75, "descripcion": "Compras al COMERCIALIZADOR agropecuario"},
    
    # --- 2% ---
    "312": {"porcentaje": 2.0, "descripcion": "Transferencia de bienes muebles corporales"},
    "343A": {"porcentaje": 2.0, "descripcion": "Energía eléctrica"},
    "343B": {"porcentaje": 2.0, "descripcion": "Construcción obra material inmueble"},
    "343C": {"porcentaje": 2.0, "descripcion": "Recepción botellas plásticas PET"},
    "344A": {"porcentaje": 2.0, "descripcion": "Pago tarjeta crédito/débito reportada"},
    "344B": {"porcentaje": 2.0, "descripcion": "Sustancias minerales"},
    "322": {"porcentaje": 2.0, "descripcion": "Seguros y reaseguros (primas)"},
    "319": {"porcentaje": 2.0, "descripcion": "Arrendamiento mercantil (leasing) - sociedades"},
    "334": {"porcentaje": 2.0, "descripcion": "Enajenación derechos representativos capital"},
    "324A": {"porcentaje": 2.0, "descripcion": "Intereses operaciones crédito entre IFIs"},
    "324B": {"porcentaje": 2.0, "descripcion": "Inversiones entre IFIs"},
    "324C": {"porcentaje": 2.0, "descripcion": "Pagos BCE y depósitos centralizados"},
    
    # --- 3% ---
    "307": {"porcentaje": 3.0, "descripcion": "Servicios predomina mano de obra"},
    "309": {"porcentaje": 3.0, "descripcion": "Medios comunicación y publicidad"},
    "311": {"porcentaje": 3.0, "descripcion": "Liquidación compra (rusticidad)"},
    "323": {"porcentaje": 3.0, "descripcion": "Rendimientos financieros (No IFIs)"},
    "323A": {"porcentaje": 3.0, "descripcion": "Rendimientos: depósitos Cta Corriente"},
    "323B1": {"porcentaje": 3.0, "descripcion": "Rendimientos: depósitos Cta Ahorros Sociedades"},
    "323E": {"porcentaje": 3.0, "descripcion": "Rendimientos: depósito plazo fijo gravados"},
    "323F": {"porcentaje": 3.0, "descripcion": "Rendimientos: operaciones reporto"},
    "323G": {"porcentaje": 3.0, "descripcion": "Inversiones rendimientos distintos IFIs"},
    "323H": {"porcentaje": 3.0, "descripcion": "Rendimientos: obligaciones"},
    "323I": {"porcentaje": 3.0, "descripcion": "Rendimientos: bonos convertibles acciones"},
    "323M": {"porcentaje": 3.0, "descripcion": "Rendimientos: títulos renta fija gravados"},
    "323P": {"porcentaje": 3.0, "descripcion": "Intereses sector público"},
    "323Q": {"porcentaje": 3.0, "descripcion": "Otros intereses y rendimientos gravados"},
    "323S": {"porcentaje": 3.0, "descripcion": "Pagos BCE depósitos centralizados"},
    "3440": {"porcentaje": 3.0, "descripcion": "Otras retenciones 3% (residual)"},
    "340": {"porcentaje": 3.0, "descripcion": "Impuesto único exportación banano"},
    
    # --- 5% ---
    "303A": {"porcentaje": 5.0, "descripcion": "Servicios profesionales - SOCIEDADES"},
    "3482": {"porcentaje": 5.0, "descripcion": "Comisiones a sociedades"},
    
    # --- 10% ---
    "303": {"porcentaje": 10.0, "descripcion": "Honorarios profesionales - personas naturales"},
    "304": {"porcentaje": 10.0, "descripcion": "Servicios prevalece intelecto - personas naturales"},
    "304A": {"porcentaje": 10.0, "descripcion": "Comisiones a personas naturales"},
    "304B": {"porcentaje": 10.0, "descripcion": "Notarios y registradores"},
    "304C": {"porcentaje": 10.0, "descripcion": "Deportistas, entrenadores, árbitros"},
    "304D": {"porcentaje": 10.0, "descripcion": "Artistas"},
    "304E": {"porcentaje": 10.0, "descripcion": "Docencia"},
    "308": {"porcentaje": 10.0, "descripcion": "Uso imagen/renombre (influencers)"},
    "320": {"porcentaje": 10.0, "descripcion": "Arrendamiento bienes inmuebles"},
    "314A": {"porcentaje": 10.0, "descripcion": "Regalías franquicias - personas naturales"},
    "314B": {"porcentaje": 10.0, "descripcion": "Cánones, derechos autor - personas naturales"},
    "314C": {"porcentaje": 10.0, "descripcion": "Regalías franquicias - sociedades"},
    "314D": {"porcentaje": 10.0, "descripcion": "Cánones, derechos autor - sociedades"},
    "333": {"porcentaje": 10.0, "descripcion": "Ganancia enajenación derechos capital"},
    
    # --- 15% ---
    "335": {"porcentaje": 15.0, "descripcion": "Loterías, rifas, apuestas"},
    "3480": {"porcentaje": 15.0, "descripcion": "Operadores turismo receptivo"},
    
    # --- 25% ---
    "325": {"porcentaje": 25.0, "descripcion": "Anticipo dividendos"},
    "325A": {"porcentaje": 25.0, "descripcion": "Préstamos accionistas residentes"},
    
    # --- Porcentajes variables ---
    "326": {"porcentaje": 13.0, "descripcion": "Dividendos distribuidos (12 o 14%)"},
    "327": {"porcentaje": 13.0, "descripcion": "Dividendos a personas naturales (12 o 14%)"},
    "336": {"porcentaje": 0.2, "descripcion": "Venta combustibles a comercializadoras (2/mil)"},
    "337": {"porcentaje": 0.3, "descripcion": "Venta combustibles a distribuidores (3/mil)"},
    "338": {"porcentaje": 1.5, "descripcion": "Producción/venta banano (1 a 2%)"},
    "350": {"porcentaje": 1.75, "descripcion": "Otras autorretenciones (1.50 o 1.75%)"},
    "346": {"porcentaje": 3.0, "descripcion": "Otras retenciones otros porcentajes"},
    "346A": {"porcentaje": 3.0, "descripcion": "Otras ganancias capital"},
    "346B": {"porcentaje": 0.0, "descripcion": "Donaciones (según Art.36 LRTI)"},
    "346C": {"porcentaje": 5.0, "descripcion": "Autorretención exportación concentrados"},
    "346D": {"porcentaje": 5.0, "descripcion": "Autorretención comercialización minerales"},
    "3481": {"porcentaje": 3.0, "descripcion": "Autorretenciones Grandes Contribuyentes"},
}


# =============================================================================
# MAPEO: tipo_concepto_ir → codigo_sri
# Incluye diferenciación por tipo_contribuyente (PN vs SOC)
# =============================================================================

def obtener_codigo_sri(tipo_concepto_ir: str, tipo_contribuyente: str) -> str:
    """
    Dado un tipo_concepto_ir y tipo_contribuyente, retorna el codigo_sri.
    
    Parámetros:
    - tipo_concepto_ir: valor de la columna tipo_concepto_ir de la tabla
    - tipo_contribuyente: "PERSONA NATURAL" o "SOCIEDAD"
    
    Retorna:
    - codigo_sri: código oficial del SRI
    """
    # Normalizar
    tipo_concepto = str(tipo_concepto_ir).upper().strip() if tipo_concepto_ir else ""
    tipo_contrib = str(tipo_contribuyente).upper().strip() if tipo_contribuyente else ""
    
    es_pn = "NATURAL" in tipo_contrib or tipo_contrib == "PN"
    
    # Mapeo tipo_concepto → codigo_sri
    # Para conceptos con diferencia PN/SOC, se usa condicional
    MAPEO = {
        # Bienes
        'BIEN_MUEBLE': '312',
        'BIEN MUEBLE': '312',
        'BIENES': '312',
        'BIEN_AGROPECUARIO': '312C',
        'BIEN AGROPECUARIO': '312C',
        'AGROPECUARIO': '312C',
        'BIEN_AGROPECUARIO_PRODUCTOR': '312A',
        'BIEN_AGROPECUARIO_COMERCIALIZADOR': '312C',
        'MINERALES': '344B',
        'ENERGIA': '343A',
        'ENERGÍA': '343A',
        'CONSTRUCCION': '343B',
        'CONSTRUCCIÓN': '343B',
        'RECICLAJE': '343C',
        
        # Transporte
        'TRANSPORTE': '310',
        'TRANSPORTE_PUBLICO': '332C',
        'TRANSPORTE PUBLICO': '332C',
        'TRANSPORTE_PUBLICO_PASAJEROS': '332C',
        
        # Servicios mano de obra
        'SERVICIO_MANO_OBRA': '307',
        'SERVICIO MANO OBRA': '307',
        'SERVICIO_MANO_DE_OBRA': '307',
        'MANO_OBRA': '307',
        'MANO OBRA': '307',
        
        # Medios y publicidad
        'MEDIOS_COMUNICACION': '309',
        'MEDIOS COMUNICACION': '309',
        'MEDIOS_COMUNICACIÓN': '309',
        'PUBLICIDAD': '309',
        
        # Liquidación compra
        'LIQUIDACION_COMPRA': '311',
        'LIQUIDACION COMPRA': '311',
        'LIQUIDACIÓN_COMPRA': '311',
        
        # Financiero
        'SEGUROS': '322',
        'FINANCIERO_BANCO': '323O',
        'FINANCIERO BANCO': '323O',
        'BANCO': '323O',
        'FINANCIERO_OTROS': '3440',
        'FINANCIERO OTROS': '3440',
        'FINANCIERO_IFI': '324A',
        'RENDIMIENTOS_FINANCIEROS': '323',
        'RENDIMIENTOS FINANCIEROS': '323',
        
        # Arrendamientos
        'ARRENDAMIENTO_INMUEBLE': '320',
        'ARRENDAMIENTO INMUEBLE': '320',
        'ARRENDAMIENTO_BIENES_INMUEBLES': '320',
        
        # Imagen y entretenimiento
        'IMAGEN_RENOMBRE': '308',
        'IMAGEN RENOMBRE': '308',
        'IMAGEN': '308',
        'INFLUENCER': '308',
        'DEPORTISTAS': '304C',
        'DEPORTISTA': '304C',
        'ARTISTAS': '304D',
        'ARTISTA': '304D',
        'NOTARIOS': '304B',
        'NOTARIO': '304B',
        
        # Regalías
        'REGALIAS': '314B' if es_pn else '314D',
        'REGALÍAS': '314B' if es_pn else '314D',
        
        # Dividendos
        'DIVIDENDOS': '328',
        
        # Loterías
        'LOTERIAS': '335',
        'LOTERÍAS': '335',
        
        # Combustibles
        'COMBUSTIBLES': '336',
        
        # Banano
        'BANANO': '338',
        
        # Otros
        'ENAJENACION_DERECHOS': '334',
        'DONACIONES': '346B',
        'PAGO_TARJETA': '332G',
        'OPERADORES_TURISMO': '3480',
        'AUTORRETENCION': '350',
        'SECTOR_PUBLICO': '3440',
        'SECTOR PUBLICO': '3440',
        'SECTOR_PÚBLICO': '3440',
        'DOMESTICO': '332',
        'DOMÉSTICO': '332',
        'EXTRATERRITORIAL': '332',
        'NO_SUJETO': '332',
        'NO SUJETO': '332',
        'RESIDUAL': '3440',
    }
    
    # Conceptos con DIFERENCIA PN vs SOC (regla especial)
    MAPEO_DIFERENCIADO = {
        'SERVICIO_PROFESIONAL': ('303', '303A'),      # PN=10%, SOC=5%
        'SERVICIO PROFESIONAL': ('303', '303A'),
        'SERVICIOS_PROFESIONALES': ('303', '303A'),
        'SERVICIOS PROFESIONALES': ('303', '303A'),
        'PROFESIONAL': ('303', '303A'),
        
        'SERVICIO_INTELECTO': ('304', '303A'),        # PN=10%, SOC=5%
        'SERVICIO INTELECTO': ('304', '303A'),
        'SERVICIOS_INTELECTO': ('304', '303A'),
        'INTELECTO': ('304', '303A'),
        
        'COMISIONES': ('304A', '3482'),               # PN=10%, SOC=5%
        'COMISION': ('304A', '3482'),
        'COMISIÓN': ('304A', '3482'),
        
        'EDUCACION': ('304E', '303A'),                # PN=10%, SOC=5%
        'EDUCACIÓN': ('304E', '303A'),
        'DOCENCIA': ('304E', '303A'),
        
        'ARRENDAMIENTO_MERCANTIL': ('3440', '319'),   # PN=3%, SOC=2%
        'ARRENDAMIENTO MERCANTIL': ('3440', '319'),
        'LEASING': ('3440', '319'),
    }
    
    # Buscar primero en mapeo diferenciado
    if tipo_concepto in MAPEO_DIFERENCIADO:
        codigo_pn, codigo_soc = MAPEO_DIFERENCIADO[tipo_concepto]
        return codigo_pn if es_pn else codigo_soc
    
    # Buscar en mapeo simple
    if tipo_concepto in MAPEO:
        return MAPEO[tipo_concepto]
    
    # Si no encuentra, retornar residual
    return '3440'


# =============================================================================
# FUNCIÓN PRINCIPAL: calcular_retencion_renta
# =============================================================================

def calcular_retencion_renta(row: pd.Series) -> Tuple[str, float, str, str]:
    """
    Calcula la retención de renta para una fila de la tabla de proveedores.
    
    Parámetros:
    - row: fila del DataFrame con las columnas del proveedor
    
    Retorna:
    - Tupla: (codigo_sri, porcentaje, descripcion, base_calculo)
    """
    
    # Extraer valores de la fila (normalizar)
    contribuyente_especial = str(row.get('contribuyente_especial', '')).upper().strip()
    clase_contribuyente = str(row.get('clase_contribuyente', '')).upper().strip()
    categoria = str(row.get('categoria', '')).upper().strip()
    tipo_contribuyente = str(row.get('tipo_contribuyente', '')).upper().strip()
    tipo_concepto_ir = str(row.get('tipo_concepto_ir', '')).upper().strip()
    
    # =========================================================================
    # REGLA 1: ¿Es Contribuyente Especial?
    # =========================================================================
    # Columnas binarias: 1 = SI, 0 = NO
    es_contribuyente_especial = (
        contribuyente_especial in ['1', '1.0', 'SI', 'SÍ', 'S', 'TRUE', 'VERDADERO'] or
        (isinstance(row.get('contribuyente_especial'), (int, float)) and row.get('contribuyente_especial') == 1)
    )
    
    if es_contribuyente_especial:
        return ('N/A', 0.0, 'NO RETENER - Contribuyente Especial', 'Art.92 LORTI')
    
    if clase_contribuyente == 'ESPECIAL':
        return ('N/A', 0.0, 'NO RETENER - Contribuyente Especial', 'Art.92 LORTI')
    
    # =========================================================================
    # REGLA 2: ¿Es RIMPE Negocio Popular?
    # =========================================================================
    es_rimpe = clase_contribuyente == 'RIMPE' or 'RIMPE' in clase_contribuyente
    es_negocio_popular = 'NEGOCIO' in categoria and 'POPULAR' in categoria
    
    if es_rimpe and es_negocio_popular:
        concepto = CONCEPTOS_SRI['332']
        return ('332', concepto['porcentaje'], concepto['descripcion'], 'RIMPE Negocio Popular')
    
    # =========================================================================
    # REGLA 3: ¿Es RIMPE Emprendedor?
    # =========================================================================
    es_emprendedor = 'EMPRENDEDOR' in categoria
    
    if es_rimpe and es_emprendedor:
        concepto = CONCEPTOS_SRI['343']
        return ('343', concepto['porcentaje'], concepto['descripcion'], 'RIMPE Emprendedor')
    
    # =========================================================================
    # REGLA 4: Clasificar por tipo_concepto_ir
    # =========================================================================
    
    # Obtener codigo_sri según tipo_concepto_ir y tipo_contribuyente
    codigo_sri = obtener_codigo_sri(tipo_concepto_ir, tipo_contribuyente)
    
    # Obtener porcentaje y descripción del catálogo
    if codigo_sri in CONCEPTOS_SRI:
        concepto = CONCEPTOS_SRI[codigo_sri]
        return (
            codigo_sri, 
            concepto['porcentaje'], 
            concepto['descripcion'],
            f"{tipo_concepto_ir} → {codigo_sri}"
        )
    
    # Fallback: residual 3%
    concepto = CONCEPTOS_SRI['3440']
    return ('3440', concepto['porcentaje'], concepto['descripcion'], 'Residual')


# =============================================================================
# FUNCIÓN: Aplicar a DataFrame completo
# =============================================================================

def aplicar_retencion_renta(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica el cálculo de retención de renta a todo el DataFrame.
    
    Parámetros:
    - df: DataFrame con las columnas de proveedores
    
    Retorna:
    - DataFrame con columnas adicionales:
        - codigo_sri_renta
        - porcentaje_renta
        - descripcion_renta
        - base_calculo_renta
    """
    
    # Aplicar función a cada fila
    resultados = df.apply(calcular_retencion_renta, axis=1)
    
    # Separar tupla en columnas
    df['codigo_sri_renta'] = resultados.apply(lambda x: x[0])
    df['porcentaje_renta'] = resultados.apply(lambda x: x[1])
    df['descripcion_renta'] = resultados.apply(lambda x: x[2])
    df['base_calculo_renta'] = resultados.apply(lambda x: x[3])
    
    return df


# =============================================================================
# EJEMPLO DE USO
# =============================================================================

if __name__ == "__main__":
    
    # Crear datos de prueba
    datos_prueba = [
        {
            'ruc': '1790123456001',
            'tipo_contribuyente': 'SOCIEDAD',
            'clase_contribuyente': 'OTROS',
            'categoria': 'REGIMEN GENERAL',
            'cod_ciiu': 'M6920',
            'tipo_concepto_ir': 'SERVICIO_PROFESIONAL',
            'contribuyente_especial': 'SI'
        },
        {
            'ruc': '1712345678001',
            'tipo_contribuyente': 'PERSONA NATURAL',
            'clase_contribuyente': 'RIMPE',
            'categoria': 'NEGOCIO POPULAR',
            'cod_ciiu': 'G4711',
            'tipo_concepto_ir': 'BIEN_MUEBLE',
            'contribuyente_especial': 'NO'
        },
        {
            'ruc': '1798765432001',
            'tipo_contribuyente': 'PERSONA NATURAL',
            'clase_contribuyente': 'RIMPE',
            'categoria': 'EMPRENDEDOR',
            'cod_ciiu': 'I5610',
            'tipo_concepto_ir': 'SERVICIO_MANO_OBRA',
            'contribuyente_especial': 'NO'
        },
        {
            'ruc': '1701234567001',
            'tipo_contribuyente': 'PERSONA NATURAL',
            'clase_contribuyente': 'OTROS',
            'categoria': 'REGIMEN GENERAL',
            'cod_ciiu': 'M6920',
            'tipo_concepto_ir': 'SERVICIO_PROFESIONAL',
            'contribuyente_especial': 'NO'
        },
        {
            'ruc': '1790987654001',
            'tipo_contribuyente': 'SOCIEDAD',
            'clase_contribuyente': 'OTROS',
            'categoria': 'REGIMEN GENERAL',
            'cod_ciiu': 'M6920',
            'tipo_concepto_ir': 'SERVICIO_PROFESIONAL',
            'contribuyente_especial': 'NO'
        },
        {
            'ruc': '1756789012001',
            'tipo_contribuyente': 'PERSONA NATURAL',
            'clase_contribuyente': 'OTROS',
            'categoria': 'REGIMEN GENERAL',
            'cod_ciiu': 'J6201',
            'tipo_concepto_ir': 'SERVICIO_INTELECTO',
            'contribuyente_especial': 'NO'
        },
        {
            'ruc': '1791234567001',
            'tipo_contribuyente': 'SOCIEDAD',
            'clase_contribuyente': 'OTROS',
            'categoria': 'REGIMEN GENERAL',
            'cod_ciiu': 'J6201',
            'tipo_concepto_ir': 'SERVICIO_INTELECTO',
            'contribuyente_especial': 'NO'
        },
        {
            'ruc': '1792345678001',
            'tipo_contribuyente': 'SOCIEDAD',
            'clase_contribuyente': 'OTROS',
            'categoria': 'REGIMEN GENERAL',
            'cod_ciiu': 'G4752',
            'tipo_concepto_ir': 'BIEN_MUEBLE',
            'contribuyente_especial': 'NO'
        },
        {
            'ruc': '1793456789001',
            'tipo_contribuyente': 'SOCIEDAD',
            'clase_contribuyente': 'OTROS',
            'categoria': 'REGIMEN GENERAL',
            'cod_ciiu': 'H4923',
            'tipo_concepto_ir': 'TRANSPORTE',
            'contribuyente_especial': 'NO'
        },
        {
            'ruc': '1794567890001',
            'tipo_contribuyente': 'PERSONA NATURAL',
            'clase_contribuyente': 'OTROS',
            'categoria': 'REGIMEN GENERAL',
            'cod_ciiu': 'P8510',
            'tipo_concepto_ir': 'EDUCACION',
            'contribuyente_especial': 'NO'
        },
        {
            'ruc': '1795678901001',
            'tipo_contribuyente': 'SOCIEDAD',
            'clase_contribuyente': 'OTROS',
            'categoria': 'REGIMEN GENERAL',
            'cod_ciiu': 'P8510',
            'tipo_concepto_ir': 'EDUCACION',
            'contribuyente_especial': 'NO'
        },
    ]
    
    # Crear DataFrame
    df = pd.DataFrame(datos_prueba)
    
    # Aplicar cálculo
    df_resultado = aplicar_retencion_renta(df)
    
    # Mostrar resultados
    print("=" * 120)
    print("RESULTADOS DEL CÁLCULO DE RETENCIÓN DE RENTA")
    print("=" * 120)
    
    columnas_mostrar = ['ruc', 'tipo_contribuyente', 'clase_contribuyente', 'categoria', 
                        'tipo_concepto_ir', 'codigo_sri_renta', 'porcentaje_renta', 'descripcion_renta']
    
    for idx, row in df_resultado.iterrows():
        print(f"\n--- Proveedor {idx + 1} ---")
        print(f"RUC: {row['ruc']}")
        print(f"Tipo: {row['tipo_contribuyente']} | Clase: {row['clase_contribuyente']} | Categoría: {row['categoria']}")
        print(f"Tipo concepto IR: {row['tipo_concepto_ir']}")
        print(f"➜ Código SRI: {row['codigo_sri_renta']} | Porcentaje: {row['porcentaje_renta']}%")
        print(f"➜ {row['descripcion_renta']}")
        print(f"➜ Base: {row['base_calculo_renta']}")

