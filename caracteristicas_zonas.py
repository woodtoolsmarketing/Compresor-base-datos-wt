# -*- coding: utf-8 -*-
"""
Tabla característica telefónica (código de área, SIN el 0) -> zona(s) WoodTools.

Cómo leerla:
  zonas       zonas del catálogo a las que puede pertenecer un cliente con esa característica.
              La primera es la principal. Si hay más de una, el motor usa el texto de la fila
              para desempatar; si no hay texto y defecto=True, usa la principal.
  defecto     True cuando la característica es compartida pero una localidad domina claramente
              (ej: 299 = Neuquén capital 148, salvo que la fila diga Cutral Có 152).
  decision    True cuando la localidad NO está en el catálogo oficial y la zona es una deducción
              geográfica: conviene que el negocio la confirme (o la ajuste en caracteristicas_zonas.json).

Para corregir una entrada sin recompilar, crear al lado del programa un caracteristicas_zonas.json:
  {"caracteristicas": {"2494": ["124"]}, "centrales_amba": {"4201": "102"}, "viejas": {"2293": "249"}}

Compilada el 2026-09-07 a partir del plan de numeración argentino y del catálogo ZONAS_CATALOGO.
"""

# Las cinco zonas que conviven dentro del 011 (AMBA)
ZONAS_AMBA = ["110", "101", "102", "104", "107"]


def _e(zonas, cabecera, localidades, provincia, defecto=False, decision=False, nota=""):
    d = {"zonas": list(zonas), "cabecera": cabecera, "localidades": list(localidades), "provincia": provincia}
    if defecto: d["defecto"] = True
    if decision: d["decision"] = True
    if nota: d["nota"] = nota
    return d

BA, LP, SF, ER, CB, MZ, SJ, SL, NQ, RN, CH, SC, TF = ("Buenos Aires", "La Pampa", "Santa Fe", "Entre Ríos", "Córdoba",
                                                    "Mendoza", "San Juan", "San Luis", "Neuquén", "Río Negro", "Chubut",
                                                    "Santa Cruz", "Tierra del Fuego")

CARACTERISTICAS = {
    # ------------------------------------------------------------------ AMBA y periferia
    "220":  _e(["107", "116"], "Merlo", ["Merlo", "San Antonio de Padua", "Libertad", "Marcos Paz", "General Las Heras"], BA, defecto=True,
               nota="Merlo/Marcos Paz = Oeste; Gral. Las Heras = 116"),
    "230":  _e(["141"], "Pilar", ["Pilar", "Del Viso", "Presidente Derqui", "Manzanares", "Villa Rosa"], BA),
    "237":  _e(["107", "141"], "Moreno", ["Moreno", "Paso del Rey", "Francisco Álvarez", "General Rodríguez"], BA, defecto=True, decision=True,
               nota="Moreno = Oeste (107); General Rodríguez no está en el catálogo, por cercanía a Luján podría ser 141"),
    "348":  _e(["101"], "Escobar", ["Belén de Escobar", "Ingeniero Maschwitz", "Matheu", "Loma Verde"], BA, decision=True,
               nota="Escobar no figura en el catálogo; se asume ZONA NORTE"),
    "3327": _e(["101"], "Garín", ["Garín", "Benavídez", "Dique Luján"], BA, decision=True,
               nota="Garín/Benavídez (Escobar/Tigre) no figuran en el catálogo; se asume ZONA NORTE"),
    # ------------------------------------------------------------------ La Plata y alrededores
    "221":  _e(["103"], "La Plata", ["La Plata", "Ensenada", "Berisso", "City Bell", "Gonnet", "Los Hornos", "Tolosa"], BA),
    "2221": _e(["103"], "Magdalena", ["Magdalena", "Verónica", "Punta Indio"], BA, decision=True, nota="No está en el catálogo; se asume La Plata por cercanía"),
    "2229": _e(["102"], "Juan María Gutiérrez", ["Juan María Gutiérrez", "El Pato"], BA, decision=True, nota="Zona rural de Berazategui; se asume 102"),
    # ------------------------------------------------------------------ 115 Gral. Belgrano / San Vicente
    "2223": _e(["115"], "Brandsen", ["Brandsen", "Jeppener"], BA),
    "2225": _e(["115"], "San Vicente", ["San Vicente", "Alejandro Korn", "Domselaar"], BA),
    "2224": _e(["104"], "Glew", ["Glew", "Guernica", "Presidente Perón"], BA, decision=True, nota="Alte. Brown / Pte. Perón: se asume Zona Sur Lanús-Lomas (104)"),
    "2243": _e(["115"], "General Belgrano", ["General Belgrano", "Gorchs"], BA),
    "2244": _e(["115"], "Las Flores", ["Las Flores"], BA, decision=True, nota="Las Flores no está en el catálogo; Ruta 3, se asume 115"),
    "2271": _e(["115"], "San Miguel del Monte", ["San Miguel del Monte", "Monte"], BA),
    # ------------------------------------------------------------------ 116 Navarro / Cañuelas / Lobos
    "2226": _e(["116"], "Cañuelas", ["Cañuelas", "Uribelarrea", "Vicente Casares"], BA),
    "2227": _e(["116", "130"], "Lobos", ["Lobos", "Roque Pérez"], BA, defecto=True, nota="Lobos = 116; Roque Pérez = 130 (catálogo)"),
    "2272": _e(["116"], "Navarro", ["Navarro", "Las Marianas"], BA),
    # ------------------------------------------------------------------ 120 Ruta 2
    "2241": _e(["120", "115"], "Chascomús", ["Chascomús", "Castelli", "Ranchos"], BA, defecto=True, nota="Chascomús/Castelli = 120; Ranchos = 115 (catálogo)"),
    "2242": _e(["120"], "Lezama", ["Lezama"], BA),
    "2245": _e(["120"], "Dolores", ["Dolores", "Tordillo", "General Guido"], BA),
    "2268": _e(["120"], "Maipú", ["Maipú", "General Madariaga"], BA),
    # ------------------------------------------------------------------ 122 Mar del Plata / Costa Atlántica
    "223":  _e(["122"], "Mar del Plata", ["Mar del Plata", "Chapadmalal", "Santa Clara del Mar", "Batán", "Sierra de los Padres"], BA),
    "2246": _e(["122"], "Santa Teresita", ["Santa Teresita", "Mar del Tuyú", "Las Toninas", "Costa del Este", "Aguas Verdes"], BA),
    "2252": _e(["122"], "San Clemente del Tuyú", ["San Clemente del Tuyú"], BA),
    "2254": _e(["122"], "Pinamar", ["Pinamar", "Ostende", "Valeria del Mar", "Cariló"], BA),
    "2255": _e(["122"], "Villa Gesell", ["Villa Gesell", "Mar de las Pampas", "Mar Azul"], BA),
    "2257": _e(["122"], "Mar de Ajó", ["Mar de Ajó", "San Bernardo", "La Lucila del Mar", "Nueva Atlantis"], BA),
    "2265": _e(["122"], "Coronel Vidal", ["Coronel Vidal", "Mar Chiquita", "Vivoratá"], BA, decision=True, nota="Partido de Mar Chiquita, no está en el catálogo; se asume 122"),
    "2266": _e(["122"], "Balcarce", ["Balcarce"], BA, decision=True, nota="Balcarce no está en el catálogo; a 65 km de Mar del Plata"),
    "2267": _e(["122"], "General Madariaga", ["General Madariaga", "Pinamar (zona rural)"], BA, decision=True, nota="Madariaga no está en el catálogo; se asume Costa Atlántica"),
    "2291": _e(["122"], "Miramar", ["Miramar", "Mar del Sur", "Otamendi"], BA),
    "2296": _e(["122"], "Ayacucho", ["Ayacucho"], BA, decision=True, nota="No está en el catálogo; Ruta 74 hacia Mar del Plata"),
    "2297": _e(["122"], "Rauch", ["Rauch"], BA, decision=True, nota="No está en el catálogo"),
    # ------------------------------------------------------------------ 124 Necochea / Quequén / Tandil (decisión)
    "2262": _e(["124"], "Necochea", ["Necochea", "Quequén", "Juan N. Fernández"], BA),
    "2261": _e(["132"], "Lobería", ["Lobería", "San Manuel"], BA, nota="El catálogo pone Lobería en 132 BAHIA BLANCA"),
    "2292": _e(["124"], "Benito Juárez", ["Benito Juárez", "Barker"], BA, decision=True, nota="No está en el catálogo; se asume Necochea"),
    "249":  _e(["124"], "Tandil", ["Tandil", "Vela", "Gardey"], BA, decision=True, nota="Tandil NO está en el catálogo: se asume 124 NECOCHEA (también podría ser 122). Confirmar"),
    "2983": _e(["124"], "Tres Arroyos", ["Tres Arroyos", "Claromecó", "Reta"], BA, decision=True, nota="Tres Arroyos NO está en el catálogo: se asume 124 (también podría ser 132). Confirmar"),
    "2982": _e(["124"], "Orense", ["Orense", "San Francisco de Bellocq"], BA, decision=True, nota="Partido de Tres Arroyos"),
    # ------------------------------------------------------------------ 130 Ruta 5
    "236":  _e(["130"], "Junín", ["Junín", "Agustín Roca"], BA),
    "2281": _e(["144", "130"], "Azul", ["Azul", "Cacharí", "Ariel", "Chillar"], BA, defecto=True, decision=True, nota="Chillar (partido de Azul) = 144 en el catálogo; Azul ciudad no está catalogada, se asume 144. Confirmar (podría ser 130)."),
    "2284": _e(["130"], "Olavarría", ["Olavarría", "Sierras Bayas", "Hinojo", "Loma Negra"], BA),
    "2342": _e(["130", "144"], "Bragado", ["Bragado", "General O'Brien"], BA, defecto=True, nota="Bragado = 130; O'Brien = 144 (catálogo)"),
    "2353": _e(["130", "144"], "General Arenales", ["General Arenales", "Ascensión"], BA, defecto=True, nota="Arenales = 130; Ascensión = 144 (catálogo)"),
    "2392": _e(["130"], "Trenque Lauquen", ["Trenque Lauquen", "30 de Agosto"], BA),
    "2395": _e(["130"], "Carlos Casares", ["Carlos Casares"], BA),
    "2396": _e(["130"], "Pehuajó", ["Pehuajó"], BA, decision=True, nota="Pehuajó no está en el catálogo; está sobre la Ruta 5 entre C. Casares y T. Lauquen"),
    "2317": _e(["130"], "9 de Julio", ["9 de Julio"], BA, decision=True, nota="9 de Julio no está en el catálogo; está sobre la Ruta 5"),
    "2314": _e(["130"], "Bolívar", ["Bolívar", "Urdampilleta"], BA, decision=True, nota="No está en el catálogo; se asume Ruta 5"),
    "2316": _e(["130"], "Daireaux", ["Daireaux"], BA, decision=True, nota="No está en el catálogo; se asume Ruta 5"),
    "2354": _e(["130"], "Vedia", ["Vedia", "Alberdi"], BA, decision=True, nota="Ruta 7 cerca de Junín; se asume 130"),
    "2355": _e(["130"], "Lincoln", ["Lincoln"], BA, decision=True, nota="Lincoln no está en el catálogo; se asume 130"),
    "2356": _e(["130"], "General Pinto", ["General Pinto"], BA, decision=True),
    "2357": _e(["130"], "Carlos Tejedor", ["Carlos Tejedor"], BA, decision=True),
    "2358": _e(["130"], "Los Toldos", ["Los Toldos", "General Viamonte"], BA, decision=True),
    "3382": _e(["130"], "Rufino", ["Rufino"], SF),
    "3388": _e(["130"], "General Villegas", ["General Villegas"], BA, decision=True, nota="No está en el catálogo; se asume 130"),
    "2924": _e(["130"], "Darregueira", ["Darregueira", "Puán"], BA, nota="El catálogo pone Darregueira en 130 RUTA 5"),
    "2283": _e(["130"], "Tapalqué", ["Tapalqué"], BA, decision=True, nota="No está en el catálogo; cerca de Olavarría"),
    "2344": _e(["144", "130"], "Saladillo", ["Saladillo", "General Alvear (Bs. As.)"], BA, defecto=True, decision=True,
               nota="Gral. Alvear (Bs.As.) = 144 (catálogo); Saladillo no está en el catálogo"),
    # ------------------------------------------------------------------ 132 Bahía Blanca / sudoeste / La Pampa
    "291":  _e(["132", "124"], "Bahía Blanca", ["Bahía Blanca", "Punta Alta", "Sierra de la Ventana", "Tornquist", "General Cerri"], BA, defecto=True,
               nota="Bahía Blanca = 132; Sierra de la Ventana = 124 (catálogo)"),
    "2932": _e(["132"], "Punta Alta", ["Punta Alta", "Coronel Rosales"], BA, decision=True, nota="Se asume Bahía Blanca por cercanía"),
    "2921": _e(["132"], "Coronel Dorrego", ["Coronel Dorrego"], BA, decision=True),
    "2922": _e(["132"], "Coronel Pringles", ["Coronel Pringles"], BA),
    "2923": _e(["132"], "Pigüé", ["Pigüé", "Saavedra"], BA),
    "2925": _e(["132"], "Villa Iris", ["Villa Iris", "Puán"], BA, decision=True),
    "2926": _e(["132"], "Coronel Suárez", ["Coronel Suárez", "Huanguelén"], BA),
    "2927": _e(["132"], "Médanos", ["Médanos", "Villarino"], BA, decision=True),
    "2928": _e(["132"], "Pedro Luro", ["Pedro Luro", "Hilario Ascasubi"], BA, decision=True),
    "2929": _e(["132"], "Guaminí", ["Guaminí"], BA, decision=True),
    "2933": _e(["132"], "Huanguelén", ["Huanguelén"], BA),
    "2935": _e(["132"], "Rivera", ["Rivera", "Adolfo Alsina"], BA, decision=True),
    "2936": _e(["132"], "Carhué", ["Carhué"], BA),
    "2285": _e(["132"], "Laprida", ["Laprida"], BA, decision=True, nota="No está en el catálogo; entre Olavarría y Cnel. Suárez"),
    "2286": _e(["132"], "General Lamadrid", ["General Lamadrid"], BA, nota="'Gral. Lacha' del catálogo se interpreta como Gral. Lamadrid"),
    "2337": _e(["132"], "América", ["América", "Rivadavia (Bs. As.)"], BA, nota="'Rivadavia' del catálogo (partido de Rivadavia, cabecera América)"),
    "2302": _e(["132"], "General Pico", ["General Pico"], LP),
    "2331": _e(["132"], "Realicó", ["Realicó"], LP),
    "2333": _e(["132"], "Quemú Quemú", ["Quemú Quemú"], LP),
    "2334": _e(["132"], "Eduardo Castex", ["Eduardo Castex"], LP),
    "2335": _e(["132"], "Ingeniero Luiggi", ["Ingeniero Luiggi", "Arata", "Caleufú", "Embajador Martini"], LP),
    "2338": _e(["132"], "Victorica", ["Victorica", "Telén"], LP),
    "2952": _e(["132"], "General Acha", ["General Acha"], LP),
    "2953": _e(["132"], "Macachín", ["Macachín"], LP),
    "2954": _e(["130", "132"], "Santa Rosa", ["Santa Rosa", "Toay"], LP,
               nota="El catálogo pone Santa Rosa en 130 Y en 132: queda ambigua hasta que el negocio decida"),
    # ------------------------------------------------------------------ 140 Pergamino / Ruta 8 / Zárate-Campana
    "2477": _e(["140"], "Pergamino", ["Pergamino", "Acevedo"], BA),
    "2474": _e(["140"], "Salto", ["Salto"], BA),
    "2478": _e(["140"], "Arrecifes", ["Arrecifes", "Capitán Sarmiento", "Todd"], BA),
    "2473": _e(["140"], "Colón", ["Colón (Bs. As.)"], BA, decision=True, nota="Colón de Buenos Aires (Ruta 8); no confundir con Colón de Entre Ríos (136)"),
    "2475": _e(["140"], "Rojas", ["Rojas"], BA, decision=True, nota="No está en el catálogo; cerca de Pergamino"),
    "2325": _e(["140"], "San Andrés de Giles", ["San Andrés de Giles"], BA),
    "2326": _e(["140", "144"], "San Antonio de Areco", ["San Antonio de Areco", "Villa Lía"], BA, defecto=True, nota="Areco = 140; Villa Lía = 144 (catálogo)"),
    "2273": _e(["140"], "Carmen de Areco", ["Carmen de Areco"], BA),
    "2352": _e(["140", "144"], "Chacabuco", ["Chacabuco", "O'Higgins"], BA, defecto=True, nota="Chacabuco = 140; O'Higgins = 144 (catálogo)"),
    "2346": _e(["140", "144"], "Chivilcoy", ["Chivilcoy", "Alberti", "Moquehuá", "Gorostiaga"], BA, defecto=True, nota="Chivilcoy = 140; Alberti/Moquehuá/Gorostiaga = 144 (catálogo)"),
    "2345": _e(["140"], "25 de Mayo", ["25 de Mayo", "Valdés", "San Enrique", "Pueblo del Valle"], BA),
    "2343": _e(["140", "144"], "Norberto de la Riestra", ["Norberto de la Riestra", "Pedernales"], BA, defecto=True, nota="N. de la Riestra = 140; Pedernales = 144 (catálogo)"),
    "3487": _e(["140"], "Zárate", ["Zárate", "Lima"], BA),
    "3489": _e(["140"], "Campana", ["Campana", "Los Cardales"], BA),
    # ------------------------------------------------------------------ 141 Luján / Pilar / Mercedes
    "2323": _e(["141"], "Luján", ["Luján", "Capilla del Señor", "Exaltación de la Cruz", "Open Door", "Jáuregui", "Cortines", "Villa Flandria", "Parada Robles"], BA),
    "2324": _e(["141", "144"], "Mercedes", ["Mercedes", "Suipacha", "Gowland"], BA, defecto=True, nota="Mercedes = 141; Suipacha = 144 (catálogo)"),
    # ------------------------------------------------------------------ 142 Rosario y sur de Santa Fe / norte bonaerense
    "341":  _e(["142", "121"], "Rosario", ["Rosario", "Funes", "Roldán", "Soldini", "Pérez", "Granadero Baigorria", "Carcarañá"], SF, defecto=True,
               nota="Carcarañá (121 en el catálogo) también usa 0341"),
    "3400": _e(["142"], "Villa Constitución", ["Villa Constitución", "Empalme Villa Constitución", "Pavón"], SF),
    "3402": _e(["142"], "Arroyo Seco", ["Arroyo Seco", "Álvarez", "Fighiera", "General Lagos"], SF),
    "3464": _e(["142"], "Casilda", ["Casilda", "Arequito", "Pujato", "Fuentes", "Arteaga", "Chabás"], SF),
    "3469": _e(["142"], "Acebal", ["Acebal", "Uranga", "Albarellos", "Carmen del Sauce"], SF),
    "3407": _e(["142"], "Ramallo", ["Ramallo", "Villa Ramallo", "Pérez Millán"], BA),
    "336":  _e(["142"], "San Nicolás", ["San Nicolás de los Arroyos", "Conesa"], BA),
    "3329": _e(["142"], "San Pedro", ["San Pedro", "Baradero", "Santa Lucía"], BA, decision=True, nota="Baradero = 142 (catálogo); San Pedro no está pero queda entre Baradero y San Nicolás"),
    "3465": _e(["142"], "Firmat", ["Firmat", "Melincué"], SF, decision=True),
    "3466": _e(["142"], "Bombal", ["Bombal", "Máximo Paz"], SF, decision=True),
    "3460": _e(["142"], "Santa Teresa", ["Santa Teresa", "Peyrano"], SF, decision=True),
    "3461": _e(["142"], "San Gregorio", ["San Gregorio", "María Teresa"], SF, decision=True),
    "3476": _e(["142", "144"], "San Lorenzo", ["San Lorenzo", "Puerto General San Martín", "Fray Luis Beltrán", "Totoras"], SF, defecto=True, decision=True,
               nota="San Lorenzo no está en el catálogo (se asume Rosario); Totoras = 144 (catálogo)"),
    "3477": _e(["142"], "Alcorta", ["Alcorta", "Juncal"], SF, decision=True),
    # ------------------------------------------------------------------ 121 Cañada de Gómez / Santa Fe capital
    "342":  _e(["121"], "Santa Fe", ["Santa Fe", "Santo Tomé (Santa Fe)", "Coronda", "Recreo", "Sauce Viejo"], SF),
    "3471": _e(["121"], "Cañada de Gómez", ["Cañada de Gómez", "Armstrong", "Correa", "Las Rosas", "Montes de Oca"], SF),
    "3401": _e(["149"], "El Trébol", ["El Trébol", "Los Cardos"], SF, decision=True, nota="Centro-oeste santafesino; se asume 149 ESPERANZA"),
    "3404": _e(["149"], "Gálvez", ["Gálvez", "San Jerónimo Norte", "San Carlos Centro"], SF, decision=True, nota="Dpto San Jerónimo; se asume 149 ESPERANZA"),
    "3405": _e(["121"], "San Javier", ["San Javier"], SF, decision=True),
    "3406": _e(["149"], "San Jorge", ["San Jorge", "Sastre", "Las Petacas"], SF, decision=True, nota="Se asume 149 ESPERANZA por geografía"),
    "3482": _e(["121"], "Reconquista", ["Reconquista", "Avellaneda (Santa Fe)"], SF, decision=True, nota="Norte santafesino, no está en el catálogo: se asume 121 SANTA FE CAP. Confirmar"),
    "3483": _e(["121"], "Vera", ["Vera", "Calchaquí"], SF, decision=True),
    "3484": _e(["121"], "Romang", ["Romang", "Malabrigo"], SF, decision=True),
    # ------------------------------------------------------------------ 149 Esperanza / Rafaela / San Francisco
    "3496": _e(["149"], "Esperanza", ["Esperanza", "Humboldt", "Franck", "San Jerónimo Norte", "Pilar (Santa Fe)"], SF),
    "3492": _e(["149"], "Rafaela", ["Rafaela", "Susana"], SF),
    "3493": _e(["149"], "Sunchales", ["Sunchales"], SF, decision=True),
    "3498": _e(["149"], "San Justo", ["San Justo (Santa Fe)"], SF),
    "3491": _e(["149"], "Ceres", ["Ceres", "Hersilia"], SF, decision=True),
    "3497": _e(["149"], "Llambi Campbell", ["Llambi Campbell", "Nelson"], SF, decision=True),
    "3408": _e(["149"], "San Cristóbal", ["San Cristóbal"], SF, decision=True),
    "3564": _e(["149"], "San Francisco", ["San Francisco (Córdoba)", "Frontera", "Devoto"], CB, nota="El catálogo pone San Francisco (Cba.) en 149"),
    # ------------------------------------------------------------------ 136 Entre Ríos (+ sur de Corrientes) / 143 Urdinarrain-Nogoyá
    "343":  _e(["143", "136"], "Paraná", ["Paraná", "Crespo", "General Ramírez", "Diamante", "Viale", "Villa Urquiza"], ER, defecto=True, decision=True,
               nota="Crespo y Ramírez = 143 (catálogo); Diamante = 136; Paraná capital no está en el catálogo, se asume 143"),
    "3435": _e(["143"], "Nogoyá", ["Nogoyá", "Lucas González"], ER),
    "3436": _e(["136", "143"], "Victoria", ["Victoria (Entre Ríos)"], ER, nota="El catálogo pone Victoria en 136 Y en 143"),
    "3437": _e(["136"], "La Paz", ["La Paz (Entre Ríos)", "Santa Elena"], ER),
    "3438": _e(["136"], "Bovril", ["Bovril", "Hasenkamp"], ER),
    "3442": _e(["136"], "Concepción del Uruguay", ["Concepción del Uruguay", "Caseros"], ER),
    "3444": _e(["136"], "Gualeguay", ["Gualeguay"], ER),
    "3445": _e(["143"], "Rosario del Tala", ["Rosario del Tala", "Basavilbaso", "Maciá"], ER),
    "3446": _e(["136", "143"], "Gualeguaychú", ["Gualeguaychú", "Urdinarrain", "Larroque", "Ceibas", "Pueblo General Belgrano"], ER, defecto=True,
               nota="Gualeguaychú/Ceibas = 136; Urdinarrain = 143 (catálogo)"),
    "3447": _e(["136"], "Colón", ["Colón (Entre Ríos)", "San José", "Villa Elisa", "Ubajay", "Pueblo Liebig"], ER),
    "345":  _e(["136"], "Concordia", ["Concordia", "San Salvador", "Los Charrúas", "Puerto Yeruá"], ER),
    "3454": _e(["136"], "Federal", ["Federal", "Conscripto Bernardi", "El Cimarrón", "Nueva Vizcaya"], ER),
    "3455": _e(["136"], "Villaguay", ["Villaguay"], ER),
    "3456": _e(["136"], "Chajarí", ["Chajarí", "Federación", "Colonia Libertad", "Mocoretá", "Villa del Rosario (E.R.)"], ER),
    "3458": _e(["136"], "San José de Feliciano", ["San José de Feliciano"], ER),
    "3772": _e(["136"], "Paso de los Libres", ["Paso de los Libres"], "Corrientes"),
    "3775": _e(["136"], "Monte Caseros", ["Monte Caseros", "Mocoretá", "Juan Pujol", "Colonia Libertad"], "Corrientes"),
    "3756": _e(["136"], "Santo Tomé", ["Santo Tomé (Corrientes)", "Gobernador Virasoro"], "Corrientes"),
    "3774": _e(["136"], "Curuzú Cuatiá", ["Curuzú Cuatiá"], "Corrientes", decision=True,
               nota="No está en el catálogo; el sudeste correntino (Monte Caseros, Paso de los Libres) está en 136"),
    # ------------------------------------------------------------------ 150 Litoral / Misiones / Corrientes  y 143 (Corrientes repartida)
    "376":  _e(["150"], "Posadas", ["Posadas", "Garupá", "Candelaria"], "Misiones"),
    "3741": _e(["150"], "Bernardo de Irigoyen", ["Bernardo de Irigoyen", "San Pedro (Misiones)"], "Misiones"),
    "3743": _e(["150"], "Puerto Rico", ["Puerto Rico", "Montecarlo", "Jardín América"], "Misiones"),
    "3751": _e(["150"], "Eldorado", ["Eldorado", "Wanda", "Puerto Esperanza"], "Misiones"),
    "3752": _e(["150"], "Aristóbulo del Valle", ["Aristóbulo del Valle", "Jardín América"], "Misiones"),
    "3754": _e(["150"], "Leandro N. Alem", ["Leandro N. Alem", "San Javier (Misiones)"], "Misiones"),
    "3755": _e(["150"], "Oberá", ["Oberá", "Campo Viera"], "Misiones"),
    "3757": _e(["150"], "Puerto Iguazú", ["Puerto Iguazú"], "Misiones"),
    "3758": _e(["150"], "Apóstoles", ["Apóstoles", "Concepción de la Sierra"], "Misiones"),
    "379":  _e(["150", "143"], "Corrientes", ["Corrientes", "Paso de la Patria", "Riachuelo"], "Corrientes",
               nota="'Corrientes' figura como débil en 143 y en 150: el negocio tiene que decidir qué parte de la provincia va a cada una"),
    "3773": _e(["150", "143"], "Mercedes (Corrientes)", ["Mercedes (Corrientes)"], "Corrientes", nota="Corrientes repartida entre 143 y 150"),
    "3777": _e(["150", "143"], "Goya", ["Goya"], "Corrientes", nota="Corrientes repartida entre 143 y 150"),
    "3781": _e(["150", "143"], "Caá Catí", ["Caá Catí"], "Corrientes", nota="Corrientes repartida entre 143 y 150"),
    "3782": _e(["150", "143"], "Saladas", ["Saladas", "San Roque", "Bella Vista"], "Corrientes", nota="Corrientes repartida entre 143 y 150"),
    "3786": _e(["150", "143"], "Ituzaingó (Corrientes)", ["Ituzaingó (Corrientes)"], "Corrientes", nota="Corrientes repartida entre 143 y 150"),
    # ------------------------------------------------------------------ 151 Chaco / Formosa
    "362":  _e(["151"], "Resistencia", ["Resistencia", "Barranqueras", "Fontana", "Puerto Vilelas"], "Chaco"),
    "364":  _e(["151"], "Presidencia Roque Sáenz Peña", ["Presidencia Roque Sáenz Peña"], "Chaco"),
    "3722": _e(["151"], "General San Martín (Chaco)", ["General San Martín (Chaco)"], "Chaco"),
    "3725": _e(["151"], "Villa Ángela", ["Villa Ángela"], "Chaco"),
    "3731": _e(["151"], "Charata", ["Charata", "Las Breñas"], "Chaco"),
    "3732": _e(["151"], "Quitilipi", ["Quitilipi", "Machagai"], "Chaco"),
    "3734": _e(["151"], "Machagai", ["Machagai", "Presidencia de la Plaza"], "Chaco"),
    "3735": _e(["151"], "Villa Berthet", ["Villa Berthet"], "Chaco"),
    "370":  _e(["151"], "Formosa", ["Formosa"], "Formosa"),
    "3711": _e(["151"], "Ingeniero Juárez", ["Ingeniero Juárez"], "Formosa"),
    "3715": _e(["151"], "Las Lomitas", ["Las Lomitas"], "Formosa"),
    "3716": _e(["151"], "Comandante Fontana", ["Comandante Fontana", "Ibarreta"], "Formosa"),
    "3717": _e(["151"], "Pirané", ["Pirané", "El Colorado"], "Formosa"),
    "3718": _e(["151"], "Clorinda", ["Clorinda", "Laguna Blanca"], "Formosa"),
    # ------------------------------------------------------------------ 146 NOA
    "387":  _e(["146"], "Salta", ["Salta", "Cerrillos", "Vaqueros"], "Salta"),
    "3873": _e(["146"], "Tartagal", ["Tartagal"], "Salta"),
    "3876": _e(["146"], "Metán", ["Metán", "Rosario de la Frontera"], "Salta"),
    "3878": _e(["146"], "Orán", ["San Ramón de la Nueva Orán", "Pichanal"], "Salta"),
    "388":  _e(["146"], "San Salvador de Jujuy", ["San Salvador de Jujuy", "Palpalá", "Perico"], "Jujuy"),
    "3884": _e(["146"], "San Pedro de Jujuy", ["San Pedro de Jujuy"], "Jujuy"),
    "3885": _e(["146"], "La Quiaca", ["La Quiaca"], "Jujuy"),
    "3886": _e(["146"], "Libertador General San Martín", ["Libertador General San Martín (Jujuy)"], "Jujuy"),
    "381":  _e(["146"], "San Miguel de Tucumán", ["San Miguel de Tucumán", "Yerba Buena", "Tafí Viejo", "Banda del Río Salí"], "Tucumán"),
    "3865": _e(["146"], "Concepción (Tucumán)", ["Concepción (Tucumán)", "Aguilares"], "Tucumán"),
    "383":  _e(["146"], "San Fernando del Valle de Catamarca", ["Catamarca", "Valle Viejo"], "Catamarca"),
    "380":  _e(["146"], "La Rioja", ["La Rioja"], "La Rioja"),
    "3825": _e(["146"], "Chilecito", ["Chilecito"], "La Rioja"),
    "385":  _e(["146"], "Santiago del Estero", ["Santiago del Estero", "La Banda"], "Santiago del Estero"),
    "3844": _e(["146"], "Añatuya", ["Añatuya"], "Santiago del Estero"),
    "3846": _e(["146"], "Termas de Río Hondo", ["Termas de Río Hondo"], "Santiago del Estero"),
    # ------------------------------------------------------------------ 137 Córdoba (toda la provincia, salvo San Francisco 149 y V. Mackenna 126)
    "351":  _e(["137"], "Córdoba", ["Córdoba", "Río Ceballos", "Rivera Indarte", "Los Boulevares", "Ferreyra", "Villa Allende", "Mendiolaza", "Unquillo", "Saldán", "Malagueño"], CB),
    "3521": _e(["137"], "Deán Funes", ["Deán Funes"], CB),
    "3522": _e(["137"], "Villa de María", ["Villa de María del Río Seco"], CB),
    "3524": _e(["137"], "Villa del Totoral", ["Villa del Totoral", "Sarmiento"], CB),
    "3525": _e(["137"], "Jesús María", ["Jesús María", "Colonia Caroya", "Sinsacate"], CB),
    "3532": _e(["137"], "Oliva", ["Oliva", "James Craik"], CB),
    "3533": _e(["137"], "Las Varillas", ["Las Varillas"], CB),
    "3534": _e(["137"], "Villa María", ["Villa María", "Villa Nueva"], CB),
    "3535": _e(["137"], "Villa María", ["Villa María", "Villa Nueva", "Tío Pujio"], CB),
    "3537": _e(["137"], "Bell Ville", ["Bell Ville", "Justiniano Posse", "Morrison"], CB),
    "3541": _e(["137"], "Villa Carlos Paz", ["Villa Carlos Paz", "Cosquín", "Tanti", "Bialet Massé", "Santa María de Punilla"], CB),
    "3542": _e(["137"], "Salsacate", ["Salsacate", "Villa Cura Brochero"], CB),
    "3543": _e(["137"], "Río Ceballos", ["Río Ceballos", "Unquillo", "Salsipuedes", "Agua de Oro"], CB),
    "3544": _e(["137"], "Villa Dolores", ["Villa Dolores", "Mina Clavero", "Nono"], CB, nota="'Dolores' del catálogo en 137 es Villa Dolores"),
    "3546": _e(["137"], "Santa Rosa de Calamuchita", ["Santa Rosa de Calamuchita", "Villa General Belgrano", "Embalse", "Calamuchita"], CB),
    "3547": _e(["137"], "Alta Gracia", ["Alta Gracia", "Anisacate", "Despeñaderos"], CB),
    "3548": _e(["137"], "La Falda", ["La Falda", "Capilla del Monte", "Huerta Grande", "La Cumbre", "Cosquín"], CB),
    "3549": _e(["137"], "Cruz del Eje", ["Cruz del Eje"], CB),
    "3562": _e(["137"], "Morteros", ["Morteros", "Brinkmann"], CB),
    "3563": _e(["137"], "Balnearia", ["Balnearia", "Miramar de Ansenuza"], CB),
    "3571": _e(["137"], "Río Tercero", ["Río Tercero", "Almafuerte", "Hernando"], CB),
    "3572": _e(["137"], "Río Segundo", ["Río Segundo", "Pilar (Córdoba)", "Costa Sacate"], CB),
    "3573": _e(["137"], "Villa del Rosario", ["Villa del Rosario (Córdoba)", "Colazo"], CB),
    "3574": _e(["137"], "Oncativo", ["Oncativo", "Laguna Larga"], CB),
    "3575": _e(["137"], "La Para", ["La Para", "Villa Fontana"], CB),
    "3576": _e(["137"], "Arroyito", ["Arroyito", "Transito"], CB),
    "358":  _e(["137"], "Río Cuarto", ["Río Cuarto"], CB, nota="Río Cuarto usa característica de 3 dígitos (0358)"),
    "3582": _e(["137"], "Sampacho", ["Sampacho", "Coronel Moldes"], CB),
    "3583": _e(["126", "137"], "Vicuña Mackenna", ["Vicuña Mackenna", "Del Campillo", "Huinca Renancó"], CB, defecto=True, nota="El catálogo pone Vicuña Mackenna en 126 CUYO; el resto del sur cordobés es 137"),
    "3584": _e(["137"], "Río Cuarto", ["Río Cuarto", "Las Higueras", "Holmberg"], CB),
    "3585": _e(["137"], "Adelia María", ["Adelia María", "Berrotarán", "Elena"], CB),
    "3586": _e(["137"], "Río de los Sauces", ["Río de los Sauces", "Elena", "Alcira Gigena"], CB),
    "3472": _e(["137"], "Marcos Juárez", ["Marcos Juárez", "Leones", "Monte Buey"], CB),
    "3462": _e(["137"], "Venado Tuerto", ["Venado Tuerto", "Villa Cañás"], SF, nota="El catálogo pone Venado Tuerto en 137 CORDOBA"),
    "3463": _e(["137"], "Canals", ["Canals", "Isla Verde"], CB),
    "3467": _e(["121", "137"], "Arteaga", ["Arteaga", "Cruz Alta", "Los Surgentes"], SF, defecto=True, nota="Arteaga (Santa Fe) = 121 (catálogo); Cruz Alta/Los Surgentes (Córdoba) = 137"),
    "3468": _e(["137"], "Corral de Bustos", ["Corral de Bustos", "Camilo Aldao"], CB),
    "3473": _e(["137"], "Las Varillas", ["Las Varillas", "La Francia"], CB),
    "3385": _e(["137"], "Laboulaye", ["Laboulaye"], CB),
    # ------------------------------------------------------------------ 126 Cuyo
    "261":  _e(["126"], "Mendoza", ["Mendoza", "Godoy Cruz", "Guaymallén", "Las Heras (Mendoza)", "Maipú (Mendoza)", "Luján de Cuyo"], MZ),
    "263":  _e(["126"], "General San Martín (Mendoza)", ["General San Martín (Mendoza)", "Palmira", "Rivadavia (Mendoza)", "Junín (Mendoza)"], MZ),
    "260":  _e(["126"], "San Rafael", ["San Rafael", "General Alvear (Mendoza)", "Malargüe"], MZ),
    "2622": _e(["126"], "Tunuyán", ["Tunuyán", "Tupungato", "San Carlos"], MZ),
    "2623": _e(["126"], "Tupungato", ["Tupungato"], MZ),
    "2625": _e(["126"], "General Alvear (Mendoza)", ["General Alvear (Mendoza)", "Bowen"], MZ),
    "2626": _e(["126"], "La Paz (Mendoza)", ["La Paz (Mendoza)"], MZ),
    "2627": _e(["126"], "San Rafael", ["San Rafael"], MZ),
    "264":  _e(["126"], "San Juan", ["San Juan", "Rawson (San Juan)", "Chimbas", "Rivadavia (San Juan)", "Santa Lucía", "Pocito", "Albardón", "Caucete"], SJ),
    "2646": _e(["126"], "Villa San Agustín", ["Valle Fértil"], SJ),
    "2647": _e(["126"], "Jáchal", ["San José de Jáchal"], SJ),
    "266":  _e(["126"], "San Luis", ["San Luis", "Juana Koslay", "La Punta"], SL),
    "2652": _e(["126"], "Merlo (San Luis)", ["Merlo (San Luis)", "Santa Rosa del Conlara"], SL),
    "2655": _e(["126"], "La Toma", ["La Toma"], SL),
    "2656": _e(["126"], "Concarán", ["Concarán", "Tilisarao"], SL),
    "2657": _e(["126"], "Villa Mercedes", ["Villa Mercedes", "Justo Daract"], SL),
    # ------------------------------------------------------------------ 148 Sur Corta (Alto Valle) / 152 Sur Larga (cordillera) / 147 Sur
    "299":  _e(["148", "152"], "Neuquén", ["Neuquén", "Cipolletti", "Plottier", "Centenario", "Cinco Saltos", "Fernández Oro", "Senillosa", "Cutral Có", "Plaza Huincul"], NQ, defecto=True,
               nota="Neuquén/Cipolletti/Plottier/Centenario = 148; Cutral Có y Plaza Huincul = 152 (catálogo)"),
    "298":  _e(["148"], "General Roca", ["General Roca", "Allen", "Cervantes", "Mainqué", "Ingeniero Huergo", "General Godoy"], RN),
    "2941": _e(["148"], "Villa Regina", ["Villa Regina", "Chichinales"], RN, nota="Villa Regina es 02941; sin esta entrada un número suyo caería en 294 Bariloche (152)"),
    "2946": _e(["148"], "Choele Choel", ["Choele Choel", "Lamarque", "Luis Beltrán", "Chimpay", "Darwin"], RN),
    "2931": _e(["148"], "Río Colorado", ["Río Colorado"], RN),
    "2934": _e(["148"], "San Antonio Oeste", ["San Antonio Oeste", "General Conesa (Río Negro)", "Las Grutas", "Sierra Grande"], RN, defecto=True,
               nota="Gral. Conesa = 148 (catálogo); San Antonio Oeste no está en el catálogo"),
    "2920": _e(["148"], "Viedma", ["Viedma", "Carmen de Patagones"], RN, decision=True, nota="Viedma NO está en el catálogo: se asume 148 SUR CORTA. Confirmar"),
    "2948": _e(["152"], "Chos Malal", ["Chos Malal"], NQ, decision=True, nota="Norte neuquino, no está en el catálogo"),
    "2942": _e(["152"], "Zapala", ["Zapala", "Aluminé", "Las Lajas"], NQ),
    "2972": _e(["152"], "San Martín de los Andes", ["San Martín de los Andes", "Junín de los Andes"], NQ),
    "2944": _e(["152"], "San Carlos de Bariloche", ["San Carlos de Bariloche", "El Bolsón", "Lago Puelo", "El Hoyo"], RN, decision=True, nota="Área real de Bariloche/El Bolsón (02944); Bariloche no está en el catálogo, se asume 152"),
    "294":  _e(["152"], "San Carlos de Bariloche", ["San Carlos de Bariloche", "El Bolsón", "Villa La Angostura", "Dina Huapi", "Lago Puelo", "El Hoyo"], RN, decision=True,
               nota="El Bolsón / V. La Angostura / Lago Puelo = 152 (catálogo); Bariloche no está en el catálogo, se asume 152 SUR LARGA"),
    "2940": _e(["152"], "Ingeniero Jacobacci", ["Ingeniero Jacobacci", "Maquinchao"], RN, decision=True, nota="Línea Sur rionegrina; se asume 152"),
    "2945": _e(["152"], "Esquel", ["Esquel", "Trevelin", "El Maitén", "Epuyén", "Cholila"], CH),
    "297":  _e(["147"], "Comodoro Rivadavia", ["Comodoro Rivadavia", "Rada Tilly", "Caleta Olivia", "Sarmiento (Chubut)"], CH),
    "280":  _e(["147"], "Trelew", ["Trelew", "Puerto Madryn", "Rawson (Chubut)", "Gaiman", "Dolavon"], CH),
    "2965": _e(["147"], "Trelew (código viejo)", ["Trelew", "Puerto Madryn"], CH),
    "2962": _e(["147"], "Puerto San Julián", ["Puerto San Julián", "Gobernador Gregores"], SC),
    "2963": _e(["147"], "Perito Moreno", ["Perito Moreno", "Los Antiguos", "Las Heras (Santa Cruz)"], SC),
    "2966": _e(["147"], "Río Gallegos", ["Río Gallegos", "Puerto Santa Cruz"], SC),
    "2902": _e(["147"], "El Calafate", ["El Calafate", "El Chaltén"], SC),
    "2964": _e(["147"], "Río Grande", ["Río Grande"], TF),
    "2901": _e(["147"], "Ushuaia", ["Ushuaia"], TF),
}

# Códigos viejos que todavía aparecen en bases históricas -> código vigente
CARACTERISTICAS_VIEJAS = {
    "2322": "230",    # Pilar
    "2320": "230",    # Del Viso / Presidente Derqui
    "3488": "348",    # Escobar / Ingeniero Maschwitz
    "2293": "249",    # Tandil
}

# Prefijos por provincia para características que no estén en la tabla:
# red de seguridad cuando toda la provincia cae en una sola zona.
# (prefijo, zonas). Se prueba del prefijo más largo al más corto.
PREFIJOS_PROVINCIA = [
    ("35", ["137"]),                                                              # Córdoba
    ("26", ["126"]),                                                              # Mendoza / San Juan / San Luis
    ("38", ["146"]),                                                              # NOA completo
    ("36", ["151"]), ("370", ["151"]), ("371", ["151"]), ("372", ["151"]), ("373", ["151"]),   # Chaco / Formosa
    ("374", ["150"]), ("375", ["150"]), ("376", ["150"]),                          # Misiones
    ("377", ["150", "143"]), ("378", ["150", "143"]), ("379", ["150", "143"]),    # Corrientes (repartida)
    ("299", ["148"]), ("298", ["148"]),                                            # Alto Valle
    ("297", ["147"]), ("280", ["147"]), ("296", ["147"]), ("290", ["147"]),       # Patagonia sur
    ("295", ["132"]), ("233", ["132"]),                                            # La Pampa
    ("222", ["115"]),                                                              # 222x: Brandsen / San Vicente / Cañuelas / Lobos (red de seguridad)
]

# ---------------------------------------------------------------------------------------------
# Centrales fijas del 011 (los 4 primeros dígitos del número local 4xxx-xxxx) -> zona.
# PROVISIONAL (borrador propio): se reemplaza por la tabla investigada en centrales_amba.json.
# ---------------------------------------------------------------------------------------------
_RANGOS_AMBA = [
    # Verificado 2026-09-07 por búsqueda web (guías, municipios, comercios).
    # 'media'/'baja' = frontera CABA/GBA o dato de una sola fuente: confirmable.
    ("4200", "4209", "102"),  # Avellaneda  # media
    ("4210", "4210", "102"),  # Quilmes  # baja
    ("4212", "4212", "102"),  # Quilmes (San Francisco Solano)  # media
    ("4216", "4216", "102"),  # Berazategui  # media
    ("4217", "4217", "102"),  # Avellaneda  # media
    ("4218", "4218", "104"),  # Lanus/Lomas de Zamora  # media
    ("4220", "4222", "102"),  # Avellaneda  # media
    ("4225", "4225", "104"),  # Lanus  # media
    ("4228", "4228", "104"),  # Lanus  # media
    ("4231", "4231", "104"),  # Lomas de Zamora  # media
    ("4238", "4238", "104"),  # Almirante Brown (Burzaco)  # media
    ("4240", "4249", "104"),  # Lanus/Lomas de Zamora/Temperley/Banfie
    ("4250", "4254", "102"),  # Quilmes  # media
    ("4255", "4255", "102"),  # Florencio Varela
    ("4256", "4256", "102"),  # Berazategui
    ("4257", "4259", "102"),  # Quilmes  # media
    ("4260", "4260", "102"),  # Quilmes/Berazategui  # baja
    ("4262", "4262", "104"),  # Lanus  # baja
    ("4267", "4267", "102"),  # Berazategui/Florencio Varela  # baja
    ("4280", "4282", "104"),  # Almirante Brown/Lomas de Zamora  # media
    ("4286", "4286", "104"),  # Almirante Brown/Lomas de Zamora  # baja
    ("4289", "4290", "104"),  # Lomas de Zamora/Temperley  # baja
    ("4293", "4294", "104"),  # Almirante Brown (Adrogue)
    ("4295", "4296", "104"),  # Esteban Echeverria (Monte Grande)
    ("4298", "4298", "104"),  # Lomas de Zamora/Temperley
    ("4301", "4308", "110"),  # CABA (Barracas/La Boca/Constitucion)
    ("4441", "4447", "107"),  # La Matanza (San Justo)
    ("4452", "4454", "107"),  # La Matanza/Ramos Mejia  # media
    ("4461", "4461", "107"),  # Moron (Haedo)/La Matanza  # media
    ("4470", "4471", "107"),  # La Matanza/Moron  # baja
    ("4484", "4484", "107"),  # Moron
    ("4512", "4512", "101"),  # San Isidro/Vicente Lopez  # media
    ("4522", "4522", "110"),  # CABA (Villa del Parque/Villa Devoto)  # baja
    ("4541", "4541", "110"),  # CABA (Villa Pueyrredon/Villa Urquiza)  # baja
    ("4566", "4568", "110"),  # CABA (Villa Devoto/Villa Real/Versalle  # media
    ("4571", "4571", "110"),  # CABA (Belgrano/Colegiales)  # baja
    ("4581", "4584", "110"),  # CABA (Villa Gral Mitre/Paternal/Villa 
    ("4611", "4612", "110"),  # CABA (Flores)  # media
    ("4621", "4626", "110"),  # CABA (Mataderos/Villa Lugano)  # baja
    ("4635", "4639", "110"),  # CABA (Villa Luro/Mataderos/Villa Lugan  # media
    ("4651", "4657", "107"),  # La Matanza (Ramos Mejia)/Haedo  # media
    ("4666", "4666", "107"),  # Hurlingham/Moron  # media
    ("4671", "4671", "110"),  # CABA (Flores/Floresta)
    ("4693", "4699", "107"),  # Moron/Castelar/Haedo  # media
    ("4709", "4716", "101"),  # Vicente Lopez/Olivos/La Lucila  # media
    ("4720", "4729", "101"),  # Vicente Lopez/Florida/Munro/Carapachay  # media
    ("4731", "4735", "101"),  # San Isidro (Martinez/Acassuso)/Tigre  # media
    ("4740", "4746", "101"),  # San Isidro (Beccar)/San Fernando (Vict  # media
    ("4749", "4749", "101"),  # Tigre
    ("4750", "4759", "101"),  # San Isidro/Boulogne/San Fernando/Vicen  # media
    ("4762", "4769", "101"),  # Vicente Lopez (Munro/Carapachay/Villa   # media
    ("4777", "4777", "110"),  # CABA (Palermo)  # media
    ("4790", "4794", "101"),  # Vicente Lopez/Olivos  # media
    ("4854", "4862", "110"),  # CABA (Villa Crespo/Palermo/Chacarita)
    ("4911", "4919", "110"),  # CABA (Barracas/Parque Patricios)
    ("4922", "4928", "110"),  # CABA (Chacarita/Villa Crespo/Villa Ort
    ("4941", "4941", "110"),  # CABA (Once/Balvanera/Abasto)
    ("4992", "4998", "110"),  # CABA (Balvanera/Congreso/Once)
]
CENTRALES_AMBA = {}
for _a, _b, _z in _RANGOS_AMBA:
    for _c in range(int(_a), int(_b) + 1):
        CENTRALES_AMBA[str(_c)] = _z
