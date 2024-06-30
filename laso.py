import aiohttp
import asyncio
import pandas as pd
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin

async def fetch(session, url, cache):
    if url in cache:
        return cache[url]
    async with session.get(url) as response:
        response.raise_for_status()
        html = await response.text()
        cache[url] = html
        return html

async def descargar_imagenes_desde_urls(session, url, codigo_busqueda, carpeta_destino, cache, lineas, index, lote_num):
    try:
        html = await fetch(session, url, cache)
        soup = BeautifulSoup(html, 'html.parser')
        product_top_wrapper_div = soup.find('div', class_='product-top-wrapper')
        if product_top_wrapper_div:
            img_tags = product_top_wrapper_div.find_all('img')
            lote_carpeta_destino = os.path.join(carpeta_destino, f"lote_{lote_num}")
            os.makedirs(lote_carpeta_destino, exist_ok=True)

            tasks = []
            for img_index, img_tag in enumerate(img_tags):
                img_url = urljoin(url, img_tag['src'])
                tasks.append(descargar_y_guardar_imagen(session, img_url, lote_carpeta_destino, lineas, index, codigo_busqueda, img_index, cache))

            await asyncio.gather(*tasks)
        else:
            print(f"No se encontró el div 'product-top-wrapper' en la URL: {url}")
    except Exception as e:
        print(f"Error al descargar imágenes desde la URL: {url}. Excepción: {str(e)}")

async def descargar_y_guardar_imagen(session, img_url, lote_carpeta_destino, lineas, index, codigo_busqueda, img_index, cache):
    try:
        if img_url in cache:
            img_response = cache[img_url]
        else:
            async with session.get(img_url) as response:
                response.raise_for_status()
                img_response = await response.read()
                cache[img_url] = img_response

        nombre_archivo = os.path.join(lote_carpeta_destino, f"{lineas[index]}_013_{codigo_busqueda}_{img_index+1:03}.jpg")
        with open(nombre_archivo, 'wb') as f:
            f.write(img_response)
        print(f"Imagen descargada: {nombre_archivo}")
    except Exception as e:
        print(f"Error al descargar la imagen {img_url}: {str(e)}")

async def procesar_lote(session, codigos_lote, lineas_lote, carpeta_imagenes, cache, lote_num):
    url_base = 'https://www.laso.de/es/buscar/'
    resultados = []
    datos_por_col1 = {'Código': [], 'linea': [], 'marca': [], 'codlinea': []}
    datos_por_col2 = {'Código': [], 'linea': [], 'Contenido': [], 'codlinea': []}
    datos_por_col3 = {'Código': [], 'linea': [], 'Contenido': [], 'codlinea': []}

    for index, codigo in enumerate(codigos_lote):
        print(f"Procesando código {index+1}/{len(codigos_lote)}: {codigo}")
        url = f'{url_base}?mainsearch={codigo}&search='
        try:
            html = await fetch(session, url, cache)
            soup = BeautifulSoup(html, 'html.parser')
            datos_producto = {'Código': codigo, 'codlinea': lineas_lote[index]}
            divs_info = soup.find_all('div', class_='csc-textpic-text frame-type-textpic')
            for div_info in divs_info:
                h3_tags = div_info.find_all('h3')
                p_tags = div_info.find_all('p')
                for h3_tag, p_tag in zip(h3_tags, p_tags):
                    columna = h3_tag.get_text(strip=True)
                    registro = p_tag.get_text(strip=True)
                    datos_producto[columna] = registro
            contenedor = soup.find('div', class_='row3')
            if contenedor:
                col1_divs = contenedor.find_all('div', class_='col1')
                if len(col1_divs) > 0:
                    col1_div = col1_divs[0]
                    h4_tags = col1_div.find_all('h4')
                    h4_texts = [tag.get_text(strip=True) for tag in h4_tags]
                    markenname_tags = col1_div.find_all('span', class_='markenname')
                    current_h4_index = -1
                    paired_data = []
                    for element in col1_div.find_all(['h4', 'span']):
                        if element.name == 'h4':
                            current_h4_index += 1
                            paired_data.append({'linea': element.get_text(strip=True), 'marca': []})
                        elif element.name == 'span' and 'markenname' in element.get('class', []):
                            if current_h4_index >= 0:
                                paired_data[current_h4_index]['marca'].append(element.get_text(strip=True))
                    for data in paired_data:
                        data['marca'] = ', '.join(data['marca'])
                        datos_por_col1['Código'].append(codigo)
                        datos_por_col1['linea'].append(data['linea'])
                        datos_por_col1['marca'].append(data['marca'])
                        datos_por_col1['codlinea'].append(lineas_lote[index])
                if len(col1_divs) > 1:
                    col1_div = col1_divs[1]
                    current_h4_index = -1
                    paired_data = []
                    for element in col1_div.find_all(['h4', 'span', 'div']):
                        if element.name == 'h4':
                            current_h4_index += 1
                            paired_data.append({'linea': element.get_text(strip=True), 'contenido': []})
                        elif element.name == 'div' and ('lasoTableEntry' in element.get('class', []) or 'lasoVehicleEntry' in element.get('class', []) or 'lasoSubEntry' in element.get('class', [])):
                            paired_data[current_h4_index]['contenido'].append(element.get_text(strip=True) + ',')
                    for data in paired_data:
                        contenido_text = ' '.join(data['contenido']).strip().rstrip(',')
                        datos_por_col2['Código'].append(codigo)
                        datos_por_col2['linea'].append(data['linea'])
                        datos_por_col2['Contenido'].append(contenido_text)
                        datos_por_col2['codlinea'].append(lineas_lote[index])
                if len(col1_divs) > 2:
                    col1_div = col1_divs[2]
                    current_h4_index = -1
                    paired_data = []
                    for element in col1_div.find_all(['h4', 'span', 'div']):
                        if element.name == 'h4':
                            current_h4_index += 1
                            paired_data.append({'linea': element.get_text(strip=True), 'contenido': []})
                        elif element.name == 'div' and ('lasoTableEntry' in element.get('class', []) or 'lasoVehicleEntry' in element.get('class', []) or 'lasoSubEntry' in element.get('class', [])):
                            paired_data[current_h4_index]['contenido'].append(element.get_text(strip=True) + ',')
                    for data in paired_data:
                        contenido_text = ' '.join(data['contenido']).strip().rstrip(',')
                        datos_por_col3['Código'].append(codigo)
                        datos_por_col3['linea'].append(data['linea'])
                        datos_por_col3['Contenido'].append(contenido_text)
                        datos_por_col3['codlinea'].append(lineas_lote[index])
            else:
                print("No se encontró el contenedor 'row3' en la página.")
            resultados.append(datos_producto)
            await descargar_imagenes_desde_urls(session, url, codigo, carpeta_imagenes, cache, lineas_lote, index, lote_num)
        except Exception as e:
            print(f"Error al procesar el código {codigo}: {str(e)}")
            resultados.append({'Código': codigo, 'Error': str(e)})

    return resultados, datos_por_col1, datos_por_col2, datos_por_col3

async def main():
    # Leer los códigos desde el archivo Excel
    ruta_archivo_codigos =  r'C:\Users\analistacdo\Documents\PRUEBA\prueba\codigo2.xlsx'
    df_codigos = pd.read_excel(ruta_archivo_codigos, dtype={'linea': str})

    # Mostrar todas las columnas disponibles
    print("Columnas disponibles en el archivo Excel:", df_codigos.columns)

    # Verificar si existe la columna "linea"
    if 'linea' in df_codigos.columns:
        print(df_codigos['linea'])
    else:
        print("La columna 'linea' no se encuentra en el archivo Excel.")

    codigos = df_codigos['codigo'].tolist()
    lineas = df_codigos['linea'].tolist()

    # Carpeta destino para imágenes
    carpeta_imagenes = r'C:\Users\analistacdo\Documents\PRUEBA\prueba\dat\imagenes'
    cache = {}

    async with aiohttp.ClientSession() as session:
        # Procesar los códigos en lotes de 5000
        tamanio_lote = 5000
        for i in range(0, len(codigos), tamanio_lote):
            codigos_lote = codigos[i:i + tamanio_lote]
            lineas_lote = lineas[i:i + tamanio_lote]
            lote_num = i // tamanio_lote + 1
            print(f"Procesando lote {lote_num}: códigos {i + 1} a {i + len(codigos_lote)}")

            resultados, datos_por_col1, datos_por_col2, datos_por_col3 = await procesar_lote(session, codigos_lote, lineas_lote, carpeta_imagenes, cache, lote_num)

            # Guardar los resultados de cada lote en un archivo Excel
            ruta_archivo_resultado = f'C:\\Users\\analistacdo\\Documents\\PRUEBA\\prueba\\dat\\MERCEDES_lote_{lote_num}.xlsx'
            with pd.ExcelWriter(ruta_archivo_resultado, engine='xlsxwriter') as writer:
                df_resultados = pd.DataFrame(resultados)
                df_resultados.to_excel(writer, sheet_name='caracteristica', index=False)

                df_col1 = pd.DataFrame(datos_por_col1)
                df_col1.to_excel(writer, sheet_name='referencia_cruzada', index=False)

                df_col2 = pd.DataFrame(datos_por_col2)
                df_col2.to_excel(writer, sheet_name='marca', index=False)

                df_col3 = pd.DataFrame(datos_por_col3)
                df_col3.to_excel(writer, sheet_name='vehiculo', index=False)

            print(f"Datos guardados en {ruta_archivo_resultado}")

            # Esperar 80 segundos antes de procesar el siguiente lote
            await asyncio.sleep(80)

# Ejecutar el bucle principal asyncio
if __name__ == "__main__":
    asyncio.run(main())
