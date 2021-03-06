from bs4 import BeautifulSoup
from datetime import datetime


def to_float(num_str):
    try:
        num = float(num_str)
    except ValueError as ve:
        num = 0.0
    return num


def to_date(date_str):
    try:
        tmp = date_str.split(' ')
        date = tmp[0].split('/')
        time = tmp[1].split(':')
        formatted_date = datetime(int(date[2]), int(date[1]), int(date[0]), int(time[0]), int(time[1]))
    except Exception as ex:
        formatted_date = datetime.now()
    return formatted_date.strftime('%d/%m/%y %H:%M:%S')


class HtmlToDict:

    @staticmethod
    def transform(form):
        soup = BeautifulSoup(str(form), "html.parser")
        elements = soup.find_all('ul')

        try:
            div_link = soup.find('div', class_='estiloOculto')
            link_contratacion = div_link.find('a')['href']
        except:
            link_contratacion = ''

        dict_data = {'link_contratacion': link_contratacion, 'org_contratacion': '', 'estado': '', 'objeto': '',
                       'presupuesto_sin_impuestos': '', 'valor_estimado': '', 'tipo_contrato': '', 'cpv': '',
                       'lugar': '', 'procedimiento': '',
                       'info': {}, 'fecha': datetime.now()}

        for element in elements:
            if element.get('class') and (
                    'ancho100' in element.get('class') or 'altoDetalleLicitacion' in element.get('class')):
                if element.get('id') == 'fila2':
                    dict_data['org_contratacion'] = element.find('span', class_="outputText").text
                elif element.get('id') == 'fila3':
                    dict_data['estado'] = element.find('span', class_="outputText").text
                elif element.get('id') == 'fila4':
                    dict_data['objeto'] = element.find('span', class_="outputText").text
                elif element.get('id') == 'fila5':
                    dict_data['presupuesto_sin_impuestos'] = to_float(
                        element.find('span', class_="outputText").text.replace('.', '').replace(',', '.'))
                elif element.get('id') == 'fila6':
                    dict_data['valor_estimado'] = to_float(
                        element.find('span', class_="outputText").text.replace('.', '').replace(',', '.'))
                elif element.get('id') == 'fila7':
                    dict_data['tipo_contrato'] = element.find('span', class_="outputText").text
                elif element.get('id') == 'fila8':
                    dict_data['cpv'] = element.find('span', class_="outputText").text
                elif element.get('id') == 'fila9':
                    dict_data['lugar'] = element.find('span', class_="outputText").text
                elif element.get('id') == 'fila10':
                    dict_data['procedimiento'] = element.find('span', class_="outputText").text
                # Info
                if element.find('span', class_="tipo3"):
                    if element.find('span', class_="tipo3").text == 'Fecha fin de presentaci??n de oferta':
                        dict_data['info']['fecha_fin_presentacion_oferta'] = to_date(element.find('span',
                                                                                            class_="outputText").text)
                    elif element.find('span', class_="tipo3").text == 'Resultado':
                        dict_data['info']['resultado'] = element.find('span', class_="outputText").text
                    elif element.find('span', class_="tipo3").text == 'Adjudicatario':
                        dict_data['info']['adjudicatario'] = element.find('span', class_="outputText").text
                    elif element.find('span', class_="tipo3").text == 'N?? de Licitadores Presentados':
                        dict_data['info']['num_licitadores'] = element.find('span', class_="outputText").text
                    elif element.find('span', class_="tipo3").text == 'Importe de Adjudicaci??n':
                        try:
                            dict_data['info']['importe_adjudicacion'] = to_float(
                                element.find('span', class_="outputText").text.replace('.', '').replace(',', '.'))
                        except:
                            dict_data['info']['importe_adjudicacion'] = to_float(element.find('span', class_="outputText").text)
                    elif element.find('span', class_="tipo3").text == 'Fecha de Actualizaci??n del Expte.':
                        dict_data['info']['fecha_act_expediente'] = to_date(element.find('span', class_="outputText").text)
                    elif element.find('span', class_="tipo3").text == 'Fecha fin de presentaci??n de solicitud':
                        dict_data['info']['fecha_presentacion_solicitud'] = to_date(element.find('span',
                                                                                           class_="outputText").text)

        return dict_data
