from bs4 import BeautifulSoup


class HtmlToDict:

    @staticmethod
    def transform(form):
        soup = BeautifulSoup(str(form), "html.parser")
        elements = soup.find_all('ul')

        dict_data = {'org_contratacion': '', 'estado': '', 'objeto': '', 'presupuesto_sin_impuestos': '',
                       'valor_estimado': '', 'tipo_contrato': '', 'cpv': '', 'lugar': '', 'procedimiento': '',
                       'info': {}}

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
                    dict_data['presupuesto_sin_impuestos'] = float(
                        element.find('span', class_="outputText").text.replace('.', '').replace(',', '.'))
                elif element.get('id') == 'fila6':
                    dict_data['valor_estimado'] = float(
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
                    if element.find('span', class_="tipo3").text == 'Fecha fin de presentación de oferta':
                        dict_data['info']['fecha_fin_presentacion_oferta'] = element.find('span',
                                                                                            class_="outputText").text
                    elif element.find('span', class_="tipo3").text == 'Resultado':
                        dict_data['info']['resultado'] = element.find('span', class_="outputText").text
                    elif element.find('span', class_="tipo3").text == 'Adjudicatario':
                        dict_data['info']['adjudicatario'] = element.find('span', class_="outputText").text
                    elif element.find('span', class_="tipo3").text == 'Nº de Licitadores Presentados':
                        dict_data['info']['num_licitadores'] = element.find('span', class_="outputText").text
                    elif element.find('span', class_="tipo3").text == 'Importe de Adjudicación':
                        try:
                            dict_data['info']['importe_adjudicacion'] = float(
                                element.find('span', class_="outputText").text.replace('.', '').replace(',', '.'))
                        except:
                            dict_data['info']['importe_adjudicacion'] = element.find('span', class_="outputText").text
                    elif element.find('span', class_="tipo3").text == 'Fecha de Actualización del Expte.':
                        dict_data['info']['fecha_act_expediente'] = element.find('span', class_="outputText").text
                    elif element.find('span', class_="tipo3").text == 'Fecha fin de presentación de solicitud':
                        dict_data['info']['fecha_presentacion_solicitud'] = element.find('span',
                                                                                           class_="outputText").text
                        
        return dict_data
    