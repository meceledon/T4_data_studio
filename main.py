import requests
import pandas as pd
from xml.etree import ElementTree
import gspread
from gspread_dataframe import set_with_dataframe

indicadores = ['Number of deaths', 'Number of infant deaths', 'Number of under-five deaths',
'Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)','Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)',
'Estimates of number of homicides','Crude suicide rates (per 100 000 population)','Mortality rate attributed to unintentional poisoning (per 100 000 population)',
'Number of deaths attributed to non-communicable diseases, by type of disease and sex', 'Estimated road traffic death rate (per 100 000 population)', 'Estimated number of road traffic deaths',
'Mean BMI (kg/m&#xb2;) (crude estimate)','Mean BMI (kg/m&#xb2;) (age-standardized estimate)',  'Prevalence of obesity among adults, BMI &GreaterEqual; 30 (age-standardized estimate) (%)', 
'Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)', 'Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)',
'Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)','Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)', 'Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)',
'Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)', 'Estimate of daily cigarette smoking prevalence (%)', 'Estimate of daily tobacco smoking prevalence (%)', 'Estimate of current cigarette smoking prevalence (%)', 
'Estimate of current tobacco smoking prevalence (%)', 'Mean systolic blood pressure (crude estimate)','Mean fasting blood glucose (mmol/l) (crude estimate)', 'Mean Total Cholesterol (crude estimate)']

indicadores_muerte_infantil = ['Number of deaths', 'Number of infant deaths', 'Number of under-five deaths', 'Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)']

indicadores_muerte_general = ['Number of deaths', 'Number of infant deaths', 'Number of under-five deaths',
'Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)','Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)',
'Estimates of number of homicides','Crude suicide rates (per 100 000 population)','Mortality rate attributed to unintentional poisoning (per 100 000 population)',
'Number of deaths attributed to non-communicable diseases, by type of disease and sex', 'Estimated road traffic death rate (per 100 000 population)', 'Estimated number of road traffic deaths']

indicadores_peso = ['Mean BMI (kg/m&#xb2;) (crude estimate)','Mean BMI (kg/m&#xb2;) (age-standardized estimate)',  'Prevalence of obesity among adults, BMI &GreaterEqual; 30 (age-standardized estimate) (%)', 
'Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)', 'Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)',
'Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)','Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)', 'Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)']

indicadores_otros = ['Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)', 'Estimate of daily cigarette smoking prevalence (%)', 'Estimate of daily tobacco smoking prevalence (%)', 'Estimate of current cigarette smoking prevalence (%)', 
'Estimate of current tobacco smoking prevalence (%)', 'Mean systolic blood pressure (crude estimate)','Mean fasting blood glucose (mmol/l) (crude estimate)', 'Mean Total Cholesterol (crude estimate)']

respuesta_albania = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_ALB.xml')
respuesta_austria = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_AUT.xml')
respuesta_canada = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_CAN.xml')
respuesta_chile = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_CHL.xml')
respuesta_japon = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_JPN.xml')
respuesta_usa = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_USA.xml')

respuesta_albania_xml_root = ElementTree.fromstring(respuesta_albania.content)
respuesta_austria_xml_root = ElementTree.fromstring(respuesta_austria.content)
respuesta_canada_xml_root = ElementTree.fromstring(respuesta_canada.content)
respuesta_chile_xml_root = ElementTree.fromstring(respuesta_chile.content)
respuesta_japon_xml_root = ElementTree.fromstring(respuesta_japon.content)
respuesta_usa_xml_root = ElementTree.fromstring(respuesta_usa.content)

# mi proxima función aprendí a hacerla en https://medium.com/@robertopreste/from-xml-to-pandas-dataframes-9292980b1c1c asi que tiene codigo que aparece ahí

def xml_a_pandas(indicadores, root):
    colums = ['GHO', 'COUNTRY', 'SEX', 'YEAR', 'GHECAUSES', 'AGEGROUP', 'Display', 'Numeric', 'Low', 'High']
    rows = []
    facts = root.findall('Fact')
    for node in facts: 
        fila = []
        if node.find(colums[0]).text in indicadores:
            fila.append(node.find(colums[0]).text)
            for el in colums[1:]: 
                if node is not None and node.find(el) is not None:
                    fila.append(node.find(el).text)
                else: 
                    fila.append(None)
            rows.append({colums[i]: fila[i] 
                        for i, _ in enumerate(colums)})
    
    out_df = pd.DataFrame(rows, columns=colums)
    return out_df

parse_albania_muertes_infantiles = xml_a_pandas(indicadores_muerte_infantil, respuesta_albania_xml_root)
parse_austria_muertes_infantiles = xml_a_pandas(indicadores_muerte_infantil, respuesta_austria_xml_root)
parse_canada_muertes_infantiles = xml_a_pandas(indicadores_muerte_infantil, respuesta_canada_xml_root)
parse_chile_muertes_infantiles = xml_a_pandas(indicadores_muerte_infantil, respuesta_chile_xml_root)
parse_japon_muertes_infantiles = xml_a_pandas(indicadores_muerte_infantil, respuesta_japon_xml_root)
parse_usa_muertes_infantiles = xml_a_pandas(indicadores_muerte_infantil, respuesta_usa_xml_root)

df_paises_muertes_infantiles = pd.concat([parse_albania_muertes_infantiles, parse_austria_muertes_infantiles, parse_canada_muertes_infantiles, parse_chile_muertes_infantiles, parse_japon_muertes_infantiles, parse_usa_muertes_infantiles])

parse_albania_muertes_general = xml_a_pandas(indicadores_muerte_general, respuesta_albania_xml_root)
parse_austria_muertes_general = xml_a_pandas(indicadores_muerte_general, respuesta_austria_xml_root)
parse_canada_muertes_general = xml_a_pandas(indicadores_muerte_general, respuesta_canada_xml_root)
parse_chile_muertes_general = xml_a_pandas(indicadores_muerte_general, respuesta_chile_xml_root)
parse_japon_muertes_general = xml_a_pandas(indicadores_muerte_general, respuesta_japon_xml_root)
parse_usa_muertes_general = xml_a_pandas(indicadores_muerte_general, respuesta_usa_xml_root)

df_paises_muertes_general = pd.concat([parse_albania_muertes_general, parse_austria_muertes_general, parse_canada_muertes_general, parse_chile_muertes_general, parse_japon_muertes_general, parse_usa_muertes_general])

parse_albania_peso = xml_a_pandas(indicadores_peso, respuesta_albania_xml_root)
parse_austria_peso = xml_a_pandas(indicadores_peso, respuesta_austria_xml_root)
parse_canada_peso = xml_a_pandas(indicadores_peso, respuesta_canada_xml_root)
parse_chile_peso = xml_a_pandas(indicadores_peso, respuesta_chile_xml_root)
parse_japon_peso = xml_a_pandas(indicadores_peso, respuesta_japon_xml_root)
parse_usa_peso = xml_a_pandas(indicadores_peso, respuesta_usa_xml_root)

df_paises_peso = pd.concat([parse_albania_peso, parse_austria_peso, parse_canada_peso, parse_chile_peso, parse_japon_peso, parse_usa_peso])

parse_albania_otros = xml_a_pandas(indicadores_otros, respuesta_albania_xml_root)
parse_austria_otros = xml_a_pandas(indicadores_otros, respuesta_austria_xml_root)
parse_canada_otros = xml_a_pandas(indicadores_otros, respuesta_canada_xml_root)
parse_chile_otros = xml_a_pandas(indicadores_otros, respuesta_chile_xml_root)
parse_japon_otros = xml_a_pandas(indicadores_otros, respuesta_japon_xml_root)
parse_usa_otros = xml_a_pandas(indicadores_otros, respuesta_usa_xml_root)

df_paises_otros = pd.concat([parse_albania_otros, parse_austria_otros, parse_canada_otros, parse_chile_otros, parse_japon_otros, parse_usa_otros])

#parse_chile_otros.to_excel("probar.xlsx")

# ACCES GOOGLE SHEET
gc = gspread.service_account(filename='taller-tarea-4-316723-eebcdcb7899e.json')
# 'your_google_sheet_ID'
sh = gc.open_by_key('17W69uceh-foPGasoqTjxZ1Eyu6DmnPsdnGbc1K8lucw')
# sheet_index_no -> 0 first sheet, 1 second  sheet
worksheet = sh.get_worksheet(0)
worksheet1 = sh.get_worksheet(1)
worksheet2 = sh.get_worksheet(2)
worksheet3 = sh.get_worksheet(3)


# # APPEND DATA TO SHEET
# your_dataframe = pd.DataFrame()
# ->This exports your dataframe to the googleSheet
set_with_dataframe(worksheet, df_paises_muertes_infantiles)
set_with_dataframe(worksheet1, df_paises_muertes_general)
set_with_dataframe(worksheet2, df_paises_peso)
set_with_dataframe(worksheet3, df_paises_otros)

