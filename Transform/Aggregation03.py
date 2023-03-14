#Dieses Skript enthält alle notwendigen Aggregationsschritte zur Beantwortung der Fragen und zur Erzeugung der xlsx

import pandas as pd
import warnings
from pandas.core.common import SettingWithCopyWarning

#Settings für Pycharm
desired_width = 400
pd.set_option('display.width', desired_width)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_columns', 10)
pd.set_option("display.max_colwidth", 15)
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)

#Load data:
def open_csv():
    csv_name = "Transform/JobAgent_stage.csv"
    path = (f"/home/student/Cloud/Owncloud/SyncVM/CIP Project/JobAgent_T/{csv_name}")
    df = pd.read_csv(path, index_col = ["Primärschlüssel"], sep=";")
    df.info()
    return df

def question1(df):
    '''
    Question 1: Welche Branche hat momentan die meisten offenen Stellen im Data Science Bereich?
    :param df: df, welches aus JobAgent_stage.csv erzeugt wird.
    :return: aggregierte df mit relativen und absoluten zahlen
    '''
    print(40 * "*")
    print("Question 1: Welche Branche hat momentan die meisten offenen Stellen im Data Science Bereich?")

    #Group by Branche mit absoluten Zahlen:
    grouped_branche = df.groupby('Branche')
    abs_branche = grouped_branche.size().sort_values(ascending=False)
    # Or:# abs= df.groupby(['Branche'])["Branche"].count().sort_values(ascending=False)
    #print(abs)

    print("Am meisten offenen Stellen (Top 5) im Data Science Bereich sind in den Branchen:")
    print(abs_branche.head(5))
    print(40* "*")

    #Count per Branche als neue Variable:
    df2 = df.copy()
    df2['Count_per_Branche'] = df.groupby('Branche')['Branche'].transform('count')
    # df2['CUMSUM'] = df['JobNr'].cumsum() #cumsum

    #Group by Branche mit Relativen Zahlen:
    sum_branche = grouped_branche.size().sum()
    df2["In % von Total"] = round((df2["Count_per_Branche"] / sum_branche) * 100,2)

    rel_branche = df2.groupby(['Branche'])["In % von Total"].mean().sort_values(ascending=False)

    #Concat Result:
    result_branche = pd.concat([abs_branche,rel_branche],axis=1)
    result_branche.rename(columns = {0 : "Anzahl offener Stellen"}, inplace = True)
    print(result_branche)

    return result_branche

def question2(df):
    '''
    #Question 2: In welchem Kanton und in welcher Stadt / Ortschaft gibt es die meisten offenen Stellen im Data Science Bereich?
    :param df: df, welches aus JobAgent_stage.csv erzeugt wird.
    :return: Zwei aggregierte dfs mit relativen und absoluten zahlen und ein pivot df.
    '''

    #Group by Kanton und Arbeitsort als absolute Zahlen:
    print(40 * "*")
    print("Question 2: In welchem Kanton und in welcher Stadt / Ortschaft gibt es die meisten offenen Stellen im Data Science Bereich?")

    grouped_canton_loc = df.groupby(["Kanton", "Arbeitsort"])
    abs_canton_loc = grouped_canton_loc.size().sort_values(ascending=False)
    #print(abs_canton_loc.head(5))
    #print(40 * "*")

    #Group by Kanton als absolute Zahlen:
    grouped_canton = df.groupby(["Kanton"])
    abs_canton = grouped_canton.size().sort_values(ascending=False)
    #print(abs_canton.head(5))
    #print(40 * "*")

    #Neue Variablen erstellen:
    df2 = df.copy()
    df2['Count_per_Kanton_and_Arbeitsort'] = df.groupby(["Kanton", "Arbeitsort"])["Kanton"].transform('count')
    df2['Count_per_Kanton'] = df.groupby(["Kanton"])["Kanton"].transform('count')
    #print(df2)

    #Pivot erstellen:
    #print("Dropping missing values out with no canton:")
    #print("Before drop out:",  len(df2))
    pivot_df = df2.dropna(axis = 0, subset = "Kanton")
    #print("After drop out: ", len(pivot_df))
    #print(40 * "*")
    pivoted = pd.pivot_table(pivot_df, index = "Kanton", columns = "Arbeitsort", values= ["Count_per_Kanton_and_Arbeitsort"], aggfunc='count')
    #print(pivoted)
    #print(40 * "*")
    #print(pivoted[0:1])
    #print(40 * "*")

    #Group by Kanton und Arbeisort als relative Zahlen:
    sum_canton_loc = grouped_canton_loc.size().sum()
    df2["In % von Total"] = round((df2["Count_per_Kanton_and_Arbeitsort"] / sum_canton_loc) * 100, 2)

    rel_canton_loc = df2.groupby(['Kanton', "Arbeitsort"])["In % von Total"].mean().sort_values(ascending=False)


    #Group by Kanton als relative Zahlen:
    sum_canton = grouped_canton.size().sum()
    df2["In % von Total"] = round((df2["Count_per_Kanton"] / sum_canton) * 100, 2)

    rel_canton = df2.groupby(['Kanton'])["In % von Total"].mean().sort_values(ascending=False)

    #Concat result für Kanton und Arbeitsort:
    result_canton_loc = pd.concat([abs_canton_loc, rel_canton_loc], axis=1)
    result_canton_loc.rename(columns={0: "Anzahl offener Stellen"}, inplace=True)
    print("Am meisten offenen Stellen (Top 5) sind im Kanton und der Ortschaft:")
    print(result_canton_loc)
    print(40 * "*")

    # Concat result für Kanton:
    result_canton = pd.concat([abs_canton, rel_canton], axis=1)
    result_canton.rename(columns={0: "Anzahl offener Stellen"}, inplace=True)
    print("Am meisten offenen Stellen (Top 5) sind in den Kantonen:")
    print(result_canton)


    return result_canton_loc, result_canton, pivoted

def question3(df):
    '''
    #Question 3: Wie ist das durchschnittliche Rating der Arbeitgeber nach Branche?
    :param df: df, welches aus JobAgent_stage.csv erzeugt wird.
    :return: Ein aggregierter df mit durchschnittlichen, max und Anzahl Rating der Arbeitgeber nach Branche.
    '''

    print(40 * "*")
    print("Question 3: Wie ist das durchschnittliche Rating der Arbeitgeber nach Branche?")

    #Durchschnittliches Rating berechnen:
    mean_rating = pd.Series.mean(df['Arbeitgeber_Bewertung'])
    print("Das durchschnittliche Rating aller Arbeitgeber über alle Branchen im Durchschnitt ist: ", mean_rating)

    # Durchschnittliches, maximales und Anzahl Rating pro Branche berechnen:
    print("Das durchschnittliche Rating der Arbeitgeber nach Branche ist:")

    functions = ["mean", "max", "count"]
    grouped_branche = df.groupby(["Branche"])
    functions_rating_branche = grouped_branche["Arbeitgeber_Bewertung"].agg(functions).sort_values(by = "mean", ascending=False)
    print(functions_rating_branche)

    return functions_rating_branche



def question4(df):
    '''
    #Question 4: Was ist das häufigst geforderte Mindest-Arbeitspensum?
    :param df: df, welches aus JobAgent_stage.csv erzeugt wird.
    :return: aggregierte df mit relativen und absoluten zahlen
    '''
    print(40*"*")
    print ("Question 4: Was ist das häufigst geforderte Mindest-Arbeitspensum?")

    #Group by Arbeitspensum als absolute Zahlen:
    grouped_pensum = df.groupby('Arbeitspensum_min')
    abs_pensum= grouped_pensum.size().sort_values(ascending=False, )

    pensum = abs_pensum.head(1).index[0]
    max_pensum = int(grouped_pensum.size().sort_values(ascending=False).max())
    sum_pensum = int(grouped_pensum.size().sort_values(ascending=False).sum())
    max_sum_pensum = round((max_pensum/sum_pensum) * 100,2)

    print(f"Das häufigst geforderte Mindest-Arbeitspensum ist {pensum} % und tritt insgesamt {max_pensum} mal auf. Das entspricht {max_sum_pensum} %.")
    #print(abs)

    #Neue Variable erstellen:
    df2 = df.copy()
    df2['Count_per_Arbeitspensum'] = df.groupby(["Arbeitspensum_min"])["Arbeitspensum_min"].transform('count')

    # Group by Arbeitspensum als relative Zahlen:
    sum_pensum2 = grouped_pensum.size().sum()
    df2["In % von Total"] = round((df2["Count_per_Arbeitspensum"] / sum_pensum2) * 100, 2)

    rel_pensum = df2.groupby(['Arbeitspensum_min'])["In % von Total"].mean().sort_values(ascending=False)

    # Concat result für Arbeitspensum:
    result_pensum = pd.concat([abs_pensum, rel_pensum], axis=1)
    result_pensum.rename(columns={0: "Anzahl offener Stellen"}, inplace=True)
    result_pensum.sort_values(by = ["Arbeitspensum_min","Anzahl offener Stellen"],ascending=False, inplace = True)
    print(result_pensum)

    return result_pensum

def question5(df):
    '''
    #Question 5: Kann mit dieser Source nicht beantwortet werden. Sondern nur über die Daten von Jobscout24.
    :param df: df, welches aus JobAgent_stage.csv erzeugt wird.
    :return: Keins
    '''
    print(40*"*")
    print("Question 5: Kann mit dieser Source nicht beantwortet werden. Sondern nur über die Daten von Jobscout24.")


def question6(df):
    '''
    #Question 6: Welche Programmiersprache und andere Software-Tools werden am meisten erwartet, gibt es Unterschiede zwischen den Branchen?
    :param df: df, welches aus JobAgent_stage.csv erzeugt wird.
    :return: df, was die aus absoluten und relativen Zahlen pro Branche besteht
    Die relative Kennzahl errechnet sich wie folgt: Anzahl Stellen mit dem geforderten Programmier oder Softwarekenntnise / Anzahl Stellen pro Branche
    '''

    print(40*"*")
    pd.set_option("display.max_colwidth", 30)
    print ("Question 6: Welche Programmiersprache und andere Software-Tools werden am meisten erwartet, gibt es Unterschiede zwischen den Branchen? (Komplexe Frage)" )

    #Kopien erstellen
    df_new = df[["Programmier_und_Softwarekenntnisse","Branche"]].copy()
    df2 = df.copy()

    # List_to_Check = ['ETL', 'Excel', 'Java', 'JavaScript','JS',
    #             'Linux','Machine Learning', 'PowerBI', 'Python','SAS',
    #                  'SQL', 'Tableau',  '\\sR\\s']


    #Neue Variablen erstellen:
    df_branche_groupby = df2.groupby(['Branche'])
    df_count_per_branche = df_branche_groupby.size()
    df_count_per_branche.rename("Count_per_Branche", inplace=True)
    #print(df_count_per_branche)

    #ETL
    # Counting:
    found1 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('ETL', na=False)]
    found1 = found1.assign(ETL=1)

        # Creating absolut values:
    grouped_etl = found1.groupby(["Branche"])
    abs_etl = grouped_etl.size().sort_values(ascending=False)
    abs_etl.rename("ETL", inplace=True)

        # Merge to result:
    result_etl = pd.merge(abs_etl, df_count_per_branche, on="Branche", how="outer")
    result_etl["ETL in % von Branche"] = round(
        (result_etl["ETL"] / result_etl["Count_per_Branche"] * 100), 2)
        # print(result_etl)

    #EXCEL:
        #Counting:
    found2 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('Excel', na=False)]
    found2 = found2.assign(Excel=1)

        #Creating absolut values:
    grouped_excel = found2.groupby(["Branche"])
    abs_excel  = grouped_excel.size().sort_values(ascending=False)
    abs_excel.rename("Excel", inplace = True)

        #Merge to result:
    result_excel =  pd.merge(abs_excel, df_count_per_branche, on ="Branche", how = "outer")
    result_excel["Excel in % von Branche"]= round((result_excel["Excel"] / result_excel["Count_per_Branche"] * 100),2)
    #print(result_excel)

    #JAVA
        #Counting:
    found3 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('Java', na=False)]
    found3 = found3.assign(Java=1)

        #Creating absolut values:
    grouped_java = found3.groupby(["Branche"])
    abs_java  = grouped_java.size().sort_values(ascending=False)
    abs_java.rename("Java", inplace = True)

        #Merge to result:
    result_java =  pd.merge(abs_java, df_count_per_branche, on ="Branche", how = "outer")
    result_java["Java in % von Branche"]= round((result_java["Java"] / result_java["Count_per_Branche"] * 100),2)
    #print(result_java)


    #JAVASCRIPT
    #Counting:
    found4 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains("Javascript|JS", na=False)] #  | = Javascript or JS
    found4 = found4.assign(Javascript=1)

        #Creating absolut values:
    grouped_javascript = found4.groupby(["Branche"])
    abs_javascript  = grouped_javascript.size().sort_values(ascending=False)
    abs_javascript.rename("Javascript", inplace = True)

        #Merge to result:
    result_javascript =  pd.merge(abs_javascript, df_count_per_branche, on ="Branche", how = "outer")
    result_javascript["Javascript in % von Branche"]= round((result_javascript["Javascript"] / result_javascript["Count_per_Branche"] * 100),2)
    #print(result_javascript)

    #LINUX
    found5 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains("Linux", na=False)]
    found5 = found5.assign(Linux=1)

        #Creating absolut values:
    grouped_linux = found5.groupby(["Branche"])
    abs_linux  = grouped_linux.size().sort_values(ascending=False)
    abs_linux.rename("Linux", inplace = True)

        #Merge to result:
    result_linux =  pd.merge(abs_linux, df_count_per_branche, on ="Branche", how = "outer")
    result_linux["Linux in % von Branche"]= round((result_linux["Linux"] / result_linux["Count_per_Branche"] * 100),2)
    #print(result_linux)


    #MACHINE LEARNING:
    found6 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains("R", na=False)]
    found6 = found6.assign(Machine_Learning=1)

    # Creating absolut values:
    grouped_ml = found5.groupby(["Branche"])
    abs_ml = grouped_ml.size().sort_values(ascending=False)
    abs_ml.rename("Machine Learning", inplace=True)

    # Merge to result:
    result_ml = pd.merge(abs_ml, df_count_per_branche, on="Branche", how="outer")
    result_ml["Machine Learning in % von Branche"] = round((result_ml["Machine Learning"] / result_ml["Count_per_Branche"] * 100), 2)
    # print(result_ml)

    #PYTHON:
        # Counting:
    found7 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('Python', na=False)]
    found7 = found7.assign(Python=1)

        # Creating absolut values:
    grouped_python = found7.groupby(["Branche"])
    abs_python = grouped_python.size().sort_values(ascending=False)
    abs_python.rename("Python", inplace=True)

        # Merge to result:
    result_python = pd.merge(abs_python, df_count_per_branche, on="Branche", how="outer")
    result_python["Python in % von Branche"] = round(
        (result_python["Python"] / result_python["Count_per_Branche"] * 100), 2)
        # print(result_python)

    #POWER BI
        # Counting:
    found8 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('Power BI', na=False)]
    found8 = found8.assign(Power_BI=1)

        # Creating absolut values:
    grouped_power_bi = found8.groupby(["Branche"])
    abs_power_bi = grouped_power_bi.size().sort_values(ascending=False)
    abs_power_bi.rename("Power BI", inplace=True)

     # Merge to result:
    result_power_bi = pd.merge(abs_power_bi, df_count_per_branche, on="Branche", how="outer")
    result_power_bi["Power BI in % von Branche"] = round(
        (result_power_bi["Power BI"] / result_power_bi["Count_per_Branche"] * 100), 2)
    #print(result_power_bi)

    #R
        # Counting:
    found9 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('R', na=False)]
    found9 = found9.assign(R=1)

        # Creating absolut values:
    grouped_r = found9.groupby(["Branche"])
    abs_r = grouped_r.size().sort_values(ascending=False)
    abs_r.rename("R", inplace=True)

        # Merge to result:
    result_r = pd.merge(abs_r, df_count_per_branche, on="Branche", how="outer")
    result_r["R in % von Branche"] = round((result_r["R"] / result_r["Count_per_Branche"] * 100), 2)
    #print(result_r)

    #SAS
       # Counting:
    found10 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('SAS', na=False)]
    found10 = found10.assign(SAS=1)

        # Creating absolut values:
    grouped_sas = found10.groupby(["Branche"])
    abs_sas = grouped_sas.size().sort_values(ascending=False)
    abs_sas.rename("SAS", inplace=True)

        # Merge to result:
    result_sas = pd.merge(abs_sas, df_count_per_branche, on="Branche", how="outer")
    result_sas["SAS in % von Branche"] = round((result_sas["SAS"] / result_sas["Count_per_Branche"] * 100), 2)
    #print(result_sas)


    #SQL
       # Counting:
    found11 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('SQL', na=False)]
    found11 = found11.assign(SQL=1)

        # Creating absolut values:
    grouped_sql = found11.groupby(["Branche"])
    abs_sql = grouped_sql.size().sort_values(ascending=False)
    abs_sql.rename("SQL", inplace=True)

        # Merge to result:
    result_sql = pd.merge(abs_sql, df_count_per_branche, on="Branche", how="outer")
    result_sql["SQL in % von Branche"] = round((result_sql["SQL"] / result_sql["Count_per_Branche"] * 100), 2)
    #print(result_sql)

    #TABLEAU
    # Counting:
    found12 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('Tableau', na=False)]
    found12 = found12.assign(Tableau=1)

    # Creating absolut values:
    grouped_tableau = found12.groupby(["Branche"])
    abs_tableau = grouped_tableau.size().sort_values(ascending=False)
    abs_tableau.rename("Tableau", inplace=True)

    # Merge to result:
    result_tableau = pd.merge(abs_tableau, df_count_per_branche, on="Branche", how="outer")
    result_tableau["Tableau in % von Branche"] = round((result_tableau["Tableau"] / result_tableau["Count_per_Branche"] * 100), 2)
    # print(result_tableau)


    #MERGE ALL TOGETHER:
    excel_python = pd.merge(result_python, result_excel,  left_on = "Branche", right_on = "Branche", how = "outer")
    excel_python.rename(columns = {"Count_per_Branche_x" : "Anzahl Stellen pro Branche"}, inplace = True)
    neworder = ['Anzahl Stellen pro Branche', 'Python', 'Python in % von Branche','Excel', 'Excel in % von Branche']
    excel_python = excel_python.reindex(columns=neworder)
    excel_python = excel_python.sort_values(by = "Branche", ascending=True)

    etl_excel_python = pd.merge(excel_python, result_etl,  left_on = "Branche", right_on = "Branche", how = "outer")
    etl_excel_python.drop(columns=["Count_per_Branche"],inplace=True)

    etl_excel_java_python = pd.merge(etl_excel_python, result_java, left_on="Branche", right_on="Branche", how="outer")
    etl_excel_java_python.drop(columns=["Count_per_Branche"], inplace=True)

    etl_excel_java_js_python = pd.merge(etl_excel_java_python, result_javascript, left_on="Branche", right_on="Branche", how="outer")
    etl_excel_java_js_python.drop(columns=["Count_per_Branche"], inplace=True)

    etl_excel_java_js_linux_python = pd.merge(etl_excel_java_js_python, result_linux, left_on="Branche", right_on="Branche", how="outer")
    etl_excel_java_js_linux_python.drop(columns=["Count_per_Branche"], inplace=True)

    etl_excel_java_js_linux_ml_python = pd.merge(etl_excel_java_js_linux_python, result_ml, left_on="Branche", right_on="Branche", how="outer")
    etl_excel_java_js_linux_ml_python.drop(columns=["Count_per_Branche"], inplace=True)

    etl_excel_java_js_linux_ml_python_powerbi = pd.merge(etl_excel_java_js_linux_ml_python, result_power_bi, left_on="Branche", right_on="Branche", how="outer")
    etl_excel_java_js_linux_ml_python_powerbi.drop(columns=["Count_per_Branche"], inplace=True)

    etl_excel_java_js_linux_ml_python_powerbi_r = pd.merge(etl_excel_java_js_linux_ml_python_powerbi, result_r,left_on="Branche", right_on="Branche", how="outer")
    etl_excel_java_js_linux_ml_python_powerbi_r.drop(columns=["Count_per_Branche"], inplace=True)

    etl_excel_java_js_linux_ml_python_powerbi_r_sas = pd.merge(etl_excel_java_js_linux_ml_python_powerbi_r, result_sas,left_on="Branche", right_on="Branche", how="outer")
    etl_excel_java_js_linux_ml_python_powerbi_r_sas.drop(columns=["Count_per_Branche"], inplace=True)

    etl_excel_java_js_linux_ml_python_powerbi_r_sas_sql = pd.merge(etl_excel_java_js_linux_ml_python_powerbi_r_sas, result_sql,left_on="Branche", right_on="Branche", how="outer")
    etl_excel_java_js_linux_ml_python_powerbi_r_sas_sql.drop(columns=["Count_per_Branche"], inplace=True)

    etl_excel_java_js_linux_ml_python_powerbi_r_sas_sql_tableau = pd.merge(etl_excel_java_js_linux_ml_python_powerbi_r_sas_sql, result_tableau, left_on="Branche", right_on="Branche", how="outer")
    etl_excel_java_js_linux_ml_python_powerbi_r_sas_sql_tableau.drop(columns=["Count_per_Branche"], inplace=True)

    print("Im folgenden ein Ausschnitt aus den analysierten Daten. Im Excel File sind mehr Details:\n")

    print(etl_excel_java_python)

    result_final = etl_excel_java_js_linux_ml_python_powerbi_r_sas_sql_tableau
    return result_final

def question7(df):
    '''
    #Question 7: Wie sieht der / die optimale Bewerber / Bewerberin aus? (Komplexe Frage)
    Die Idee dahinter ist es ein Stellenprofil zu erstellen, was einem angehenden Data Scientists zeigen soll,
    welche Branchenkenntnisse, Software und Programmierkenntnisse gefordert sind und was für ein Pensum er in seiner Bewerbung angeben soll.
    Ein Bewerber kann somit kritisch reflektieren welche Branchenkenntnisse er sich noch aneignen sollte
    oder welche Programmier und Softwarekenntnisse er noch lernen muss.

    Da in den Kantonen verschiedene Branchen stark vertreten sind, wird immer ein optimaler Bewerber pro Kanton berechnet.
    :param df: df, welches aus JobAgent_stage.csv erzeugt wird.
    :return: df, welches das optimale Pensum und die optimalen Branchen enthält. Die Programmier und Softwarekenntnise werden im q7_merge beantwortet.
    '''
    print(40* "*")
    print("Question 7: Wie sieht der / die optimale Bewerber / Bewerberin aus?")


    #Kopie von Dataframe
    df2 = df.copy()

    #Leeres Data Frame für die finalen Resultate erzeugen:
    bewerber_df = pd.DataFrame(columns=["Kanton", "Branche", "Pensum"])
    #print(bewerber_df)

    #Filter bei Kanton
    cantons = ["AG", "AI", "AR", "BE", "BL", "BS", "FR", "GE", "GL", "GR", "JU", "LU", "NE", "NW", "OW", "SG", "SH", "SO", "SZ", "TG", "TI", "UR", "VD", "VS", "ZG", "ZH"]
    #print(len(cantons))

    for canton_input in cantons:
        #print(canton_input)
        filtered_by_canton = df2["Kanton"]==canton_input


        df2_filtered = df2[filtered_by_canton]

        #Absoluten Zahlen für Branche und Pensum:
        grouped_pensum= df2_filtered.groupby(["Arbeitspensum_min"])
        grouped_branche = df2_filtered.groupby(["Branche"])
        abs_branche = grouped_branche.size().sort_values(ascending=False)
        abs_pensum = grouped_pensum.size().sort_values(ascending=False)

        # Count per Branche und Count per Pensum als neue Variablen:
        df2_filtered['Count_per_Branche'] = df2_filtered.groupby('Branche')['Branche'].transform('count')
        df2_filtered['Count_per_Pensum'] = df2_filtered.groupby('Arbeitspensum_min')['Arbeitspensum_min'].transform('count')

        # Group by Branche und Pensum mit Relativen Zahlen:
        sum_branche = grouped_branche.size().sum()
        sum_pensum = grouped_pensum.size().sum()
        df2_filtered["In % von Total"] = round((df2_filtered["Count_per_Branche"] / sum_branche) * 100, 2)
        df2_filtered["In % von Total"] = round((df2_filtered["Count_per_Pensum"] / sum_branche) * 100, 2)

        rel_branche = df2_filtered.groupby(['Branche'])["In % von Total"].mean().sort_values(ascending=False)
        rel_pensum = df2_filtered.groupby(['Arbeitspensum_min'])["In % von Total"].mean().sort_values(ascending=False)

        # Concat Results:
        result_branche = pd.concat([abs_branche, rel_branche], axis=1)
        result_branche.rename(columns={0: "Anzahl offener Stellen"}, inplace=True)
        result_branche_list = result_branche["In % von Total"].values.tolist()

        result_pensum = pd.concat([abs_pensum, rel_pensum], axis=1)
        result_pensum.rename(columns={0: "Anzahl offener Stellen"}, inplace=True)
        result_pensum_list = result_pensum["In % von Total"].values.tolist()

        #Berechnung des optimalen Bewerber nach Branche:
        iter_result_branche_list = iter(result_branche_list)
        sum_branche = 0
        counter = 0

        while iter_result_branche_list:
            try:
                if sum_branche > 50.00:
                    break
                else:
                    counter += 1
                    i = next(iter_result_branche_list)
                    sum_branche = sum_branche + i
            except StopIteration:
                #print("Sorry der Kanton hat keine Stellen")
                break

        list_bewerber_branche = list(result_branche.index[0:counter])
        branche = " ,".join(str(i) for i in list_bewerber_branche)

        # Berechnung des optimalen Bewerber nach Pensum:
        iter_result_pensum_list = iter(result_pensum_list)
        sum_pensum = 0
        counter2 = 0

        while iter_result_pensum_list:
            try:
                if sum_pensum > 50.00:
                    break
                else:
                    counter2 += 1
                    i = next(iter_result_pensum_list)
                    sum_pensum = sum_pensum + i
            except StopIteration:
                #print("Sorry der Kanton hat keine Stellen")
                break

        list_bewerber_pensum = list(result_pensum.index[0:counter2])
        pensum = " ,".join(str(i) for i in list_bewerber_pensum)

        #Zuweisung der Berechnung des ptimalen Bewerber nach Branche zum Dataframe
        dict = {"Kanton" : canton_input, "Branche" : branche, "Pensum" : pensum}
        bewerber_df = bewerber_df.append(dict, ignore_index = True)

    print("Ergebnis ohne Programmier und Softwarekenntnise. Diese sind nur im Excel zu finden:")
    print(bewerber_df)

    return bewerber_df

def question7_ergänzung(df):
    '''
    damit question 7 nicht zu groß wird, wird es um eine zusätzliche funktion ergänzt.
    diese funktion kümmert sich nur um die programmierkenntnisse nach Kanton. grundsätzlich muss man dafür die sehr lange logik von question 6 nehmen,
    aber nicht bei Branche groupen sondern nach kanton
    :param df: df, welches aus JobAgent_stage.csv erzeugt wird.
    :return: df, welches Programmier und Softwarekenntnise gruppiert nach Kantonen enthält. Es wird im q7_merge mit question7 gemerged.
    '''

    # Kopien erstellen
    df_new = df[["Programmier_und_Softwarekenntnisse", "Kanton"]].copy()
    df2 = df.copy()

    # List_to_Check = ['ETL', 'Excel', 'Java', 'JavaScript','JS',
    #             'Linux','Machine Learning', 'PowerBI', 'Python','SAS',
    #                  'SQL', 'Tableau',  '\\sR\\s']

    # Neue Variablen erstellen:
    df_Kanton_groupby = df2.groupby(['Kanton'])
    df_count_per_Kanton = df_Kanton_groupby.size()
    df_count_per_Kanton.rename("Count_per_Kanton", inplace=True)
    # print(df_count_per_Kanton)

    # ETL
    # Counting:
    found1 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('ETL', na=False)]
    found1 = found1.assign(ETL=1)


    # Creating absolut values:
    grouped_etl = found1.groupby(["Kanton"])
    abs_etl = grouped_etl.size().sort_values(ascending=False)
    abs_etl.rename("ETL", inplace=True)


    # Merge to result:
    result_etl = pd.merge(abs_etl, df_count_per_Kanton, on="Kanton", how="outer")
    result_etl["ETL in % von Kanton"] = round(
        (result_etl["ETL"] / result_etl["Count_per_Kanton"] * 100), 2)
    # print(result_etl)

    # EXCEL:
    # Counting:
    found2 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('Excel', na=False)]
    found2 = found2.assign(Excel=1)

    # Creating absolut values:
    grouped_excel = found2.groupby(["Kanton"])
    abs_excel = grouped_excel.size().sort_values(ascending=False)
    abs_excel.rename("Excel", inplace=True)

    # Merge to result:
    result_excel = pd.merge(abs_excel, df_count_per_Kanton, on="Kanton", how="outer")
    result_excel["Excel in % von Kanton"] = round((result_excel["Excel"] / result_excel["Count_per_Kanton"] * 100), 2)
    # print(result_excel)

    # JAVA
    # Counting:
    found3 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('Java', na=False)]
    found3 = found3.assign(Java=1)

    # Creating absolut values:
    grouped_java = found3.groupby(["Kanton"])
    abs_java = grouped_java.size().sort_values(ascending=False)
    abs_java.rename("Java", inplace=True)

    # Merge to result:
    result_java = pd.merge(abs_java, df_count_per_Kanton, on="Kanton", how="outer")
    result_java["Java in % von Kanton"] = round((result_java["Java"] / result_java["Count_per_Kanton"] * 100), 2)
    # print(result_java)

    # JAVASCRIPT
    # Counting:
    found4 = df_new[
        df_new["Programmier_und_Softwarekenntnisse"].str.contains("Javascript|JS", na=False)]  # | = Javascript or JS
    found4 = found4.assign(Javascript=1)

    # Creating absolut values:
    grouped_javascript = found4.groupby(["Kanton"])
    abs_javascript = grouped_javascript.size().sort_values(ascending=False)
    abs_javascript.rename("Javascript", inplace=True)

    # Merge to result:
    result_javascript = pd.merge(abs_javascript, df_count_per_Kanton, on="Kanton", how="outer")
    result_javascript["Javascript in % von Kanton"] = round(
        (result_javascript["Javascript"] / result_javascript["Count_per_Kanton"] * 100), 2)
    # print(result_javascript)

    # LINUX
    found5 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains("Linux", na=False)]
    found5 = found5.assign(Linux=1)

    # Creating absolut values:
    grouped_linux = found5.groupby(["Kanton"])
    abs_linux = grouped_linux.size().sort_values(ascending=False)
    abs_linux.rename("Linux", inplace=True)

    # Merge to result:
    result_linux = pd.merge(abs_linux, df_count_per_Kanton, on="Kanton", how="outer")
    result_linux["Linux in % von Kanton"] = round((result_linux["Linux"] / result_linux["Count_per_Kanton"] * 100), 2)
    # print(result_linux)

    # MACHINE LEARNING:
    found6 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains("R", na=False)]
    found6 = found6.assign(Machine_Learning=1)

    # Creating absolut values:
    grouped_ml = found5.groupby(["Kanton"])
    abs_ml = grouped_ml.size().sort_values(ascending=False)
    abs_ml.rename("Machine Learning", inplace=True)

    # Merge to result:
    result_ml = pd.merge(abs_ml, df_count_per_Kanton, on="Kanton", how="outer")
    result_ml["Machine Learning in % von Kanton"] = round(
        (result_ml["Machine Learning"] / result_ml["Count_per_Kanton"] * 100), 2)
    # print(result_ml)

    # PYTHON:
    # Counting:
    found7 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('Python', na=False)]
    found7 = found7.assign(Python=1)

    # Creating absolut values:
    grouped_python = found7.groupby(["Kanton"])
    abs_python = grouped_python.size().sort_values(ascending=False)
    abs_python.rename("Python", inplace=True)

    # Merge to result:
    result_python = pd.merge(abs_python, df_count_per_Kanton, on="Kanton", how="outer")
    result_python["Python in % von Kanton"] = round(
        (result_python["Python"] / result_python["Count_per_Kanton"] * 100), 2)
    # print(result_python)

    # POWER BI
    # Counting:
    found8 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('Power BI', na=False)]
    found8 = found8.assign(Power_BI=1)

    # Creating absolut values:
    grouped_power_bi = found8.groupby(["Kanton"])
    abs_power_bi = grouped_power_bi.size().sort_values(ascending=False)
    abs_power_bi.rename("Power BI", inplace=True)

    # Merge to result:
    result_power_bi = pd.merge(abs_power_bi, df_count_per_Kanton, on="Kanton", how="outer")
    result_power_bi["Power BI in % von Kanton"] = round(
        (result_power_bi["Power BI"] / result_power_bi["Count_per_Kanton"] * 100), 2)
    # print(result_power_bi)

    # R
    # Counting:
    found9 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('R', na=False)]
    found9 = found9.assign(R=1)

    # Creating absolut values:
    grouped_r = found9.groupby(["Kanton"])
    abs_r = grouped_r.size().sort_values(ascending=False)
    abs_r.rename("R", inplace=True)

    # Merge to result:
    result_r = pd.merge(abs_r, df_count_per_Kanton, on="Kanton", how="outer")
    result_r["R in % von Kanton"] = round((result_r["R"] / result_r["Count_per_Kanton"] * 100), 2)
    # print(result_r)

    # SAS
    # Counting:
    found10 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('SAS', na=False)]
    found10 = found10.assign(SAS=1)

    # Creating absolut values:
    grouped_sas = found10.groupby(["Kanton"])
    abs_sas = grouped_sas.size().sort_values(ascending=False)
    abs_sas.rename("SAS", inplace=True)

    # Merge to result:
    result_sas = pd.merge(abs_sas, df_count_per_Kanton, on="Kanton", how="outer")
    result_sas["SAS in % von Kanton"] = round((result_sas["SAS"] / result_sas["Count_per_Kanton"] * 100), 2)
    # print(result_sas)

    # SQL
    # Counting:
    found11 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('SQL', na=False)]
    found11 = found11.assign(SQL=1)

    # Creating absolut values:
    grouped_sql = found11.groupby(["Kanton"])
    abs_sql = grouped_sql.size().sort_values(ascending=False)
    abs_sql.rename("SQL", inplace=True)

    # Merge to result:
    result_sql = pd.merge(abs_sql, df_count_per_Kanton, on="Kanton", how="outer")
    result_sql["SQL in % von Kanton"] = round((result_sql["SQL"] / result_sql["Count_per_Kanton"] * 100), 2)
    # print(result_sql)

    # TABLEAU
    # Counting:
    found12 = df_new[df_new["Programmier_und_Softwarekenntnisse"].str.contains('Tableau', na=False)]
    found12 = found12.assign(Tableau=1)

    # Creating absolut values:
    grouped_tableau = found12.groupby(["Kanton"])
    abs_tableau = grouped_tableau.size().sort_values(ascending=False)
    abs_tableau.rename("Tableau", inplace=True)

    # Merge to result:
    result_tableau = pd.merge(abs_tableau, df_count_per_Kanton, on="Kanton", how="outer")
    result_tableau["Tableau in % von Kanton"] = round(
        (result_tableau["Tableau"] / result_tableau["Count_per_Kanton"] * 100), 2)
    # print(result_tableau)

    # MERGE ALL TOGETHER:
    excel_python = pd.merge(result_python, result_excel, left_on="Kanton", right_on="Kanton", how="outer")
    excel_python.rename(columns={"Count_per_Kanton_x": "Anzahl Stellen pro Kanton"}, inplace=True)
    neworder = ['Anzahl Stellen pro Kanton', 'Python', 'Python in % von Kanton', 'Excel', 'Excel in % von Kanton']
    excel_python = excel_python.reindex(columns=neworder)
    excel_python = excel_python.sort_values(by="Kanton", ascending=True)

    etl_excel_python = pd.merge(excel_python, result_etl, left_on="Kanton", right_on="Kanton", how="outer")
    etl_excel_python.drop(columns=["Count_per_Kanton"], inplace=True)

    etl_excel_java_python = pd.merge(etl_excel_python, result_java, left_on="Kanton", right_on="Kanton", how="outer")
    etl_excel_java_python.drop(columns=["Count_per_Kanton"], inplace=True)

    etl_excel_java_js_python = pd.merge(etl_excel_java_python, result_javascript, left_on="Kanton", right_on="Kanton",
                                        how="outer")
    etl_excel_java_js_python.drop(columns=["Count_per_Kanton"], inplace=True)

    etl_excel_java_js_linux_python = pd.merge(etl_excel_java_js_python, result_linux, left_on="Kanton",
                                              right_on="Kanton", how="outer")
    etl_excel_java_js_linux_python.drop(columns=["Count_per_Kanton"], inplace=True)

    etl_excel_java_js_linux_ml_python = pd.merge(etl_excel_java_js_linux_python, result_ml, left_on="Kanton",
                                                 right_on="Kanton", how="outer")
    etl_excel_java_js_linux_ml_python.drop(columns=["Count_per_Kanton"], inplace=True)

    etl_excel_java_js_linux_ml_python_powerbi = pd.merge(etl_excel_java_js_linux_ml_python, result_power_bi,
                                                         left_on="Kanton", right_on="Kanton", how="outer")
    etl_excel_java_js_linux_ml_python_powerbi.drop(columns=["Count_per_Kanton"], inplace=True)

    etl_excel_java_js_linux_ml_python_powerbi_r = pd.merge(etl_excel_java_js_linux_ml_python_powerbi, result_r,
                                                           left_on="Kanton", right_on="Kanton", how="outer")
    etl_excel_java_js_linux_ml_python_powerbi_r.drop(columns=["Count_per_Kanton"], inplace=True)

    etl_excel_java_js_linux_ml_python_powerbi_r_sas = pd.merge(etl_excel_java_js_linux_ml_python_powerbi_r, result_sas,
                                                               left_on="Kanton", right_on="Kanton", how="outer")
    etl_excel_java_js_linux_ml_python_powerbi_r_sas.drop(columns=["Count_per_Kanton"], inplace=True)

    etl_excel_java_js_linux_ml_python_powerbi_r_sas_sql = pd.merge(etl_excel_java_js_linux_ml_python_powerbi_r_sas,
                                                                   result_sql, left_on="Kanton", right_on="Kanton",
                                                                   how="outer")
    etl_excel_java_js_linux_ml_python_powerbi_r_sas_sql.drop(columns=["Count_per_Kanton"], inplace=True)

    etl_excel_java_js_linux_ml_python_powerbi_r_sas_sql_tableau = pd.merge(
        etl_excel_java_js_linux_ml_python_powerbi_r_sas_sql, result_tableau, left_on="Kanton", right_on="Kanton",
        how="outer")
    etl_excel_java_js_linux_ml_python_powerbi_r_sas_sql_tableau.drop(columns=["Count_per_Kanton"], inplace=True)


    result_final = etl_excel_java_js_linux_ml_python_powerbi_r_sas_sql_tableau

    ####Jetzt kommt der neue Kram:

    result_final = result_final.drop(columns=["ETL", "Excel", "Java", "Javascript", "Linux", "Machine Learning", "Power BI", "R", "Python", "SAS", "SQL", "Tableau", "Anzahl Stellen pro Kanton"])
    result_final = result_final.sort_values(by='AG', axis=1, ascending= False)
    #print(result_final)

    return result_final

def question7_merge(q7, q7add):

    q7_merge = pd.merge(q7, q7add, left_on="Kanton", right_on="Kanton",
                                        how="outer")

    return q7_merge

def create_xlsx(result, question_number):
    '''
    Eine zentrale Funktion, die alle Excels erzeugt.
    :param result:
    :param question_number:
    :return: 6 verschiedene Excels.
    '''
    if int(question_number) == 1:
        df = result #unpacking tuple

        with pd.ExcelWriter(f"Result_Question_{question_number}.xlsx") as writer:
            df.to_excel(writer, sheet_name= str("Branche"))
            #df2.to_excel(writer, sheet_name=str("relative"))

    elif int(question_number) == 2:
        df,df2,pivot = result #unpacking tuple

        with pd.ExcelWriter(f"Result_Question_{question_number}.xlsx") as writer:
            df.to_excel(writer, sheet_name=str("Kanton_und_Ortschaft"))
            df2.to_excel(writer, sheet_name=str("Kanton"))
            pivot.to_excel(writer, sheet_name=str("Pivot"))

    elif int(question_number) == 3:
        df = result

        with pd.ExcelWriter(f"Result_Question_{question_number}.xlsx") as writer:
            df.to_excel(writer, sheet_name=str("Rating_nach_Branche"))


    elif int(question_number) == 4:
        df = result

        with pd.ExcelWriter(f"Result_Question_{question_number}.xlsx") as writer:
            df.to_excel(writer, sheet_name=str("Arbeitspensum"))

    elif int(question_number) == 6:
        df = result

        with pd.ExcelWriter(f"Result_Question_{question_number}.xlsx") as writer:
            df.to_excel(writer, sheet_name=str("Programmierkenntn. n. Branche"))

    elif int(question_number) == 7:
        df = result

        with pd.ExcelWriter(f"Result_Question_{question_number}.xlsx") as writer:
            df.to_excel(writer, sheet_name=str("Optimaler Bewerber nach Kanton"))

    elif int(question_number) == 8:
        df = result

        with pd.ExcelWriter(f"Result_Question_{question_number}.xlsx") as writer:
            df.to_excel(writer, sheet_name=str("Optimaler Bewerber nach Kanton"))


def main():
    df = open_csv()
    q1= question1(df)
    csv1 = create_xlsx(q1,question_number= "01")
    q2 = question2(df)
    csv2 = create_xlsx(q2,question_number= "02")
    q3 = question3(df)
    csv3 = create_xlsx(q3, question_number="03")
    q4 = question4(df)
    csv4 = create_xlsx(q4, question_number="04")
    q5 = question5(df)
    #csv5 = create_xlsx(q5, question_number="05")
    q6 = question6(df)
    csv6 = create_xlsx(q6, question_number="06")
    q7 = question7(df)
    q7_add = question7_ergänzung(df)
    q7_merge = question7_merge(q7, q7_add)
    csv7 = create_xlsx(q7_merge, question_number="07")

if __name__ == "__main__":
    main()



