#Dieses Skript merged alle Sources zusammen (df_jobs, df_company_size und df_wiki)
#und transformiert das merged dataframe nach den definierten Standards gemäß ETL Plan für Arbeitspensum und Branche.

# Benötigte Module
import pandas as pd

# Settings für PyCharm
desired_width = 400
pd.set_option('display.width', desired_width)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_columns', 10)
pd.set_option("display.max_colwidth", 25)

#Load Data
def load_first_source():
    #First source (cleaned):
    csv_name1 = "Transform/JobAgent_cleaned.csv"
    path1 = (f"/home/student/Cloud/Owncloud/SyncVM/CIP Project/JobAgent_T/{csv_name1}")
    df_cleaned = pd.read_csv(path1)
    return df_cleaned

def load_second_source():
    #Secound source (cleaned):
    csv_name2 = "Transform/JobAgent_company_size_cleaned.csv"
    path2 = (f"/home/student/Cloud/Owncloud/SyncVM/CIP Project/JobAgent_T/{csv_name2}")
    df_company_size_cleaned = pd.read_csv(path2)
    return df_company_size_cleaned

def load_third_source():
    ## Third source:
    csv_name3 = "Extract_Beautiful_Soup/Wiki_src.csv"
    path3 = (f"/home/student/Cloud/Owncloud/SyncVM/CIP Project/JobAgent_T/{csv_name3}")
    df_wiki = pd.read_csv(path3)
    df_wiki_canton = df_wiki.loc[:, "Offizieller Gemeindename": "Kanton"] #Slice only to the needed columns
    # print(df_wiki_canton.head())
    return df_wiki_canton


def merge(df_cleaned, df_wiki_canton):
    '''
    Wiki und df_cleaned (enthält die Infos per Jobs) werden zusammen gemerged
    :param df_cleaned
    :param df_wiki_canton
    :return: df_merge
    '''

    #Merge anhand von Arbeitsort:
    print("Anzahl der Datensätze vor merge", len(df_cleaned))
    df_merge = pd.merge(df_cleaned, df_wiki_canton, left_on="Arbeitsort", right_on="Offizieller Gemeindename",
                        how="left")
    print(df_merge.head())
    print("Anzahl der Datensätze nach merge", len(df_merge))
    print("Der Index ist: \n", df_merge.index)
    print(30 * "*")

    # Zeige mir alle an, die nicht gematched haben:
    NaN_values_canton = df_merge[df_merge["Kanton"].isna()]
    print("Anzahl der Datensätze mit Null-Wert:", len(NaN_values_canton))
    print(NaN_values_canton.groupby("Arbeitsort").count())
    # print(NaN_values_canton)
    print(30 * "*")

    # Wir haben Problem mit der Ortschaft Holderbank, da sie in zwei Kantonen vorkommt, aber wir haben keinen Hinweis auf den Kanton und müssen manuell helfen:
    idx = df_merge["Arbeitsort"].str.find("Holderbank")
    a = df_merge['Arbeitsort'].where(df_merge['Arbeitsort'] == "Holderbank")
    list_a = list(a)
    pos = list_a.index("Holderbank")  # find position of it
    print(f"Bitte die Position {pos} im dataframe checken für die Ortschaft Holderbank")
    print(30 * "*")

    # Quick Google: Die Firma befindet sich im Kanton Aargau!
    # Reclean:
    df_cleaned["Arbeitsort"].replace({
        "Holderbank": "Holderbank (AG)"}, inplace=True)

    #Erneuter merge nach Cleaning der Daten oben:
    df_merge = pd.merge(df_cleaned, df_wiki_canton, left_on="Arbeitsort", right_on="Offizieller Gemeindename",
                        how="left")

    # Zeige mir alle an, die nicht gematched haben:
    NaN_values_canton = df_merge[df_merge["Kanton"].isna()]
    print("Anzahl der Datensätze mit Null-Wert:", len(NaN_values_canton))
    print(NaN_values_canton.groupby("Arbeitsort").count())
    # --> Schweiz, Liechtenstein, Deutschland, etc. --> sind die definierten Null Values und dürfen bestehen bleiben.

    #Drop the column
    df_merge = df_merge.drop(columns=["Offizieller Gemeindename", "Unnamed: 0"])  # doppelt sonst

    #Results:
    print(df_merge.columns)
    print(df_merge.index)
    print(30 * "*")
    return df_merge

def merge2(df_merge, df_company_size_cleaned):
    '''
    df_company size (enthält Unternehmensgrössen per Job) und df_merge (enthält die Infos per Jobs und die Kantone) werden zusammen gemerged
    :param df_merge:
    :param df_company_size_cleaned:
    :return: Das Resultat ist das finale gemerged df. --> df_all_merged
    '''

    #Settings:
    pd.set_option('display.max_columns', 10)
    pd.set_option("display.max_colwidth", 25)

    #Merge:
    print("Anzahl der Datensätze vor merge", len(df_merge))
    df_all_merged = pd.merge(df_merge, df_company_size_cleaned, left_on="Primärschlüssel",
                         right_on="Primärschlüssel", how="left")

    # print(df_all_merged.head())
    print("Anzahl der Datensätze nach merge", len(df_all_merged))

    # Zeige mir alle an, die nicht gematched haben:
    NaN_company_size = df_all_merged[df_all_merged["Unternehmensgrösse"].isna()]
    print("Anzahl der Datensätze mit Null-Wert:", len(NaN_company_size))
    print("Zeige mir alle an, die nicht gematched haben:")
    print(NaN_company_size)

    #Drop column:
    df_all_merged = df_all_merged.drop(columns=["Unnamed: 0"])

    #Print results:
    print(df_all_merged.columns)
    print(df_all_merged.index)

    #Mach ein dataframe als Zwischenziel:
    print(30 * "*")
    df_all_merged.to_csv('JobAgent_cleaned_and_merged.csv', index=True)
    return df_all_merged

def transform_arbeitspensum(df_all_merged):
    '''
    :param df_all_merged: df was aus allen merged Sources besteht
    :return: df, was nach dem vorgaben im etl prozess plan transformiert wurde. (als FLOAT/INT)
    '''
    # Make a copy
    pensum_list = df_all_merged["Arbeitspensum"].values.tolist()
    print("Anzahl der transformierten Pensen vor Transformation", len(pensum_list))
    # print(pensum_list)

    #Die Pensen werden als Float und Int mit einer Obergrenze (max) und Untergrenze (min) definiert:
    Arbeitspensum_min_list = []
    Arbeitspensum_max_list = []

    for pensum in pensum_list:
        pensum_edit = pensum.replace("%", "")
        pensum_edit2 = pensum_edit.partition("-") #80-100 % soll getrennt werden z.B. in: 80,-,100
        untergrenze, platzhalter, obergrenze = pensum_edit2  # ergebnis ist ein tupel
        if obergrenze != "":
            Arbeitspensum_min = int(untergrenze)
            Arbeitspensum_max = int(obergrenze)
            #new_pensum = (obergrenze + untergrenze) / 2  # "Goldene Mitte"
        else:
            Arbeitspensum_min = (untergrenze)
            Arbeitspensum_max =  None

        Arbeitspensum_min_list.append(Arbeitspensum_min)
        Arbeitspensum_max_list.append(Arbeitspensum_max)

    #print(new_pensum_list)
    print("Anzahl der transformierten Pensen nach Transformation",len(Arbeitspensum_min_list))
    print(30 * "*")

    # Drop the old column
    df_all_merged_transformed = df_all_merged.drop("Arbeitspensum", axis=1)

    # Create new columns and put the new values in:
    pd.set_option("display.max_colwidth", 15)
    df_all_merged_transformed["Arbeitspensum_min"] = Arbeitspensum_min_list
    df_all_merged_transformed["Arbeitspensum_max"] = Arbeitspensum_max_list

    #Result:
    print("Darstellung von 5 transformierten Datensätzen. Beachte Arbeitspensum ist jetzt eine Zahl!")
    print(df_all_merged_transformed.loc[0:5, "Stellentitel": "Arbeitspensum_max"])

    #Umformen von Arbeitspensum_min und Arbeitspensum_max zum Datentyp INT / FLOAT:
    df_all_merged_transformed['Arbeitspensum_min'] = df_all_merged_transformed['Arbeitspensum_min'].astype('int')
    df_all_merged_transformed['Arbeitspensum_max'] = df_all_merged_transformed['Arbeitspensum_max'].astype('float') #muss float sein, da Datentyp int nicht NA umgehen kann
    print(30* "*")

    #Check ob alle Datentypen jetzt stimmen:
    print(df_all_merged_transformed.info())
    print(30 * "*")

    return df_all_merged_transformed

def transform_branche(df_all_merged_transformed):
    '''
    :param df_all_merged_transformed: df was aus allen merged Sources besteht und nach Pensum transformierti st.
    :return: ein df wird returned was alle Transformationen nach Branche enthält, wie im ETL Prozes Plan definiert.
    '''
    #Make a copy
    df_all_merged_transformed2 = df_all_merged_transformed

    #Slice:
    branchen_list = df_all_merged_transformed2["Branche"].values.tolist()
    print("Anzahl der transformierten Branchen vor Transformation", len(branchen_list))
    # print(branchen_list)

    #Die Jobs enthalten Branchen, die anders heissen als bei Jobscout24.
    # Wie im ETL Prozess Plan definert, werden sie nach den Bezeichnungen von Jobscout24 umbenannt.
    new_branchen_list = []

    for branche in branchen_list:
        if branche == "Architektur und Planung":
            new_branche = "Immobilien" #so heißt es in Jobscout24
        elif branche == "Bildung":
            new_branche = "Bildungswesen / Schulen"
        elif branche == "Detailhandel":
            new_branche = "Detailhandel / Grosshandel / Vertrieb"
        elif branche == "Elektro- und Medizinaltechnik, Optik":
            new_branche = "Maschinenindustrie / Elektroindustrie"
        elif branche == "Energieversorgung":
            new_branche = "Energiewirtschaft / Wasserwirtschaft"
        elif branche == "Fahrzeugbau":
            new_branche = "Automobilindustrie"
        elif branche == "Fahrzeughandel und -unterhalt":
            new_branche = "Automobilindustrie"
        elif branche == "Finanzdienstleistungen":
            new_branche = "Banking / Finance"
        elif branche == "Forschung":
            new_branche = "Wissenschaft / Forschung"
        elif branche == "Gastronomie und Hotellerie":
            new_branche = "Gastgewerbe / Hotellerie / Tourismus"
        elif branche == "Gesundheitswesen":
            new_branche = "Gesundheitswesen / Spitäler"
        elif branche == "Glas":
            new_branche = "Industrie / Produktion"
        elif branche == "Grosshandel":
            new_branche = "Detailhandel / Grosshandel / Vertrieb"
        elif branche == "Holz und Papier":
            new_branche = "Industrie / Produktion"
        elif branche == "Informatik":
            new_branche = "Informatik / Neue Medien"
        elif branche == "Land- und Forstwirtschaft":
            new_branche = "Agrarwirtschaft / Forstwirtschaft"
        elif branche == "Lebensmittel":
            new_branche = "Nahrungsmittelindustrie / Genussmittelindustrie"
        elif branche == "Luftfahrt":
            new_branche = "Logistik / Transport / Verkehr"
        elif branche == "Marketing und Kommunikation":
            new_branche = "Medien / Grafik / Druck / Papier"
        elif branche == "Maschinenbau":
            new_branche = "Maschinenindustrie / Elektroindustrie"
        elif branche == "Metallindustrie":
            new_branche = "Industrie / Produktion"
        elif branche == "Möbel":
            new_branche = "Detailhandel / Grosshandel / Vertrieb"
        elif branche == "NPO":
            new_branche = "Öffentliche Verwaltung / Verbände / Behörden"
        elif branche == "Öffentliche Verwaltung":
            new_branche = "Öffentliche Verwaltung / Verbände / Behörden"
        elif branche == "Pharma und Chemie":
            new_branche = "Chemie / Pharma / Biotechnologie / Medizinaltechnik"
        elif branche == "Rechts- und Unternehmensberatung":
            new_branche = "Consulting / Treuhand / Recht"
        elif branche == "Sozialversicherung":
            new_branche = "Versicherungen / Krankenkassen"
        elif branche == "Sport, Kultur und Unterhaltung":
            new_branche = "Sportindustrie / Freizeitindustrie"
        elif branche == "Textilgewerbe":
            new_branche = "Mode / Textil"
        elif branche == "Tourismus":
            new_branche = "Gastgewerbe / Hotellerie / Tourismus"
        elif branche == "Uhren und Schmuck":
            new_branche = "Uhren / Schmuck / Optik"
        elif branche == "Umwelttechnik":
            new_branche = "Ingenieurwesen"
        elif branche == "Verkehr und Transport":
            new_branche = "Logistik / Transport / Verkehr"
        elif branche == "Verlags- und Druckbranche":
            new_branche = "Medien / Grafik / Druck / Papier"
        elif branche == "Versicherungen":
            new_branche = "Versicherungen / Krankenkassen"
        elif branche == "Wasserversorgung":
            new_branche = "Energiewirtschaft / Wasserwirtschaft"
        else:
            new_branche = branche

        new_branchen_list.append(new_branche)

    #print(new_branchen_list)
    print("Anzahl der transformierten Branchen nach Transformation", len(new_branchen_list))
    print(30 * "*")

    # Drop the old column
    df_all_merged_transformed2 = df_all_merged_transformed2.drop("Branche", axis=1)

    # Create a a new column and put the new values in:
    pd.set_option("display.max_colwidth", 15)
    df_all_merged_transformed2["Branche"] = new_branchen_list

    #Result:
    print("Darstellung von 5 transformierten Datensätzen. Beachte Branche ist jetzt gleich wie die Branchen bei Jobscout gemäß ETL Plan definiert !")
    print(df_all_merged_transformed2.loc[0:5, "Stellentitel": "Branche"])
    print(30 * "*")
    pd.set_option("display.max_colwidth", 30)
    print(df_all_merged_transformed2.groupby('Branche').count())
    print(30 * "*")
    return df_all_merged_transformed2

def create_csv1(df_all_merged):
    df_all_merged.to_csv('JobAgent_cleaned_and_merged.csv', index=True)

def create_csv2(df_all_merged_transformed2):
    #Umbenennen von zwei Variablen, damit es besser fürs Loading ist:
    df_all_merged_transformed2 = df_all_merged_transformed2.rename(columns = {"Programmier-und Softwarekenntnisse" : "Programmier_und_Softwarekenntnisse"})
    df_all_merged_transformed2 = df_all_merged_transformed2.rename(index ={"": "JobID"})
    df_all_merged_transformed2.index = df_all_merged_transformed2.index.set_names(['JobNr'])
    #Result:
    df_all_merged_transformed2.to_csv('JobAgent_stage.csv', index=True, sep=";")
    #Transformation is finished!

def main():
    df_cleaned= load_first_source()
    df_company_size_cleaned = load_second_source()
    df_wiki_canton = load_third_source ()

    df_merge= merge(df_cleaned,df_wiki_canton)
    #create_csv1(df_merge) #nur für Testzwecke
    df_all_merged = merge2(df_merge, df_company_size_cleaned)
    create_csv1(df_all_merged)

    df_all_merged_transformed = transform_arbeitspensum(df_all_merged)
    df_all_merged_transformed2 = transform_branche(df_all_merged_transformed)
    create_csv2(df_all_merged_transformed2)


if __name__ == "__main__":
    main()
