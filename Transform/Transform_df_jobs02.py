#Dieses Skript cleaned die Source df_jobs

# Benötigte Module
import pandas as pd
from langdetect import detect, DetectorFactory
import re


# Settings für PyCharm
desired_width = 400
pd.set_option('display.width', desired_width)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_columns', 10)
pd.set_option("display.max_colwidth", 25)


def load_data_and_inspect():
    ## First source
    csv_name = "Extract_Beautiful_Soup/JobAgent_src.csv"
    path = (f"/home/student/Cloud/Owncloud/SyncVM/CIP Project/JobAgent_T/{csv_name}")
    df_jobs = pd.read_csv(path, index_col=["Unnamed: 0"], na_values=["NA", "set()"])
    df_jobs.rename(columns={"Unnamed: 0": "Index"})

    df_jobs.info()
    print(30 * "*")
    print(df_jobs.shape)
    print(30 * "*")
    print(df_jobs.head())
    print(30 * "*")
    print(df_jobs.describe())
    print(30*"*")
    print(df_jobs.describe(include=['object']))
    print(30 * "*")
    # Slicing:
    # print(df_jobs.loc[0:5,"Programmier-und Softwarekenntnisse":"Arbeitspensum"])  #first 5 entries --> label based.
    # print(df_jobs.iloc[0:5,5:8]) #first 4 entries --> position based.
    return df_jobs


def data_cleaning_missing_values(df_jobs):
    '''
    Diese Funktion prüft nach missing values und dropped Datensätze, wenn Indices fehlen, ein definerter Thresh nicht erfüllt ist
    oder wenn eine Analyse zu einem Bereinigungsergebnis führt. Die Analyse ist individuell pro source.
    :param df_jobs
    :return: df_jobs_dropped
    '''

    # Checken auf Null Values:
    print("Testen auf Null Values:")
    print(pd.isnull(df_jobs))
    print(30 * "*")

    # Erster Schritt: Drop missing indices.
    print("Vor dem Droput der Missing Indices: ", len(df_jobs))
    df_jobs.index.dropna
    print("Nach dem Dropout der Missing Indices:", len(df_jobs))

    # Zweiter Schritt: Analyse
    # print(df_jobs.loc[df_jobs.isna().any(1)]) #Kein sinnvoller Code. Alle zeilen werden ausgegeben, da Datum_Veröffentlichung  per Definition nur leere Werte hat.
    print("Folgende Columns enthalten Missing Values:", df_jobs.columns[df_jobs.isna().any(0)].tolist())

        # Visual checks with groupby statements:
        # groupby = df_jobs_dropped_3.groupby('Branche')
        # print(groupby.count())

    NaN_values_rating = df_jobs[df_jobs["Arbeitgeber_Bewertung"].isna()]
    NaN_values_skills = df_jobs[df_jobs["Programmier-und Softwarekenntnisse"].isna()]

    all_line_with_NaN_value_rating = NaN_values_rating.index.tolist()
    all_line_with_NaN_value_skills = NaN_values_skills.index.tolist()

    # Arbeitgeber_Bewertung --> Mittelwert von anderen Stellenanzeigen als fill!:
    df_jobs_filled = df_jobs  # Kopie erstellen

    mean_rating = pd.Series.mean(df_jobs['Arbeitgeber_Bewertung'])
    print("Ersetze Null Values in der Arbeitgeber Bewertung mit dem mean: ", mean_rating)
    df_jobs_filled = df_jobs_filled.fillna({'Arbeitgeber_Bewertung': mean_rating})

    mean_rating_new = pd.Series.mean(df_jobs_filled['Arbeitgeber_Bewertung'])
    print("Prüfen ob fillna geklappt hat: ",
          df_jobs_filled[df_jobs_filled["Arbeitgeber_Bewertung"].isna()])  # wenn empty data frame dann gut
    print("Neuer mean:", mean_rating_new)

    # Dritter Schritt: Anwendung eines Thresh
        # Thresh = 3 wurde definiert, damit Daten die zuviele Missing Values nicht die Datenqualität beeinflussen.
        # Thresh < 3 schließt zuviele Daten aus, da Datum_Veröffentlichung  per Definition nur leere Werte hat.
    df_jobs_dropped = df_jobs_filled.dropna(thresh=3)
    print("Nach dem Droput der Missing Values: ", len(df_jobs_dropped))
    print(30 * "*")

    return df_jobs_dropped

def data_cleaning_duplicate(df_jobs_dropped):
    '''
    Diese Funktion prüft nach Duplikaten und entfernt Datensätze, wenn sie doppelt vorkommen.
    :param df_jobs_dropped
    :return: df_cleaned
    '''

    ##Erster Schritt: Prüfen auf Duplikate:

    print("Prüfen auf Duplikate:")
    # print(df_jobs_dropped.duplicated()) #Duplikate feststellen
    duplicates = df_jobs_dropped[df_jobs_dropped.duplicated(keep=False)]
    duplicates = duplicates.sort_values("Primärschlüssel", ascending=True)
    print(duplicates)

    # Zweiter Schritte: Droppen von Duplikaten:
    print(30 * "*")
    print("Vor dem Drop der Duplikate: ", len(df_jobs_dropped))
    df_jobs_dropped = df_jobs_dropped.drop_duplicates()
    print("Nach dem Drop der Duplikate: ", len(df_jobs_dropped))
    # print(df_jobs_dropped.duplicated())
    print(30 * "*")

    #Dritter Schritt: Droppen und prüfen weiterer Duplikate:
    print("Gibt es weitere Duplikate?")
    # print(df_jobs_dropped.duplicated(subset = ["Stellentitel", "Arbeitsort", "Arbeitgeber"]))
    further_duplicates = df_jobs_dropped[df_jobs_dropped.duplicated(subset=["Stellentitel", "Arbeitgeber", "Arbeitsort"], keep=False)]  # Duplikate
    further_duplicates = further_duplicates.sort_values("Primärschlüssel", ascending=True)
    print("Folgende Duplikate wurden gefunden, die den Bedingungen entsprechen: ", further_duplicates)
    print(30 * "*")
    df_cleaned = df_jobs_dropped.drop_duplicates(subset=["Stellentitel", "Arbeitgeber", "Arbeitsort"])
    # df_cleaned_sort = df_cleaned.sort_values("Primärschlüssel", ascending=True)
    # print(df_cleaned_sort)
    print("Nach dem Drop der weiteren Duplikate: ", len(df_cleaned))

    return df_cleaned

def data_cleaning_typos(df_cleaned):
    '''
    Eine simple Funktion, die nur groupby Statements um visuell nach Typos zu checken.
    :param df_cleaned:
    :return: kein return notwendig.
    '''

    # Suchen nach Typos in Arbeitspensum with groupby statements:
    groupby = df_cleaned.groupby('Arbeitspensum')
    print(groupby.count())  # no typos in Arbeitspensum
    print("-> no typos in Arbeitspensum")
    print(30 * "*")

    # Suchen nach Typos in Branche with groupby statements:
    groupby = df_cleaned.groupby('Branche')
    print(groupby.count())  # no typos in Branche
    print("-> no typos in Branche")
    print(30 * "*")


def data_cleaning_outlier_arbeitsort (df_cleaned):
    '''
    Eine ausführliche Funktion, die nach Outlier im Arbeitsort sucht. Diese Outlier werden bereinigt nach Kriterien wie sie im ETL Prozess definiert wurden.
    :param df_cleaned:
    :return: df_cleaned_no
    '''

    # Kopie erstellen
    df_cleaned_no = df_cleaned

    #Erstes Problem - Falsche Format:
    #groupby = df_cleaned.groupby('Arbeitsort')
        #Daten zeigen:
    print("Checken nach Outliers im Arbeitsort:")
    print("Die Daten sehen momentan so aus: \n" , df_cleaned["Arbeitsort"])
    print(30 * "*")

        #Cleaning:
    print("Anzahl Daten vor Cleaning der Outlier:", len(df_cleaned_no))
    df_cleaned_no["Arbeitsort"] = df_cleaned_no["Arbeitsort"].str.replace(r'[0-9]' ,"", regex=True) #Entfernen der Postleitzahlen
    df_cleaned_no["Arbeitsort"] = df_cleaned_no["Arbeitsort"].str.replace("CH-", "") #Entfernen von CH-
    df_cleaned_no["Arbeitsort"] = df_cleaned_no["Arbeitsort"].str.lstrip() #Whitespaces elminieren

    df_cleaned_no["Arbeitsort"] = df_cleaned_no["Arbeitsort"].map(lambda n: str(n).partition(",")[0]) #alles vor dem komma sollen stehen bleiben
    df_cleaned_no["Arbeitsort"] = df_cleaned_no["Arbeitsort"].map(lambda n: str(n).partition("oder")[0])
    df_cleaned_no["Arbeitsort"] = df_cleaned_no["Arbeitsort"].map(lambda n: str(n).partition("/")[0])
    df_cleaned_no["Arbeitsort"] = df_cleaned_no["Arbeitsort"].map(lambda n: str(n).partition(" - ")[0])

        #Cleaning: Dieses dict enthält Werte, die ersetzt werden sollen. Trotz Cleaning Anstrengungen oben, müssen bei Bedarf manuell solche Werte ergänzt werden:
    df_cleaned_no["Arbeitsort"].replace({
        "Basel Bern Geneva Zurich" : "Basel",
        "Basel Zürich" : "Basel",
        "Bern-Liebefeld" : "Bern",
        "Bern " : "Bern",
        "Biel" : "Biel/Bienne",
        "Birmensdorf ZH": "Birmensdorf (ZH)",
        "Carouge" : "Carouge (GE)",
        "Cointrin" : "Meyrin",
        "Davos Dorf": "Davos",
        "GE-Genève" : "Genève",
        "Glattpark (Opfikon": "Opfikon",
        "Geneva Zurich" : "Genf",
        "Gümligen" : "Muri bei Bern",
        "Herisau (AR)" : "Herisau",
        "Küsnacht ZH" : "Küsnacht (ZH)",
        "Le Lignon" : "Vernier",
        "Neuchatel": "Neuchâtel",
        "Petit Lancy" : "Lancy",
        "Rotkreuz" : "Risch",
        "Seewen SZ"  : "Seewen",
        "Stein AG" : "Stein (AG)",
        "St.Gallen": "St. Gallen",
        "Zürich St. Gallen" : "Zürich",
        "Zürich Stadt ZH" :"Zürich",
        "Zürich-Seefeld" : "Zürich"}, inplace=True)


    #Zweites  Problem - Stelle nicht auf Ortschaft gemapped:
        #Gefunden: Deutschschweiz, Kanton: Waadt, Romandie-Lausanne, Schweiz, Switzerland
        #Da die Ortschaft nicht bekannt ist, wird alles auf Schweiz umgemapped.
        #Außerdem giobt es eine Stelle in Köln (DE) und eine in Eschen (FL) welche mit dem Land gemapped werden.
        #Es könnten weitere solche Ausnahmen hinzukommen und müssten manuell ergänzt werden.

    df_cleaned_no["Arbeitsort"].replace({
        "Deutschschweiz" : "Schweiz",
        "Eschen" : "Liechenstein",
        "Flexible Work Location" : "Schweiz",
        "Kanton: Waadt" : "Schweiz",
        "Köln" : "Deutschland",
        "Romandie-Lausanne": "Schweiz",
        "Switzerland": "Schweiz",}, inplace=True)

    #Drittes Problem - Falsche Sprache:
    location= df_cleaned_no.loc[:,"Arbeitsort"]
    str_location = location.to_string(index=False)
    lines = str_location.splitlines()
    lines = lines[1:] #remove first list entry (it is the header)
    lines_list = [line.strip(" ") for line in lines]

    DetectorFactory.seed = 0 #DetectorFactory ist ein Modul, was automatisch Sprachen identifiziert. Es erzielt bei Ortschaften aber keine perfekten Resultate.
    lang_list = [line + " the detected language is: " + detect(line) for line in lines if detect(line) != "de"]

        #Analyse mit folgendem Code: (auskommentiert, damit der print nicht zu lang ist)
    # for line in lines_list:
    #     detected = detect(line)
    #     #print(line + " the detected language is: " + detected)

        #Folgende Ortschaften werden nach Analyse umgeändert:
    df_cleaned_no.replace({"Lucerne" : "Luzern", "GENEVE": "Genève", "Genf" : "Genève", "Geneva" : "Genève", "Zurich" : "Zürich"}, inplace = True)

    #Result:
    print("Anzahl Daten nach Cleaning der Outlier:", len(df_cleaned_no))
    print("Die aufgeräumten Daten sehen so aus: \n", df_cleaned_no.groupby("Arbeitsort").count())
    print(30 * "*")

    return df_cleaned_no

def data_cleaning_outlier_stellentitel(df_cleaned_no):
    '''
    Eine Funktion die nach Outlier im Stellentitel sucht. Diese Outlier werden gedropped, da sie wahrscheinlich keine Data Science Stellen sind.
    Siehe ETL Prozess Plan.
    :param df_cleaned_no
    :return: df_cleaned_no_drop
    '''
    #Settings
    pd.set_option("display.max_colwidth", 100)
    pd.set_option('display.max_columns', 5)

    #Schritt 1: Slicen der Daten mit Filter Funktion
    print("Gibt es Stellentitel, die eigentlich keine Data Science Stellen sind?? ")
    filter = df_cleaned_no.filter(items=["Stellentitel", "Programmier-und Softwarekenntnisse"])

    #Schritt 2: Alle Stellentitel werden auf lowercase transformiert (wir auf diese Weise weniger Filter)
    filter["Stellentitel"] = df_cleaned_no["Stellentitel"].map(lambda n: str(n).lower())

    #Schritt 3: Schrittweise werden immer mehr Stellentitel herausgefiltert
    filter2 = filter[filter['Stellentitel'].str.match('^.*data.*') == False] #Taucht der Begriff "data" irgendwo im Stellentitel auf?
                                                                            # Wenn nein: Bitte speichere mir diese Stellentitel in der filter2 variable
    # print(filter2)
    filter3 = filter2[filter2['Stellentitel'].str.match('^.*daten.*') == False] #Filter3, filtert auf Basis des Ergebnisse von Filter 2, taucht der Begriff "daten" irgendo auf?
    # print(filter3)
    filter4 = filter3[filter3['Stellentitel'].str.match('^.*dati.*') == False]
    # print(filter4)
    filter5 = filter4[filter4['Stellentitel'].str.match('^.*données.*') == False]
    # print(filter5)
    filter6 = filter5[filter5['Programmier-und Softwarekenntnisse'].isnull() == True] #Filter mir zum Abschluss nur Stellentitel ohne Programmier und Softwarekenntnisse
    print("Folgende Titel sind sehr wahrscheinlich keine Data Science Stellen: \n", filter6)


    #Schritt 4: Entfernen der Outlier:
    print(30 * "*")
    # Im Filter6 sind gescrapte Stellen, die nicht zu den Begriffen Data Science passen und zudem keine geforderten Programmier-und Softwarekenntnisse zeigen.
    # Daher sind es Outlier, die entfernt werden müssen.
    outlier = filter6.index.tolist()
    #print(outlier)
    counter = 0

    for i in outlier:
        # Der Loop dropped alle Einträge die in der Liste outlier vorkommt. Dabei wird diese Liste iteriert, bis alle Outlier weg sind.
        # Es wird eine Warnung kommen, aber die kann man ignorieren.
        counter += 1
        if counter == 1: #Wir nur für den ersten Eintrag benötigt, da zunächst eine df_cleaned_no_drop erstellt werden muss.
            df_cleaned_no_drop = df_cleaned_no.drop(i, 0)
        else:
            df_cleaned_no_drop = df_cleaned_no_drop.drop(i, 0)

    #Result:
    print("Vor dem Drop der Outliers waren", len(df_cleaned_no), "Datensätze")
    # print(df_cleaned_no_drop)
    print("Nach dem Drop der Outliers sind" ,len(df_cleaned_no_drop),"Datensätze")
    return df_cleaned_no_drop


def data_cleaning_cosmetics_arbeitgeber(df_cleaned_no_drop):
    '''
    Diese Funktion ist kosmetischer Natur und es wird die Arbeitgeber lesbarer zu machen, sodass Sonderzeichen oder Daten, die eigentlich kein Stellentitel
    sind soweit wie möglich bereinigt werden. z.B. Bundesamt für Bauten und Logistik (BBL)  in --> Bundesamt für Bauten und Logistik
    :param df_cleaned_no_drop:
    :return: df_cleaned_no_drop_cosmetic
    '''

    #Data Cleaning: Kosmetik bei Arbeitgebern:
    print(30 * "*")

    #Kopie machen:
    df_cleaned_no_drop_cosmetic= df_cleaned_no_drop.copy()
    #print(df_cleaned_no_drop_cosmetic.columns)

    #Schritt 1 Betrachtung:
    print("Die Arbeitgeber enthalten Einträge wie Bundesamt für Bauten und Logistik (BBL). Der Eintrag in Klammern sieht unschön aus. ")
    pd.set_option("display.max_colwidth", 50)
    #print(df_cleaned_no_drop_cosmetic.groupby("Arbeitgeber").count())

    #Schritt 2: Cleaning
    arbeitgeber_list = df_cleaned_no_drop_cosmetic["Arbeitgeber"].values.tolist()
    print("Anzahl der Arbeitgeber vor Cleaning", len(arbeitgeber_list))
    #df_cleaned_no_drop["Arbeitgeber"] = df_cleaned_no_drop["Arbeitgeber"].str.replace(r"\(\w\)" ,"", regex=True)

    new_arbeitgeber_list = []
    for arbeitgeber in arbeitgeber_list:
        #Dieser For-Loop geht durch alle Arbeitgeber Einträge in der erzeugten Liste durch und ersetzt (Wort) durch ""
        #z.B. Eidgenössische Steuerverwaltung (ESTV) zu Eidgenössische Steuerverwaltung
        string = str(arbeitgeber)
        #print(string)
        cosmetics_arbeitgeber = re.sub(r"(\(.*\))|(\[.*\])", "", string)
        #print(cosmetics_arbeitgeber)
        new_arbeitgeber_list.append(cosmetics_arbeitgeber)

    #print(new_arbeitgeber_list)

    #Schritt 3:  Alte Spalte im Dataframe löschen:
    df_cleaned_no_drop_cosmetic = df_cleaned_no_drop_cosmetic.drop("Arbeitgeber", axis=1)

    #Schritt 4: Neue Spalte erzeugen im Dataframe und Values von der Liste new_arbeitgeber_list einfügen:
    df_cleaned_no_drop_cosmetic["Arbeitgeber"] = new_arbeitgeber_list

    #Schritt 5: Alte Order der Columns wieder zurückbringen
    neworder = ['Unnamed: 0', 'Primärschlüssel', 'Stellentitel', 'Arbeitsort', 'Arbeitgeber', 'Arbeitgeber_Bewertung',
                'Datum_Veröffentlichung', 'URL_First_Infos', 'URL_Detailed_Infos', 'Programmier-und Softwarekenntnisse','Branche', 'Arbeitspensum']
    df_cleaned_no_drop_cosmetic = df_cleaned_no_drop_cosmetic.reindex(columns=neworder)
    # print(df_cleaned_no_drop.columns)

    #Result
    print("Anzahl der Arbeitgeber nach Cleaning", len(arbeitgeber_list))
    print("Die aufgeräumten Daten sehen so aus:")
    print(df_cleaned_no_drop_cosmetic.groupby("Arbeitgeber").count())

    return df_cleaned_no_drop_cosmetic

def data_cleaning_cosmetics_stellentitel(df_cleaned_no_drop_cosmetic):
    '''
    Diese Funktion ist kosmetischer Natur und es wird versucht die Stellentitel lesbarer zu machen, sodass Sonderzeichen oder Daten, die eigentlich kein Stellentitel
    sind soweit wie möglich bereinigt werden. z.B. (Senior) Data Analyst (f/m/d) 100%  in --> Senior Data Analyst f/m/d
    :param df_cleaned_no_drop_cosmetic:
    :return: df_cleaned_no_drop_cosmetic2
    '''

    #Kopie machen:
    df_cleaned_no_drop_cosmetic2 = df_cleaned_no_drop_cosmetic.copy()
    print(30 * "*")

    # Schritt 1 Betrachtung:
    print("Die Stellentitel enthalten Einträge wie BI, Data & Analytics Entwickler (m/w/d). Wir wollen nur den tatsächlichen Stellentitel. "
          "/n Das wäre: BI, Data & Analytics Entwickler")
    #print(df_cleaned_no_drop_cosmetic2.groupby("Stellentitel").count())

    # Schritt 2: Cleaning
    stellentitel_list = df_cleaned_no_drop_cosmetic2["Stellentitel"].values.tolist()
    print("Anzahl der Arbeitgeber vor Cleaning", len(stellentitel_list))
    # df_cleaned_no_drop["Arbeitgeber"] = df_cleaned_no_drop["Arbeitgeber"].str.replace(r"\(\w\)" ,"", regex=True)

    new_stellentitel_list = []

    for stellentitel in stellentitel_list:
        # Dieser For-Loop geht durch alle Arbeitgeber Einträge in der erzeugten Liste durch und ersetzt  Verunreinigungen nach und nach mit dem regex modul
        string = str(stellentitel)
        cosmetics_stellentitel = re.sub(r"\(" , "", string)
        cosmetics_stellentitel1 = re.sub(">", "", cosmetics_stellentitel)
        cosmetics_stellentitel2 = re.sub(r"(#.*)", "", cosmetics_stellentitel1)
        cosmetics_stellentitel3 = re.sub(r"(%)", "", cosmetics_stellentitel2)
        cosmetics_stellentitel4 = re.sub(r"[0-9]", "", cosmetics_stellentitel3)
        cosmetics_stellentitel5 = re.sub(" -", "", cosmetics_stellentitel4)
        cosmetics_stellentitel6 = cosmetics_stellentitel5.strip()
        cosmetics_stellentitel7 = re.sub(",", "", cosmetics_stellentitel6)
        cosmetics_stellentitel8 = re.sub(",", "", cosmetics_stellentitel7)
        cosmetics_stellentitel9= re.sub('\s+', ' ', cosmetics_stellentitel8)
        cosmetics_stellentitel10 = re.sub(r'\)', '', cosmetics_stellentitel9)
        cosmetics_stellentitel11 = re.sub(r'\[', '', cosmetics_stellentitel10)
        cosmetics_stellentitel12 = re.sub(r'\]', '', cosmetics_stellentitel11)

        new_stellentitel_list.append(cosmetics_stellentitel12)

    # Schritt 3:  Alte Spalte im Dataframe löschen:
    df_cleaned_no_drop_cosmetic2 = df_cleaned_no_drop_cosmetic2.drop("Stellentitel", axis=1)

    # Schritt 4: Neue Spalte erzeugen im Dataframe und Values von der Liste new_arbeitgeber_list einfügen:
    df_cleaned_no_drop_cosmetic2["Stellentitel"] = new_stellentitel_list

    # Schritt 5: Alte Order der Columns wieder zurückbringen
    neworder = ['Primärschlüssel', 'Stellentitel', 'Arbeitsort', 'Arbeitgeber', 'Arbeitgeber_Bewertung',
                'Datum_Veröffentlichung', 'URL_First_Infos', 'URL_Detailed_Infos', 'Programmier-und Softwarekenntnisse',
                'Branche', 'Arbeitspensum']
    df_cleaned_no_drop_cosmetic2 = df_cleaned_no_drop_cosmetic2.reindex(columns=neworder)

    # Result
    print("Anzahl der Stellentitel nach Cleaning", len(new_stellentitel_list))
    print("Die aufgeräumten Daten sehen so aus (nicht perfekt, aber besser als vorher):")
    print(df_cleaned_no_drop_cosmetic2.groupby("Stellentitel").count())

    return df_cleaned_no_drop_cosmetic2

def data_cleaning_primarschlüssel(df_cleaned_no_drop_cosmetic2):
    '''
    Diese Funktion sorgt dafür, dass der ehemals unsaubere Primärschlüssel gelöscht wird und
     und einer neuer Primärschlüssel mit den gesäuberten Werten aus Arbeitgeber, Arbeitsort und Stellentitelerzeugt wird
    :param df_cleaned_no_drop_cosmetic2:
    :return: df_jobs_final
    '''


    #Schritt 1: Der alte verunreinige Primärschlüssel wird entfernt:
    df_jobs_final= df_cleaned_no_drop_cosmetic2.drop(columns = ["Primärschlüssel"])

    #Schritt 2: Neuer Primärschlüssel mit bereinigten Columns wird erstellt:

    df_jobs_final ["Primärschlüssel"] = df_jobs_final ["Stellentitel"] +"-" + df_jobs_final["Arbeitsort"] + "-" + df_jobs_final["Arbeitgeber"]


    # Schritt 3: Alte Order der Columns wieder zurückbringen
    neworder = ['Primärschlüssel', 'Stellentitel', 'Arbeitsort', 'Arbeitgeber', 'Arbeitgeber_Bewertung',
                'Datum_Veröffentlichung', 'URL_First_Infos', 'URL_Detailed_Infos', 'Programmier-und Softwarekenntnisse',
                'Branche', 'Arbeitspensum']
    df_jobs_final = df_jobs_final.reindex(columns=neworder)

    #Result
    print("Der neue aufgeräumte Primärschlüssel sieht so aus: ", df_jobs_final.head(5))

    return df_jobs_final

def create_csv(df_jobs_final):
    #Drop Columns: (werden nicht mehr gebraucht)
    df_jobs_final = df_jobs_final.drop(columns=["URL_First_Infos", "URL_Detailed_Infos"])

    # Save cleaned df to csv:
    df_jobs_final.to_csv('JobAgent_cleaned.csv', index=True)
    # Data is cleaned to map with df_company_size and wiki now.

def main():
    df_jobs= load_data_and_inspect()
    df_jobs_dropped= data_cleaning_missing_values(df_jobs)
    df_cleaned= data_cleaning_duplicate(df_jobs_dropped)
    data_cleaning_typos(df_cleaned)
    df_cleaned_no = data_cleaning_outlier_arbeitsort(df_cleaned)
    df_cleaned_no_drop = data_cleaning_outlier_stellentitel(df_cleaned_no)
    df_cleaned_no_drop_cosmetic= data_cleaning_cosmetics_arbeitgeber(df_cleaned_no_drop)
    df_cleaned_no_drop_cosmetic2 = data_cleaning_cosmetics_stellentitel(df_cleaned_no_drop_cosmetic)
    df_jobs_final = data_cleaning_primarschlüssel(df_cleaned_no_drop_cosmetic2)
    create_csv(df_jobs_final)


if __name__ == "__main__":
    main()
