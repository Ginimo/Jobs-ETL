#Dieses Skript cleaned die Source df_company_size

# Benötigte Module
import pandas as pd

# Settings für PyCharm
desired_width = 400
pd.set_option('display.width', desired_width)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_columns', 10)
pd.set_option("display.max_colwidth", 25)


def load_data_and_inspect():
    ## Second source
    csv_name2 = "Extract_Beautiful_Soup/JobAgent_src_company_size.csv"
    path2 = (f"/home/student/Cloud/Owncloud/SyncVM/CIP Project/JobAgent_T/{csv_name2}")

    df_jobs_company_size = pd.read_csv(path2, na_values = ["NA"])

    df_jobs_company_size.info()
    print(df_jobs_company_size.shape)
    print(30* "*")
    print(df_jobs_company_size.head())
    print(30 * "*")
    print(df_jobs_company_size.describe(include=['object']))
    print(30 * "*")
    return df_jobs_company_size

def data_cleaning_missing_values(df_jobs_company_size):
    '''
    Diese Funktion prüft nach missing values und dropped Datensätze, wenn Indices fehlen, ein definerter Thresh nicht erfüllt ist
    oder wenn eine Analyse zu einem Bereinigungsergebnis führt. Die Analyse ist individuell pro source.
    :param df_jobs_company_size
    :return: df_jobs_company_size_dropped
    '''

    #Checken auf Null Values:
    print("Testen auf Null Values:")
    print(pd.isnull(df_jobs_company_size))
    print(30 * "*")

    #Erster Schritt: Drop missing indices.
    print("Vor dem Droput der Missing Indices: ", len(df_jobs_company_size))
    df_jobs_company_size.index.dropna
    print("Nach dem Dropout der Missing Indices:", len (df_jobs_company_size))

    #Zweiter Schritt: Analyse
    #print(df_jobs_company_size.loc[df_jobs_company_size.isna().any(1)]) #Kein sinnvoller Code für dieses data frame. Alle zeilen werden ausgegeben, da Datum_Veröffentlichung  per Definition nur leere Werte hat.
    print(30 * "*")
    print("Folgende Columns enthalten Missing Values:", df_jobs_company_size.columns[df_jobs_company_size.isna().any(0)].tolist())

    #Dritter Schritt: Anwendung eines Threshs
    df_jobs_company_size_dropped = df_jobs_company_size.dropna(thresh = 1) #Dropped alle Reihen, die mind. 1 missing value haben.
    print("Nach dem Droput der Missing Values: ",len(df_jobs_company_size))

    print(30 * "*")
    return df_jobs_company_size_dropped

def data_cleaning_duplicate(df_jobs_company_size_dropped):
    '''
    Diese Funktion prüft nach Duplikaten und entfernt Datensätze, wenn sie doppelt vorkommen.
    :param df_jobs_company_size_dropped
    :return: df_jobs_company_size_cleaned
    '''

    #Erster Schritt: Prüfen auf Duplikate:
    print("Prüfen auf Duplikate:")
    #print(df_jobs_company_size_dropped.duplicated()) #Duplikate feststellen
    duplicates = df_jobs_company_size_dropped[df_jobs_company_size_dropped.duplicated(keep=False)]
    duplicates = duplicates.sort_values("Primärschlüssel", ascending=True)
    print(duplicates)

    #Zweiter Schritte: Droppen von Duplikaten:
    print(30 * "*")
    print("Vor dem Drop der Duplikate: ", len(df_jobs_company_size_dropped))
    df_jobs_company_size_cleaned= df_jobs_company_size_dropped.drop_duplicates()
    print("Nach dem Drop der Duplikate: ", len(df_jobs_company_size_cleaned))

    print(30 * "*")
    return df_jobs_company_size_cleaned

def create_csv(df_jobs_company_size_cleaned):
    # Save cleaned df to csv:
    df_jobs_company_size_cleaned.to_csv('JobAgent_company_size_cleaned.csv', index=True)
    # Data is cleaned to merge with df_jobs now.

def main():
    df_jobs_company_size = load_data_and_inspect()
    df_jobs_company_size_dropped = data_cleaning_missing_values(df_jobs_company_size)
    df_jobs_company_size_cleaned = data_cleaning_duplicate(df_jobs_company_size_dropped)
    create_csv(df_jobs_company_size_cleaned)

if __name__ == "__main__":
    main()
