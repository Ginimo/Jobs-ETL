#Dieses Skript merged alle Jobbörsen zusammen (Jobscout24, Jobagent und Indeed)

#Benötigte Module
import pandas as pd

# Settings für PyCharm
desired_width = 400
pd.set_option('display.width', desired_width)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_columns', 10)
pd.set_option("display.max_colwidth", 25)

#Load Data
def load_first_source():
    #First source:
    csv_name1 = "JobAgent_stage.csv"
    path1 = (f"/home/student/Cloud/Owncloud/SyncVM/CIP Project/JobsMerged/{csv_name1}")
    Jobagent = pd.read_csv(path1, sep = ";")
    print("5 Datensätze aus Jobagent: ", Jobagent.head(5))
    print(30 * "*")

    return Jobagent

def adapt_first_source(Jobagent):

    print("Jobagent wird fürs konkatenieren adaptiert:")

    #Kopie:
    Jobagent_prep = Jobagent.copy()

    # Alle CSVS aufs gleiche Format bringen später zum Appenden der Jobs:
    Jobagent_prep = Jobagent_prep.drop(columns=["JobNr"])
    Jobagent_prep = Jobagent_prep.assign(Min_Ausbildungsanforderung = "")
    Jobagent_prep = Jobagent_prep.assign(Führungsposition="")
    Jobagent_prep = Jobagent_prep.assign(Quelle="Jobagent")
    neworder = ["Primärschlüssel", "Stellentitel", "Arbeitsort", "Arbeitgeber", "Arbeitgeber_Bewertung", "Datum_Veröffentlichung",
                "Programmier_und_Softwarekenntnisse", "Kanton", "Unternehmensgrösse", "Arbeitspensum_min",  "Arbeitspensum_max", "Branche",
                "Min_Ausbildungsanforderung", "Führungsposition", "Quelle"]
    Jobagent_prep = Jobagent_prep.reindex(columns=neworder)

    print(Jobagent_prep.columns)
    print(30 * "*")

    return Jobagent_prep

def load_second_source():
    csv_name2 = "Indeed_stage.csv"
    path2 = (f"/home/student/Cloud/Owncloud/SyncVM/CIP Project/JobsMerged/{csv_name2}")
    Indeed = pd.read_csv(path2)
    print("5 Datensätze aus Indeed: ", Indeed.head(5))
    print(30 * "*")

    return Indeed

def adapt_secound_source(Indeed):

    print("Indeed wird fürs konkatenieren adaptiert:")

    #Kopie
    Indeed_prep = Indeed.copy()

    # Alle CSVS aufs gleiche Format bringen später zum Appenden der Jobs:
    Indeed_prep = Indeed_prep.drop(columns=["Unnamed: 0"])
    Indeed_prep = Indeed_prep.assign(Unternehmensgrösse="")
    Indeed_prep = Indeed_prep.assign(Branche="")
    Indeed_prep = Indeed_prep.assign(Führungsposition="")
    Indeed_prep = Indeed_prep.assign(Quelle="Indeed")
    neworder = ["Primärschlüssel", "Stellentitel", "Arbeitsort", "Arbeitgeber", "Arbeitgeber_Bewertung", "Datum_Veröffentlichung",
                "Programmier_und_Softwarekenntnisse", "Kanton", "Unternehmensgrösse", "Arbeitspensum_min",  "Arbeitspensum_max", "Branche",
                "Min_Ausbildungsanforderung", "Führungsposition", "Quelle"]
    Indeed_prep = Indeed_prep.reindex(columns=neworder)
    print(Indeed_prep.columns)
    print(30 * "*")

    return Indeed_prep
#
def load_third_source():
     csv_name3 = "jobscout24.csv"
     path3 = (f"/home/student/Cloud/Owncloud/SyncVM/CIP Project/JobsMerged/{csv_name3}")
     Jobscout24 = pd.read_csv(path3)
     print("5 Datensätze aus Jobscout24: ", Jobscout24.head(5))
     print(30 * "*")

     return Jobscout24

def adapt_third_source(Jobscout24):

     print("Jobscout24 wird fürs konkatenieren adaptiert:")

     #Kopie
     Jobscout24_prep = Jobscout24.copy()

     #Alle CSVS aufs gleiche Format bringen später zum Appenden der Jobs:
     Jobscout24_prep = Jobscout24_prep.drop(columns=["Unnamed: 0"])
     Jobscout24_prep = Jobscout24_prep.assign(Quelle="Jobscout24")
     Jobscout24_prep = Jobscout24_prep.assign(Min_Ausbildungsanforderung="")
     Jobscout24_prep.rename(columns={"Unnamed: 0": "JobNr"}, inplace=True)
     neworder = ["Primärschlüssel", "Stellentitel", "Arbeitsort", "Arbeitgeber", "Arbeitgeber_Bewertung",
                 "Datum_Veröffentlichung",
                 "Programmier_und_Softwarekenntnisse", "Kanton", "Unternehmensgrösse", "Arbeitspensum_min",
                 "Arbeitspensum_max", "Branche",
                 "Min_Ausbildungsanforderung", "Führungsposition", "Quelle"]
     Jobscout24_prep = Jobscout24_prep.reindex(columns=neworder)

     print(Jobscout24_prep.columns)
     print(30 * "*")

     return Jobscout24_prep


def merge(JobAgent, Indeed):
    '''
    JobAgent und Indeed werden zusammen gemerged
    :param JobAgent
    :param Indeed
    :return: JobsMerged_Step01
    '''

    #Merge anhand von Primärschlüssel:
    print("Anzahl der Datensätze vor merge bei JobAgent", len(JobAgent))
    print("Anzahl der Datensätze vor merge bei Indeed", len(Indeed))
    JobsMerged_Step01 = pd.merge(JobAgent, Indeed, on="Primärschlüssel", how="left")
    print("Anzahl der Datensätze nach merge", len(JobsMerged_Step01))
    print(JobsMerged_Step01.head())
    print("Der Index ist: \n", JobsMerged_Step01.index)
    print(30 * "*")

    # Zeige mir alle an, die nicht gematched haben:
    NaN_values = JobsMerged_Step01[JobsMerged_Step01["Min_Ausbildungsanforderung"].isna()] #da Min_Ausbildungsanforderung nur bei Indeed kommt, können wir isna() bzw.
    #nota() anwenden um zu checken, ob etwas gemerged hat
    Non_NaN_values = JobsMerged_Step01[JobsMerged_Step01["Min_Ausbildungsanforderung"].notna()]
    print("Anzahl der Jobs, die nicht gematched haben:", len(NaN_values))
    #print(NaN_values.groupby("Arbeitsort").count())
    print("Folgende Jobs konnten nicht mit Indeed matchen: \n" , NaN_values)
    print("Trotz starker Cleaning Bemühungen erhalten wir keinen Datensatz, der matched:\n", Non_NaN_values)
    #print(30 * "*")

    #Results:
    #print(JobsMerged_Step01.columns)
    #print(JobsMerged_Step01.index)
    #print(30 * "*")

    # Mach ein dataframe als Zwischenziel:
    print(30 * "*")
    JobsMerged_Step01.to_csv('JobsMerged_Step01.csv', index=True)

    return JobsMerged_Step01



def merge2(JobsMerged_Step01, Jobscout24):
    '''
    JobsMerged_Step01 und Jobscout24 werden gemerged
    :param JobsMerged_Step01
    :param Jobscout24
    :return: Das Resultat ist das finale merge Resultat JobsMerged_Step02
    '''

    #Merge anhand von Primärschlüssel:
    print("Anzahl der Datensätze vor merge bei JobsMerged_Steop01", len(JobsMerged_Step01))
    print("Anzahl der Datensätze vor merge bei Jobscout24", len(Jobscout24))
    JobsMerged_Step02 = pd.merge(JobsMerged_Step01, Jobscout24, on="Primärschlüssel", how="left")
    print("Anzahl der Datensätze nach merge", len(JobsMerged_Step02))
    print(JobsMerged_Step02.head())
    print("Der Index ist: \n", JobsMerged_Step02.index)
    print(30 * "*")

    # Zeige mir alle an, die nicht gematched haben:
    NaN_values = JobsMerged_Step02[JobsMerged_Step02["Stellentitel_y"].isna()]
    Non_NaN_values = JobsMerged_Step02[JobsMerged_Step02["Stellentitel_y"].notna()]
    print("Anzahl der Jobs, die nicht gematched haben:", len(NaN_values))
    # print(NaN_values.groupby("Arbeitsort").count())
    print("Folgende Jobs konnten nicht mit Jobscout24 matchen: \n", NaN_values)
    print("Trotz starker Cleaning Bemühungen erhalten wir keinen Datensatz, der matched:\n", Non_NaN_values)
    # print(30 * "*")

    # Results:
    # print(JobsMerged_Step01.columns)
    # print(JobsMerged_Step01.index)
    # print(30 * "*")

    # Mach ein dataframe am Ende:
    print(30 * "*")
    JobsMerged_Step02.to_csv('JobsMerged_Step02.csv', index=True)

    return JobsMerged_Step02

def jobs_append(Jobagent, Indeed, Jobscout24):
    '''
    :param Jobagent: 
    :param Indeed: 
    :param Jobscout24: 
    :return: 
    '''
    Jobs_concat = pd.concat([Jobagent,Indeed,Jobscout24])
    print("Erste 5 Datensätze:", Jobs_concat.head(5))
    print("Letzte 5 Datensätze:", Jobs_concat.tail(5))
    Jobs_concat.index = Jobs_concat.index.set_names(['JobNr'])
    Jobs_concat.to_csv('JobsConcat_stage.csv', index =True, sep = ";")
    print(30* "*")

def main():
    Jobagent= load_first_source()
    Indeed = load_second_source()
    Jobscout24 = load_third_source ()

    JobsMerged_Step01= merge(Jobagent,Indeed)
    JobsMerged_Step02 = merge2(JobsMerged_Step01, Jobscout24)

    Jobagent_prep = adapt_first_source(Jobagent)
    Indeed_prep = adapt_secound_source(Indeed)
    Jobscout24_prep = adapt_third_source(Jobscout24)
    Jobs_concat = jobs_append(Jobagent_prep, Indeed_prep, Jobscout24_prep)

if __name__ == "__main__":
    main()
