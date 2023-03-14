#Beachte: Das Datenset ist in der Quelle bereits bereinigt und benötigt kein Data Cleaning.
#Zur Vollständigkeit erhält es aber ein eigenes Skript, wie die anderen Sources.

# Benötigte Module
import pandas as pd

# Settings für PyCharm
desired_width = 400
pd.set_option('display.width', desired_width)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_columns', 10)
pd.set_option("display.max_colwidth", 25)


def load_data_and_inspect():
    ## Third source
    csv_name3 = "Extract_Beautiful_Soup/Wiki_src.csv"
    path3 = (f"/home/student/Cloud/Owncloud/SyncVM/CIP Project/JobAgent_T/{csv_name3}")
    df_wiki = pd.read_csv(path3)


    df_wiki.info()
    print(df_wiki.shape)
    print(df_wiki.head())

    print(df_wiki.describe())
    print(df_wiki.describe(include=['object']))

    return df_wiki



def main():
    df_wiki = load_data_and_inspect()


if __name__ == "__main__":
    main()

