#Jobagent
from Extract_Beautiful_Soup import Extract10 as E_Jobagent
from Transform import Transform_df_jobs02 as T1_Jobagent
from Transform import Transform_df_jobs_company_size01 as T2_Jobagent
from Transform import Transform_df_wiki01 as T3_Jobagent
from Transform import Transform_df_jobs_merged01 as T4_Jobagent

#Indeed
from Final import Indeed_Extract as E_Indeed
from Final import Indeed_Transform as I_Indeed

#Jobscout24
from Final_2 import THE_EXTRACT as E1_Jobscout24
from Final_2 import Fachverantwortung as E2_Jobscout24
from Final_2 import Unternehmensgrösse01 as E3_Jobscout24
from Final_2 import THE_TRANSFORM as T_Jobscout24

#JobsMerged
import Merge_JobsMerged as M
import Aggregation_JobsMerged as A
import Loading_JobsMerged as L

if __name__ == "__main__":
    print("ETL - Process started")
    print("Extraktion und Transformation von Job Agent Daten")
    df = E_Jobagent.create_pd_JobAgent()
    E_Jobagent.save_csv_JobAgent(df)
    T1_Jobagent.main()
    T2_Jobagent.main()
    T3_Jobagent.main()
    T4_Jobagent.main()
    print("Extraktion und Transformation von Indeed Daten")
    E_Indeed.main()
    I_Indeed.main()
    print("Extraktion und Transformation von Jobscout24 Daten")
    E1_Jobscout24.main()
    E2_Jobscout24.main()
    E3_Jobscout24.main()
    T_Jobscout24.main()
    print("Merge alles zusammen")
    M.main()
    print("Beantwortung der Fragen")
    A.main()
    print("Laden in die Maria DB")
    L.main()


    print("ETL - Process finished")
