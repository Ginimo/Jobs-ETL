from Extract_Beautiful_Soup import Extract10 as E
from Transform import Transform_df_jobs02 as T1
from Transform import Transform_df_jobs_company_size01 as T2
from Transform import Transform_df_wiki01 as T3
from Transform import Transform_df_jobs_merged01 as T4
from Transform import Aggregation03 as A
from Loading import Loading01 as L


if __name__ == "__main__":
    print("ETL - Process started")
    df = E.create_pd_JobAgent()
    E.save_csv_JobAgent(df)
    T1.main()
    T2.main()
    T3.main()
    T4.main()
    A.main()
    L.main()
    print("ETL - Process finished")
