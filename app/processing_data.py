import glob
import time
from cassandra.cluster import Cluster
# All files and directories ending with .txt and that don't begin with a dot:
print(glob.glob("D:\\kafka-demo\\output\\*.csv")) 
# doc tat ca cac data cu
list_old = glob.glob("D:\\kafka-demo\\output\\*.csv")

cluster = Cluster(['localhost'])
session = cluster.connect('k1')

columns = ['ID' ,'QUARTER' ,'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 
            'OP_UNIQUE_CARRIER',
            'ORIGIN',
            'DEST','DISTANCE',
            'CRS_DEP_TIME', 'DEP_TIME', 
            'DEP_DELAY','DEP_DELAY_NEW','DEP_DEL15',
            'ARR_DEL15','prediction']

dataframes_list_old = []
dataframes_list_total_old = [] 
import pandas as pd
for i in range(len(list_old)):
    dataframes_list_old = pd.read_csv(list_old[i], names = columns)
    dataframes_list_total_old.append(dataframes_list_old)
    
df = pd.concat(dataframes_list_total_old).reset_index(drop=True)



for i in range(len(df)):
    session.execute(
        "INSERT INTO stream_data (id, quarter, month, day_of_month, day_of_week, "
        "op_unique_carrier, origin, dest, distance, crs_dep_time, dep_time, "
        "dep_delay, dep_delay_new, dep_del15, arr_del15) VALUES ("
        "'" + str(df.iloc[:,0][i]) + "', "           # id (text)
        + str(df.iloc[:,1][i]) + ", "                 # quarter (int)
        + str(df.iloc[:,2][i]) + ", "                 # month (int)
        + str(df.iloc[:,3][i]) + ", "                 # day_of_month (int)
        + str(df.iloc[:,4][i]) + ", "                 # day_of_week (int)
        "'" + str(df.iloc[:,5][i]) + "', "            # op_unique_carrier (text)
        "'" + str(df.iloc[:,6][i]) + "', "            # origin (text)
        "'" + str(df.iloc[:,7][i]) + "', "            # dest (text)
        + str(df.iloc[:,8][i]) + ", "                  # distance (double)
        + str(df.iloc[:,9][i]) + ", "                  # crs_dep_time (int)
        + str(df.iloc[:,10][i]) + ", "                 # dep_time (double)
        + str(df.iloc[:,11][i]) + ", "                 # dep_delay (double)
        + str(df.iloc[:,12][i]) + ", "                 # dep_delay_new (double)
        + str(df.iloc[:,13][i]) + ", "                 # dep_del15 (double)
        + str(df.iloc[:,14][i]) + ")"                   # arr_del15 (double)
    )

while True:
# doc tat ca cac data moi
    time.sleep(15)
    list_new = glob.glob("D:\\kafka-demo\\output\\*.csv")
    res = set(list_new) - set(list_old)
    if (res != set()): 
        columns = ['ID' ,'QUARTER' ,'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 
                    'OP_UNIQUE_CARRIER',
                    'ORIGIN',
                    'DEST','DISTANCE',
                    'CRS_DEP_TIME', 'DEP_TIME', 
                    'DEP_DELAY','DEP_DELAY_NEW','DEP_DEL15',
                    'ARR_DEL15','prediction']


        dataframes_list = []
        dataframes_list_total = [] 
        import pandas as pd
        for i in range(len(res)):
            dataframes_list = pd.read_csv(list(res)[i], names = columns)
            dataframes_list_total.append(dataframes_list)
            
        df_stream = pd.concat(dataframes_list_total).reset_index(drop=True)
        
# chua sua phan trong ngoac
        for i in range(len(df_stream)):
            session.execute("Insert into stream_data (id,quarter,month,day_of_month, day_of_week, \
                op_unique_carrier,origin, dest,distance, crs_dep_time, dep_time, \
                dep_delay,dep_delay_new,dep_del15,arr_del15) values ("+"'"+str(df_stream.iloc[:,0][i])+"'"+","+str(df_stream.iloc[:,1][i])+"," 
                +str(df_stream.iloc[:,2][i])+","+str(df_stream.iloc[:,3][i])+","+str(df_stream.iloc[:,4][i])+","+"'"+ str(df_stream.iloc[:,5][i])+"'"+
                    ","+"'"+str(df_stream.iloc[:,6][i])+"'"+","+"'"+str(df_stream.iloc[:,7][i])+"'"+","+ 
                        str(df_stream.iloc[:,8][i])+","+str(df_stream.iloc[:,9][i])+","+str(df_stream.iloc[:,10][i])+","+str(df_stream.iloc[:,11][i])+","+str(df_stream.iloc[:,12][i])+","+str(df_stream.iloc[:,13][i])+","+str(df_stream.iloc[:,14][i])+ ")")

        df_stream.to_csv("D:\\kafka-demo\\data_final\\display.csv", index = False)
        list_old = list_new
    # for i in range(2):
    #     import subprocess

    #     spark_submit_str= "spark-submit test.py"
    #     process=subprocess.Popen(spark_submit_str,stdout=subprocess.PIPE,stderr=subprocess.PIPE, universal_newlines=True, shell=True)
    #     stdout,stderr = process.communicate()
    #     if process.returncode !=0:
    #         print(stderr)
    #     print(stdout)