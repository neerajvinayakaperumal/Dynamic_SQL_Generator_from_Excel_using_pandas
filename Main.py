import pandas as pd
import os,re
from Generate_SQL import *

Mappping_doc_file_name_list = os.listdir("Mapping_Doc")
print(Mappping_doc_file_name_list)

for Mapping_doc_file_name in Mappping_doc_file_name_list:
    df = pd.read_excel("Mapping_Doc\\" + Mapping_doc_file_name)
    # Generate SQL query
    Query = generate_SQL_string(df)
    print(Query)
    # Write SQL query to file
    Table_name = re.search("^[^\.]*", Mapping_doc_file_name).group()
    f = open(Table_name + ".sql", "w")
    f.write(Query)
    f.close()

