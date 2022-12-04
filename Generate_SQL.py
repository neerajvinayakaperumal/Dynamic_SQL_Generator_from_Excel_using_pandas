import pandas as pd

def generate_SQL_string(df):

    # Derive SELECT statement

    Select_statement = ""
    df_select_columns = df[(df["Select_Transformation_Logic"].isnull()) & (df["Group_Condition"].isnull())][["Source_Table", "Source_Column", "Target_Column"]]
    df_select_columns["result"] =  df_select_columns["Source_Table"] + "." + df_select_columns["Source_Column"] + " as " +df_select_columns["Target_Column"]

    df_select_derived_columns = df[df["Select_Transformation_Logic"].notnull()][["Select_Transformation_Logic", "Target_Column"]]
    df_select_derived_columns["result"] =  df_select_derived_columns["Select_Transformation_Logic"] + " as " + df_select_derived_columns["Target_Column"]

    Select_string_list = df_select_columns["result"].tolist() + df_select_derived_columns["result"].tolist()

    Select_statement = " , ".join(Select_string_list)



    # Derive FROM statement

    from_table_list = df[df["Source_Table"].notnull()]["Source_Table"].unique().tolist()

    From_statement = ""
    Join_statement = ""
    if (len(from_table_list) == 1):
        From_statement = from_table_list[0]

    elif(len(from_table_list) > 1):
        df_join_conditions = df[df["Join_Condition"].notnull()][["Source_Table", "Join_Type", "Join_Condition"]]
        df_join_conditions["result"] = " " + df_join_conditions["Join_Type"] + " " + df_join_conditions["Source_Table"] + " ON " + df_join_conditions["Join_Condition"]

        get_join_tables_list = df_join_conditions["Source_Table"].unique().tolist()
        get_non_join_tables_list = df["Source_Table"].unique().tolist()
        identify_main_table = [item for item in get_non_join_tables_list if item not in get_join_tables_list][0]

        if(len(get_join_tables_list) >= 1 and len(get_join_tables_list) + 1 >= len(from_table_list)):
            Join_statement = " ".join(df_join_conditions["result"].tolist())
        else:
            print("Not enough join conditions ", len(from_table_list) , len(get_join_tables_list))

        From_statement = identify_main_table + " " + Join_statement


    # Derive WHERE statement

    Where_statement = ""
    df_where_columns_list = df[df["Filter_Condition"].notnull()]["Filter_Condition"].unique().tolist()
    if len(df_where_columns_list) > 1:
        Where_statement = "( " + " ) AND ( ".join(df_where_columns_list) + " ) "
    else:
        Where_statement = df_where_columns_list[0]



    # Derive GROUP BY statement

    Select_statement_aggregate = ""
    Group_by_statement = ""


    df_aggregate_functions = df[df["Group_Condition"].notnull()][["Group_Condition","Target_Column"]]
    df_aggregate_functions["result"] = df["Group_Condition"] + " as " + df["Target_Column"]
    Select_statement_aggregate = " , ".join(df_aggregate_functions["result"].unique().tolist())


    df_groupby_columns = df[df["Group_Condition"].isnull()][["Source_Table","Source_Column","Select_Transformation_Logic"]]
    df_groupby_columns["direct_result"] = df_groupby_columns["Source_Table"] + "." + df_groupby_columns["Source_Column"]
    df_groupby_columns["derived_result"] = df_groupby_columns["Select_Transformation_Logic"]

    Group_by_statement = " , ".join(df_groupby_columns[df_groupby_columns["direct_result"].notnull()]["direct_result"].tolist()) + " , " +\
                         " , ".join(df_groupby_columns[df_groupby_columns["derived_result"].notnull()]["derived_result"].tolist())



    # Derive HAVING statement

    Having_statement = ""
    df_having_columns_list = df[df["Group_Filter_Condition"].notnull()]["Group_Filter_Condition"].unique().tolist()
    if len(df_having_columns_list) > 1:
        Having_statement = "( " + " ) AND ( ".join(df_having_columns_list) + " ) "
    else:
        Having_statement = df_having_columns_list[0]





    # Derive SQL statement

    Select_string = " SELECT " + Select_statement + " , " + Select_statement_aggregate
    From_string = " FROM " + From_statement
    Where_string = " WHERE " + Where_statement
    Group_by_string = " GROUP BY " + Group_by_statement
    Having_string = " HAVING " + Having_statement
    Final_SQL = Select_string + '\n' + From_string + '\n' + Where_string + '\n' + Group_by_string + '\n' + Having_string



    return(Final_SQL)







