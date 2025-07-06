from pathlib import Path

import duckdb



def insert_answer_log_data(data_folder: str = "answer-logs"):
    '''
    
    '''
    
    # create connection via duckdb
    con = duckdb.connect(database='databases/pagamo-data.db', read_only=False)
    
    # format query
    query = f"""
    DROP VIEW IF EXISTS answer_log;
    CREATE OR REPLACE VIEW answer_log AS (
        select distinct *
        from '{data_folder}/*.parquet'  
    )   
    """
    try:
        con.execute(query)
    except Exception as err:
        print(f"Error executing query: {err}")
    finally:
        con.close()
    
    return


def insert_question_structure_data(data_folder: str = "question-structures"):
    '''
    
    '''
    
    # create connection via duckdb
    con = duckdb.connect(database='databases/pagamo-data.db', read_only=False)
    
    # format query
    query = f"""
    DROP VIEW IF EXISTS question_structure;
    CREATE OR REPLACE VIEW question_structure AS (
        select distinct *
        from '{data_folder}/*.parquet'
    )   
    """
    try:
        con.execute(query)
    except Exception as err:
        print(f"Error executing query: {err}")
    finally:
        con.close()
    
    return

def insert_user_info_data(data_folder: str = "user-infos"):
    '''
    
    '''
    
    # create connection via duckdb
    con = duckdb.connect(database='databases/pagamo-data.db', read_only=False)
    
    # format query
    query = f"""
    DROP VIEW IF EXISTS user_info;
    CREATE OR REPLACE VIEW user_info AS (
        select distinct *
        from '{data_folder}/*.parquet'
    )   
    """
    try:
        con.execute(query)
    except Exception as err:
        print(f"Error executing query: {err}")
    finally:
        con.close()
    
    return


def insert_exp_log_data(data_folder: str = "experiment-logs"):
    '''
    
    '''
    
    # create connection via duckdb
    con = duckdb.connect(database='databases/pagamo-data.db', read_only=False)
    
    # format query
    query = f"""
    DROP VIEW IF EXISTS exp_log;
    CREATE OR REPLACE VIEW exp_log AS (
        select distinct *
        from '{data_folder}/*.parquet'
    )   
    """
    try:
        con.execute(query)
    except Exception as err:
        print(f"Error executing query: {err}")
    finally:
        con.close()
    
    return

if __name__ == "__main__":
    # insert answer log data
    insert_answer_log_data(data_folder=Path(__file__).parent / "databases/answer-logs")
    print("Answer log data inserted successfully.")
    
    # insert question structure data
    insert_question_structure_data(data_folder=Path(__file__).parent / "databases/question-structures")
    print("Question structure data inserted successfully.")
    
    # insert user info data
    insert_user_info_data(data_folder=Path(__file__).parent / "databases/user-infos")
    print("User info data inserted successfully.")
    
    # insert experiment log data
    insert_exp_log_data(data_folder=Path(__file__).parent / "databases/experiment-logs")
    print("Experiment log data inserted successfully.")