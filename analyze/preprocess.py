import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import duckdb
import statistics


#md
# # generate the user dataframe
# ```python
# def grade_filter(df:pd.DataFrame,grade:int,grade_colname:str):
#     df = df[df[grade_colname] == grade]
#     return df
#
# # add all response answered by 5th grade students
# def join_answer_df(id_df:pd.DataFrame, ans_df:pd.DataFrame,usr_id = "user_id"):
#     id_filter = id_df[usr_id]
#     mask = ans_df[usr_id].isin(id_filter)
#     df = ans_df[mask]
#     return df
#
# # label the ans generate from experiment
# def add_exp_label(ans_df:pd.DataFrame, exp_df:pd.DataFrame, usr_id="user_id", charater_id="gamecharacter_id"):
#     exp_ids = exp_df[usr_id]
#     bool_list = ans_df[usr_id].isin(exp_ids)
#     ans_df['is_experiment'] = bool_list
#     return ans_df
#
#
# testdf = grade_filter(df_user_info,5,"user_grade")
# testdf = join_answer_df(testdf,df_user_ans,"user_id","user_id")
# testdf = add_exp_label(testdf,df_book_vol)
# ```
# md
# # method to filter the datas
# ## idea
# - our main purpose is to make each session as a observation sequences. Thus, the first steps to take are to make the
# all answers be labeled with the user's info and the question attributions. With such a data frame, we can choose the
# data we need by filtering the grade or subject first and then use the group_by function to label the session (it may
# have multiple sessions in each user\character). After such manipulation we can group by session, user id and character
# id to have the session as an observation.
#
# ## steps
# - step 1: construct function use answer df as main df, and merege the columns from user info df and experiment book df
# - step 2: filter the necessary subjects and drop the missing values
# - step 3: use groupby and shift method to define the sessions
# - step 4: use groupby function to make the date frame with each session as an observation
#
#
def extend_answer_df(ans_df: pd.DataFrame,user_df: pd.DataFrame, exp_df: pd.DataFrame,
                     question_df: pd.DataFrame, course_df: pd.DataFrame):
    exp_df = exp_df[['id','experiment_index']]
    question_df = question_df.drop('section_id',axis='columns')

    df = pd.merge(ans_df, user_df, how='left',on='user_id' )
    df = pd.merge(df, exp_df, how="left", on=['id'] )
    df = pd.merge(df, question_df, how='left', on='question_id')
    df = pd.merge(df, course_df, how='left', on='gamecharacter_id')

    return df
def filter_exp_data(df:pd.DataFrame, grade:int = 5, subject:str = "數學", vol_name:str = '五', course_ids=None):

    grade_mask = df['user_grade'] == grade
    session_mask = pd.Series(False,index=df.index)
    if course_ids is None:
        pass
    else:
        for _id in course_ids:
            session_mask |= df['course_id'] == _id
    subject_mask = df['subject_name'] == subject
    #vol_mask = df['book_volume_name'].str.match(vol_name)
    #mask = grade_mask & subject_mask & vol_mask
    mask =  subject_mask & session_mask & grade_mask

    return df[mask]

def manual_translate_book_vol_name_to_book_vol(df:pd.DataFrame, colname:str = 'book_volume_name') -> pd.DataFrame:
    grade_code_book = {
        '六': 6,
        '五': 5,
        '四': 4,
        '三': 3,
        '二': 2,
        'ㄧ': 1,
    }
    df.loc[:,'manual_book_vol'] = (
                                    df[colname]
                                   .astype(str)
                                   .apply(lambda x : next((v for k,v in grade_code_book.items() if k in x and isinstance(x,str)),0))
                                   .astype(int)
                                   )
    return df

def construct_session_id(df:pd.DataFrame):
    df.loc[:,'created_at'] = pd.to_datetime(df['created_at_utc8'], errors = 'coerce')
    df.sort_values(by = ['created_at'],inplace=True)
    df.loc[:,'prev_time'] = df.groupby(['user_id','gamecharacter_id','section_id'])['created_at'].shift(1)
    df.loc[:,'time_diff'] = (df['created_at'] - df['prev_time']).dt.total_seconds()
    df.loc[:,'new_session_flag'] = ((df['time_diff'].copy() > 30*60 ) | df['time_diff'].copy().isna()).astype(int)
    df.loc[:,'session_id'] = df.groupby("user_id")["new_session_flag"].cumsum()
    return df
def agg_sessions(df:pd.DataFrame) -> pd.DataFrame:
    session_df = df.groupby(['user_id','gamecharacter_id','session_id']).agg({
        'user_id':pd.Series.mode,
        'gamecharacter_id':pd.Series.mode,
        'session_id': pd.Series.mode,
        'is_correct':list,
        'difficulty_level':list,
        'experiment_index':list,
        'question_id':list,
        'subject_name':list,
        'section_id':list,
        'section_name':list,
        'school_city':list,
        'created_at':list,
        'answer':list,
        'book_volume_name':list,
        'is_self_selected':list,
        'manual_book_vol':list,
        'time_diff':list
    })
    session_df.loc[:,'start_time'] = session_df['created_at'].apply(lambda x: min(x))
    session_df.loc[:,'end_time'] = session_df['created_at'].apply(lambda x: max(x))
    session_df.loc[:,'session_length'] = (session_df['end_time'] - session_df['start_time']).dt.total_seconds()
    session_df.loc[:,'answer_length'] = session_df['answer'].apply(lambda x: len(x))
    try:
        session_df.loc[:,'experiment_mode'] = session_df['experiment_index'].apply(lambda x: pd.Series(x).mode())
    except ValueError:
        print("experiment list have more than one mode")
        session_df.loc[:, 'experiment_mode'] = session_df['experiment_index'].apply(lambda x: pd.Series(x).mode()[0])

    try:
        session_df.loc[:,'manual_book_vol_mode'] = session_df['manual_book_vol'].apply(lambda x: pd.Series(x).mode())
    except ValueError:
        print("manual_book_vol_mode list have more than one mode")
        session_df.loc[:, 'manual_book_vol_mode'] = session_df['manual_book_vol'].apply(lambda x: pd.Series(x).mode()[0])

    try:
        session_df.loc[:,'subject_name_mode'] = session_df['subject_name'].apply(lambda x: pd.Series(x).mode() )
    except ValueError:
        print("subject_name_mode list have more than one mode")
        session_df.loc[:, 'subject_name_mode'] = session_df['subject_name'].apply(lambda x: pd.Series(x).mode()[0])

    """
    temp = []
    for idx,row in session_df.iterrows():
        try:
            print(row['experiment_index'])
            temp.append(pd.Series(row['experiment_index']).mode())
        except:
            print(row['experiment_index'])
            temp.append(-99)

    session_df.loc[:,'experiment_mode'] = temp
    """
    return session_df


if __name__ == '__main__':
    #os.chdir('/Users/eric/Documents/SchoolCourses/PaGamO')

    """
    conn = duckdb.connect(
        "/Users/eric/Documents/SchoolCourses/PaGamO/databases/experiment-logs/target_book_volume_log.parquet")
    conn.sql("select * from target_book_volume_log").df()
    """

    #conn = duckdb.connect("./data/raw/experiment-2025-q1/ntuecon_experiment_250525/question_structure.parquet")
    print(os.getcwd())
    df_question = pd.read_parquet("./data/raw/experiment-2025-q1/ntuecon_experiment_250525/question_structure.parquet")

    #conn = duckdb.connect("./data/raw/experiment-2025-q1/ntuecon_experiment_250525/target_book_volume_log.parquet")
    df_book_vol = pd.read_parquet("./data/raw/experiment-2025-q1/ntuecon_experiment_250525/target_book_volume_log.parquet")

    #conn = duckdb.connect("./data/raw/experiment-2025-q1/ntuecon_experiment_250525/target_user_answer_log.parquet")
    df_user_ans = pd.read_parquet("./data/raw/experiment-2025-q1/ntuecon_experiment_250603/target_user_answer_log_2025-06-03.parquet")

    #conn = duckdb.connect("./data/raw/experiment-2025-q1/ntuecon_experiment_250525/target_user_info.parquet")
    df_user_info = pd.read_parquet("./data/raw/experiment-2025-q1/ntuecon_experiment_250603/target_user_info_2025-06-03.parquet")

    #conn = duckdb.connect("./data/raw/target_gc_course_2025-06-03.parquet")
    df_course = pd.read_parquet('./data/raw/experiment-2025-q1/ntuecon_experiment_250603/target_gc_course_2025-06-03.parquet')

    course_ids = [
        -1310737327772698782,
        -7496231066340943725,
        -635781777268086024,
        3624197425649153303,
        9134764154534900321,
        6324319986388160980,
        4842372721563788596,
        1503882658553008361,
        -3524232639045232913,
        455529773227609618,
        -5664811740130319133,
        -2741743464836929110,
        5610930189656743757,
        1514036523562634339,
        -2782015361905585624
    ]

    testdf = extend_answer_df(df_user_ans,df_user_info,df_book_vol,df_question,df_course)
    testdff = filter_exp_data(testdf,course_ids=course_ids)
    testdfff = manual_translate_book_vol_name_to_book_vol(testdff)
    testdffff = construct_session_id(testdfff)
    testdfffff = agg_sessions(testdffff)
    testdfffff.to_json('/content/session_data_new.json')
    sample_df = testdfffff.sample(frac=1 / 3, random_state=42)
    #sample_df.to_csv('./data/processed/sample_session_data.csv')
    testdffff.to_json('/content/question_data_new.json')
    print('finish')
