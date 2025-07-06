import duckdb



query = '''
CREATE OR REPLACE VIEW sessions AS
WITH raw_log AS (
  SELECT *,
         CAST(created_at_utc8 AS TIMESTAMP) AS created_at
  FROM answer_log
),

question_info AS (
  SELECT * FROM question_structure
),

user_info_table AS (
  SELECT * FROM user_info
),

joined_data AS (
  SELECT 
    l.*,
    q.difficulty_level,
    q.subject_name,
    q.section_id,
    q.section_name,
    u.school_city
  FROM raw_log l
  LEFT JOIN question_info q USING (question_id)
  LEFT JOIN user_info_table u USING (user_id)
),

sorted_data AS (
  SELECT *,
         LAG(created_at) OVER (
           PARTITION BY user_id, gamecharacter_id
           ORDER BY created_at
         ) AS prev_time
  FROM joined_data
),

session_flagged AS (
  SELECT *,
         CASE
           WHEN prev_time IS NULL THEN 1
           WHEN created_at - prev_time > INTERVAL 30 MINUTE THEN 1
           ELSE 0
         END AS new_session
  FROM sorted_data
),

session_marked AS (
  SELECT *,
         SUM(new_session) OVER (
           PARTITION BY user_id, gamecharacter_id
           ORDER BY created_at
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         ) AS session_id
  FROM session_flagged
),

session_data AS (
  SELECT
    user_id,
    gamecharacter_id,
    session_id,
    array_agg(is_correct ORDER BY created_at)         AS is_corrects,
    array_agg(difficulty_level ORDER BY created_at)  AS difficulties,
    -- array_agg(experiment_index ORDER BY created_at)  AS experiment_index,
    array_agg(question_id ORDER BY created_at)       AS question_ids,
    FIRST(subject_name)                              AS subject_name,
    FIRST(section_id)                                AS section_id,
    FIRST(section_name)                              AS section_name,
    FIRST(school_city)                               AS school_city,
    MIN(created_at)                                  AS session_start,
    MAX(created_at)                                  AS session_end,
    COUNT(*)                                         AS n_questions
  FROM session_marked
  GROUP BY user_id, gamecharacter_id, session_id
)

SELECT 
    *, 
    EXTRACT(EPOCH FROM (session_end - session_start)) AS time_elapsed
FROM session_data;
'''

if __name__ == "__main__":
    
    # create connection via duckdb
    con = duckdb.connect(database='databases/pagamo-data.db', read_only=False)

    # execute the query to create or replace the view
    try:
        con.execute(query)
        print("Experiment sessions view created successfully.")
    except Exception as err:
        print(f"Error executing query: {err}")
    finally:
        con.close()