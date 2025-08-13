# Pagamo experiment data analyze



## 功能

1. 從資料中構建session
2. 以sessions 分析實驗結果

## 檔案結構
檔案結構如下
```
..
├── README.md
├── analyze
│   ├── analyze.ipynb
│   ├── analyze.py
│   ├── analyze_refactored.ipynb
│   ├── experiment_data_analyze.ipynb
│   ├── preprocess.ipynb
│   └── preprocess.py
├── data
│   ├── processed
│   ├── raw
│   └── results
├── databases
├── requirements.txt
├── script
│   └── already exists
└── venv
    

```

主要與分析有關的部分是 analyze 與 data 中的檔案，其中分析程式的用途如下：

- `analyze/preprocess.py` ：從`/data/raw/experiment-2025-q1/`中匯入資料，篩選符合需求的部分建立session，並儲存於 `data/processed/session_data_new.json` 與 `data/processed/question_data_new.json`
- `analyze/experiment_data_analyze.ipynb`：使用`/data/raw/experiment-2025-q1/`,`data/processed/session_data_new.json` 與 `data/processed/question_data_new.json` 中的資料進行分析，同時會輸出一份以章節分離session的`data/processed/session_by_section_data_new.json`
- `analyze/analyze.ipynb`：舊的分析，主要對`session_data.json`進行分析，不確定直接使用`session_data.json`進行分析是否會有問題
- `analyze/analyze_refactored.ipynb`：之前refactor完的程式，並未更新，應該無法使用
- `analyze/preprocess.ipynb`：舊的建立session程式，無法使用

## 複製實驗結果
先執行 `analyze/preprocess.py` 接著執行 `analyze/experiment_data_analyze.ipynb` 便可以得到實驗的結果

## 注意事項
部分的檔案因為過大，無法使用github保存，因此在執行時，請確保data/raw裡面的結構如下，以使得程式能成功執行。
```
data/raw
├── controlled_session_id.csv
├── experiment-2025-q1
│   ├── ntuecon_experiment_250525
│   │   ├── question_structure.parquet
│   │   ├── target_book_volume_log.parquet
│   │   ├── target_user_answer_log.parquet
│   │   └── target_user_info.parquet
│   ├── ntuecon_experiment_250603
│   │   ├── target_book_volume_log_2025-06-03.parquet
│   │   ├── target_gc_course_2025-06-03.parquet
│   │   ├── target_gc_daily_stat_2025-06-03.parquet
│   │   ├── target_user_answer_log_2025-06-03.parquet
│   │   └── target_user_info_2025-06-03.parquet
│   ├── README.md
│   └── target_gc_daily_stat_2025-06-03.parquet
├── question_sequence.csv
└── target_gc_daily_stat_2025-06-03 (1).parquet
```


