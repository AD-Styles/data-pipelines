import cudf
import pandas as pd
import time
import os

class Timer:
    """코드 실행 시간을 측정하기 위한 컨텍스트 매니저 """
    def __enter__(self):
        self.start = time.perf_counter() # 
        return self
    
    def __exit__(self, *args):
        self.end = time.perf_counter() # 
        self.interval = self.end - self.start # 

def run_benchmark(file_path):
    """지정된 파일에 대해 Pandas와 cuDF의 로딩 속도를 비교합니다."""
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    print(f"\n--- Benchmarking: {file_path} ---")
    
    # 1. Pandas (CPU) 데이터 로딩
    with Timer() as t_pd: # 
        if file_path.endswith('.csv'):
            df_cpu = pd.read_csv(file_path) # 
        elif file_path.endswith('.parquet'):
            df_cpu = pd.read_parquet(file_path) # 
        elif file_path.endswith('.txt') or file_path.endswith('.json'):
            df_cpu = pd.read_json(file_path, orient="records") # 
    
    # 2. cuDF (GPU) 데이터 로딩
    with Timer() as t_cudf: # 
        if file_path.endswith('.csv'):
            df_gpu = cudf.read_csv(file_path) # 
        elif file_path.endswith('.parquet'):
            df_gpu = cudf.io.parquet.read_parquet(file_path) # 
        elif file_path.endswith('.txt') or file_path.endswith('.json'):
            df_gpu = cudf.read_json(file_path) # 
            
    print(f"Pandas (CPU): {t_pd.interval:.5f}s")
    print(f"cuDF (GPU)  : {t_cudf.interval:.5f}s")
    
    if t_cudf.interval > 0:
        print(f"Speedup     : {t_pd.interval / t_cudf.interval:.2f}x")

if __name__ == "__main__":
    # 벤치마킹 실행 예시 (데이터 경로에 맞게 수정하여 사용)
    # run_benchmark("data/sample_data.csv")
    # run_benchmark("data/sample_data.parquet")
    pass
