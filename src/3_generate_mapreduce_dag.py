# "이 스크립트를 로컬에서 완벽하게 실행하려면 OS 수준에서 sudo apt-get install graphviz가 필요할 수 있습니다."


import dask.dataframe as dd # Dask 데이터프레임 라이브러리 (cite: 2_dask_mapreduce.py)
try:
    import dask_cudf # GPU 가속 Dask-cuDF 라이브러리 (cite: 2_dask_mapreduce.py)
except ImportError:
    dask_cudf = None # dask_cudf가 없는 환경에서도 나머지 로직이 실행되도록 무시합니다.
import pandas as pd # 임시 데이터 생성을 위한 Pandas
import glob # 파일 패턴 검색 (cite: 2_dask_mapreduce.py)
import os # 폴더 생성 등 시스템 작업 (cite: 1_benchmark_io.py, 2_dask_mapreduce.py)

def generate_mapreduce_dag_image(file_pattern, output_path="images/mapreduce_dag.png"):
    """
    2_dask_mapreduce.py의 파이프라인 구조를 시각화하여 지정된 이미지 파일로 저장합니다.
    (실제 데이터 유무와 상관없이 DAG 구조 자체를 시각화하기 위해 임시 데이터를 활용합니다.)
    """
    
    # 1. 이미지 저장 폴더 생성 (1_benchmark_io.py의 폴더 생성 로직 참고)
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"[{file_pattern}] 패턴에 대한 Dask DAG 시각화를 준비합니다...")
    
    # --- 2_dask_mapreduce.py의 파이프라인 구조 (cite: 2_dask_mapreduce.py) ---
    # 실제 데이터 유무와 상관없이 Dask-cuDF / Dask DataFrame 객체는 생성 가능합니다.
    # (실제 실행하려면 파일이 필요하므로, 이 코드에서는 npartitions로 병렬화 구조를 보여줍니다.)

    # (데이터가 없더라도, dask_cudf.read_csv 등으로 객체 생성 가능. 
    # npartitions는 실제 파일 로드 시 파일 수에 비례하여 결정됨. 예시로 npartitions=4 설정)
    
    # 2. 병렬 데이터 로딩 (Lazy Evaluation) - cite: 2_dask_mapreduce.py
    # 데이터를 여러 파티션(워커)으로 나누어 로딩하는 단계를 DAG에 반영합니다.
    # ddf = dask_cudf.read_csv(file_pattern, header=0, npartitions=4) # 주석 처리된 코드 기반

    # 임시 데이터를 활용해 DAG 구조 만들기 (예: 100행 데이터를 4개 파티션으로 분할)
    # 실제 데이터 유무와 관계없이 동일한 형태 양식의 DAG를 그릴 수 있습니다.
    pdf = pd.DataFrame({"Date Time": ["2026-04-16"]*100, 
                        "Water Level": range(100), 
                        "Sigma": [0.1]*100})
    ddf = dd.from_pandas(pdf, npartitions=4) # Dask DataFrame 사용 (GPU 없어도 실행 가능하도록)
    
    ddf.columns = ["Date Time", "Water Level", "Sigma"] # (cite: 2_dask_mapreduce.py)
    
    # 3. Map Operation (비동기 병렬 변환) - cite: 2_dask_mapreduce.py
    # 각 워커가 독립적으로 데이터를 변환하는 작업을 DAG에 추가합니다. (예: Water Level + 10)
    map_op = ddf["Water Level"] + 10 #
    
    # 4. Reduce Operation (동기화 및 집계) - cite: 2_dask_mapreduce.py
    # 분산된 결과를 집계하는 최종 Reduce 작업을 DAG에 추가합니다.
    final_aggregate = map_op.sum() #
    
    # --- DAG 시각화 및 이미지 파일 저장 (cite: 2_dask_mapreduce.py 주석 참고) ---
    print(f"DAG 그래프를 시각화하여 '{output_path}'에 저장합니다...")
    try:
        # format 매개변수로 png, pdf 등 지정 가능
        final_aggregate.visualize(filename=output_path, format="png")
        print("✅ 성공적으로 이미지가 저장되었습니다!")
    except Exception as e:
        print("❌ 로컬 Graphviz를 통한 렌더링에 실패했습니다. 온라인 API 렌더링으로 대체 시도합니다...")
        try:
            import urllib.request
            import json
            import dask.dot
            cg = dask.dot.to_graphviz(final_aggregate.dask)
            
            # 보다 입체적이고 고급스러운 디자인 템플릿(다크 모드 + 그라데이션) 주입
            premium_style = (
                '\n    graph [bgcolor="#0f172a", pad="0.2", ranksep="0.5", nodesep="0.5"];\n'
                '    node [style="filled", fillcolor="#1e293b:#334155", gradientangle="270", color="#0ea5e9", penwidth="2.5", fontcolor="#fef08a", fontname="Segoe UI Bold, Helvetica Bold", fontsize="20", margin="0.2,0.1"];\n'
                '    edge [color="#94a3b8", penwidth="2.0", arrowsize="1.0"];\n'
            )
            
            # 기존 DOT 코드를 가로채어 최상단 설정부에 커스텀 스타일 삽입
            dot_source = cg.source.replace('{', '{' + premium_style, 1)

            # dask 데이터의 기존 구조 지정 속성과 병합되어 렌더링 됨
            api_url = 'https://quickchart.io/graphviz?format=png'
            data = json.dumps({'graph': dot_source, 'format': 'png'}).encode('utf-8')
            req = urllib.request.Request(api_url, data=data, headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(req) as response:
                with open(output_path, 'wb') as f:
                    f.write(response.read())
            print("✅ 온라인 API를 통해 성공적으로 이미지가 저장되었습니다!")
        except Exception as api_e:
            print("❌ 온라인 API 렌더링마저 실패했습니다.")
            print(f"에러 메시지: {api_e}")

if __name__ == "__main__":
    # 데이터 경로 패턴 (2_dask_mapreduce.py의 기본값 data/*.csv 사용)
    data_pattern = "data/*.csv"
    
    # 시각화 실행 및 images/ 폴더에 저장
    generate_mapreduce_dag_image(data_pattern, output_path="images/mapreduce_dag.png")
