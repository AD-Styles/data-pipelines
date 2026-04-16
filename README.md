# 🚀 Accelerating Data Engineering Pipelines with RAPIDS & Dask

![Python](https://img.shields.io/badge/Python-3.7+-3776AB?style=flat-square&logo=python&logoColor=white)
![NVIDIA](https://img.shields.io/badge/NVIDIA-CUDA-76B900?style=flat-square&logo=nvidia&logoColor=white)
![RAPIDS](https://img.shields.io/badge/RAPIDS-cuDF-7401F0?style=flat-square&logo=nvidia&logoColor=white)
![Dask](https://img.shields.io/badge/Dask-Parallel-ORANGE?style=flat-square&logo=dask&logoColor=white)

---

## 📌 프로젝트 요약 (Project Overview)
본 프로젝트는 GPU 가속 데이터프레임 라이브러리인 **RAPIDS cuDF**와 분산 처리 프레임워크인 **Dask**를 활용하여 데이터 엔지니어링 파이프라인의 병목 현상을 해결하고 성능을 최적화하는 과정을 담고 있습니다.

---

## 📂 Project Structure

[cite_start]본 프로젝트는 데이터 포맷 분석부터 분산 컴퓨팅 아키텍처 설계까지 단계별로 구성되어 있습니다. 

```text
.
├── 1_Data_Formats.ipynb          # I/O 효율성 분석: JSON, CSV, Parquet 성능 비교 
├── 2_Dask_and_MapReduce.ipynb    # 분산 병렬 처리: Dask & MapReduce 파이프라인 구축 
├── Accelerating_Data_Engineering_Pipelines.pdf # DLI 워크숍 핵심 이론 및 가이드라인 
├── data/                         # 벤치마킹 및 실습용 데이터셋
│   ├── sample_json.txt           # NOAA Tides & Currents API 원본 데이터 
│   ├── sample_data.csv           # 시계열 수위 데이터 (Row-based) 
│   ├── sample_data.parquet       # 칼럼 기반 최적화 데이터 (Columnar) 
│   └── numbers.csv/parquet       # 7500x7500 고차원 벤치마킹 데이터셋 
└── images/                       # 시스템 아키텍처 및 DAG 시각화 이미지

### 1. 처리 엔진 및 라이브러리 비교 (Engine Comparison)
| 구분 | Pandas | RAPIDS cuDF | Dask / dask_cudf |
| :--- | :--- | :--- | :--- |
| **연산 가속** | CPU (Single-core / Multi-thread) | GPU (NVIDIA CUDA Cores) | Multi-node / Multi-GPU Distributed |
| **메모리 관리** | System RAM | GPU VRAM (Apache Arrow) | Distributed Memory Management |
| **평가 방식** | 즉시 실행 (Eager Evaluation) | 즉시 실행 (Eager Evaluation) | [cite_start]지연 평가 (Lazy Evaluation / DAG) [cite: 2] |
| **최적 사례** | 소규모 데이터 및 복잡한 로직 | 단일 GPU 메모리 내 대용량 연산 | 멀티 파일/TB급 대규모 데이터 분산 처리 |

### 2. 데이터 포맷 및 저장 구조 분석 (Data Architecture)
| 포맷 | 저장 방식 | 특징 및 오버헤드 | 최적의 유즈케이스 |
| :--- | :--- | :--- | :--- |
| **CSV** | Row-based (Text) | [cite_start]인간 가독성은 높으나, 특정 열 로드 시 전체 행을 읽는 I/O 병목 발생 [cite: 4, 7] | 소규모 데이터 교환, 초기 탐색 |
| **JSON** | Semi-structured | [cite_start]유연한 스키마를 제공하나 필드 이름 중복으로 인해 저장 공간 낭비 심함 [cite: 4, 5] | Web API 통신, 비정형 데이터 적재 |
| **Parquet** | Columnar-based | 스키마 메타데이터 및 압축 기술 활용. [cite_start]필요한 열만 선택적으로 로드 가능 [cite: 4] | 분석용 데이터 웨어하우스, GPU I/O 최적화 |

### 3. 분산 컴퓨팅 전략 (Parallel Processing Strategy)
| 아키텍처 | 설명 | 세부 기술 요소 |
| :--- | :--- | :--- |
| **DAG (Directed Acyclic Graph)** | 연산의 흐름을 논리적 그래프로 구성하여 최적화된 경로로 실행 | `visualize()`를 통한 작업 스케줄링 가시화 |
| **Map Operation** | 각 워커가 독립적으로 데이터를 변환하는 비동기 병렬 처리 | `map()`, `apply_rows()`를 통한 연산 가속 |
| **Reduce Operation** | 분산된 결과를 하나로 취합하는 동기화 단계 (Aggregate) | `sum()`, `mean()`, `max()` 등의 집계 연산 |
| **Partitions** | 대규모 데이터를 작은 단위로 쪼개어 개별 GPU/Worker에 할당 | 메모리 효율 극대화 및 병렬 로딩 최적화 |

### 4. 확장 개념 (Advanced Terminology)
| 개념 | 설명 | 관련 도구 |
| :--- | :--- | :--- |
| **CRUD** | [cite_start]데이터베이스 및 스토리지의 기본 작업 (Create, Read, Update, Delete) [cite: 1] | Relational Databases 연동 |
| **NVTabular** | [cite_start]추천 시스템 등을 위한 대규모 정형 데이터 가공 가속 라이브러리 [cite: 1] | Deep Learning Pipeline |
| **Plotly Dash** | [cite_start]분석 결과를 실시간 대시보드로 시각화하는 인터랙티브 프레임워크 [cite: 1] | Data Visualization |
| [cite_start]**VRAM Warm-up** | cuDF 초기화 시 발생하는 오버헤드를 줄이기 위한 사전 실행 전략 [cite: 6] | Performance Optimization |
