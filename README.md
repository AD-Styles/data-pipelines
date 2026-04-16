# 🚀 Accelerating Data Engineering Pipelines with RAPIDS & Dask

![Python](https://img.shields.io/badge/Python-3.7+-3776AB?style=flat-square&logo=python&logoColor=white)
![NVIDIA](https://img.shields.io/badge/NVIDIA-CUDA-76B900?style=flat-square&logo=nvidia&logoColor=white)
![RAPIDS](https://img.shields.io/badge/RAPIDS-cuDF-7401F0?style=flat-square&logo=nvidia&logoColor=white)
![Dask](https://img.shields.io/badge/Dask-Parallel-ORANGE?style=flat-square&logo=dask&logoColor=white)

---

## 📌 프로젝트 요약 (Project Overview)
본 프로젝트는 GPU 가속 데이터프레임 라이브러리인 **RAPIDS cuDF**와 분산 처리 프레임워크인 **Dask**를 활용하여 데이터 엔지니어링 파이프라인의 병목 현상을 해결하고 성능을 최적화하는 과정을 담고 있습니다.

---

## 📂 프로젝트 구조 (Project Structure)
```text
📂 images/                           
    └── mapreduce_dag.png               # MapReduce DAG 시각화 결과물
📂 src/                                 
    └── 1_benchmark_io.py               # I/O 효율성 분석: JSON, CSV, Parquet 성능 비교 스크립트
    └── 2_dask_mapreduce.py             # 분산 병렬 처리: Dask & MapReduce 파이프라인 스크립트
    └── 3_generate_mapreduce_dag.py     # Dask DAG 시각화 이미지 생성 스크립트
├── .gitignore                 # GitHub 업로드 제외 데이터 및 캐시 설정
├── LICENSE                    # MIT License (AD-Styles)
├── README.md                  # 프로젝트 핵심 요약 및 설명서
└── requirements.txt           # 프로젝트 실행에 필요한 파이썬 패키지 목록 
```

---

## 🛠 핵심 기술 개념 (Core Technical Concepts)

### 1. 처리 엔진 및 라이브러리 비교 (Engine Comparison)
| 구분 &emsp;&emsp;&emsp; | Pandas | RAPIDS cuDF | Dask / dask_cudf |
| :--- | :--- | :--- | :--- |
| **연산 가속** | CPU (Single-core / Multi-thread) | GPU (NVIDIA CUDA Cores) | Multi-node / Multi-GPU Distributed |
| **메모리 관리** | System RAM | GPU VRAM (Apache Arrow) | Distributed Memory Management |
| **평가 방식** | 즉시 실행 (Eager Evaluation) | 즉시 실행 (Eager Evaluation) | 지연 평가 (Lazy Evaluation / DAG) |
| **최적 사례** | 소규모 데이터 및 복잡한 로직 | 단일 GPU 메모리 내 대용량 연산 | 멀티 파일/TB급 대규모 데이터 분산처리 |

### 2. 데이터 포맷 및 저장 구조 분석 (Data Architecture)
| 포맷 | 저장 방식 | 특징 및 오버헤드 | 최적의 유즈케이스 |
| :--- | :--- | :--- | :--- |
| **CSV** | Row-based (Text) | 인간 가독성은 높으나, 특정 열 로드 시 전체 행을 읽는 I/O 병목 발생 | 소규모 데이터 교환, 초기 탐색 |
| **JSON** | Semi-structured | 유연한 스키마를 제공하나 필드 이름 중복으로 인해 저장 공간 낭비 심함 | Web API 통신, 비정형 데이터 적재 |
| **Parquet** | Columnar-based | 스키마 메타데이터 및 압축 기술을 활용하여 필요한 열만 선택적으로 로드 가능 | 분석용 데이터 웨어하우스, GPU I/O 최적화 |

### 3. 분산 컴퓨팅 전략 (Parallel Processing Strategy)
| 아키텍처 | 설명 | 세부 기술 요소 |
| :--- | :--- | :--- |
| **DAG (Directed Acyclic Graph)** | 연산의 흐름을 논리적 그래프로 구성하여 최적화된 경로로 실행 | `visualize()`를 통한 작업 스케줄링 가시화 |
| **Map Operation** | 각 워커가 독립적으로 데이터를 변환하는 비동기 병렬 처리 | `map()`, `apply_rows()`를 통한 연산 가속 |
| **Reduce Operation** | 분산된 결과를 하나로 취합하는 동기화 단계 (Aggregate) | `sum()`, `mean()`, `max()` 등의 집계 연산 |
| **Partitions** | 대규모 데이터를 작은 단위로 쪼개어 개별 GPU/Worker에 할당 | 메모리 효율 극대화 및 병렬 로딩 최적화 |

**📌 MapReduce DAG 시각화 (4 Partitions)**
Dask의 지연 평가(Lazy Evaluation)를 통해 구성된 병렬 처리 파이프라인의 작업 흐름도입니다.
![MapReduce DAG Visualization](images/mapreduce_dag.png)

### 4. 확장 개념 (Advanced Terminology)
| 개념 | 설명 | 관련 도구 |
| :--- | :--- | :--- |
| **CRUD** | 데이터베이스 및 스토리지의 기본 작업 (Create, Read, Update, Delete) | Relational Databases 연동 |
| **NVTabular** | 추천 시스템 등을 위한 대규모 정형 데이터 가공 가속 라이브러리 | Deep Learning Pipeline |
| **Plotly Dash** | 분석 결과를 실시간 대시보드로 시각화하는 인터랙티브 프레임워크 | Data Visualization |
| **VRAM Warm-up** | cuDF 초기화 시 발생하는 오버헤드를 줄이기 위한 사전 실행 전략 | Performance Optimization |

## 💡 회고록 (Retrospective)
&emsp;&emsp;개인 포트폴리오로 CNN이나 Transformer 같은 딥러닝 모델을 구현하다 보면, 항상 예상치 못한 곳에서 거대한 벽을 마주하곤 했습니다. 모델의 구조를 짜고 하이퍼파라미터를 튜닝하는 시간보다, 대용량의 학습 데이터를 메모리에 올리고 전처리하는 데 훨씬 더 많은 시간이 소요된다는 점이었습니다. 그동안 당연하게 사용해 왔던 Pandas는 데이터의 규모가 커질수록 CPU 단일 코어의 한계에 부딪혀 심각한 병목 현상을 일으켰습니다. AI 모델의 성능을 높이기 위해서는 단순히 모델 아키텍처뿐만 아니라, 데이터가 모델로 흘러 들어가는 '파이프라인' 자체를 최적화해야 한다는 것을 깨닫고 이번 가속화 프로젝트를 시작하게 되었습니다
<br>&emsp;&emsp;기존에는 익숙하다는 이유로 무조건 CSV나 JSON 포맷을 사용했습니다. 하지만 행(Row) 기반 저장 방식이 분석 시 불필요한 I/O를 발생시킨다는 것을 알게 되었습니다. 이를 열(Columnar) 기반의 Parquet 포맷으로 전환함으로써, 필요한 피처(Feature)만 선택적으로 로드하여 메모리 대역폭을 획기적으로 아낄 수 있었습니다. 이 과정에서 연산 병목을 해결하기 위해 NVIDIA RAPIDS의 cuDF를 도입했습니다. 기존 호스트 메모리(RAM)와 CPU에 의존하던 연산을 GPU VRAM과 CUDA 코어로 옮기자, 수만 행 이상의 데이터 처리 시간이 초 단위로 단축되는 것을 직접 두 눈으로 확인했습니다. 또한 단일 GPU 메모리를 초과하는 대규모 데이터는 어떻게 처리할 것인가에 대한 해답으로 Dask를 활용했습니다. 연산을 즉시 실행하지 않고 지연 평가(Lazy Evaluation)를 통해 DAG(방향성 비순환 그래프)를 먼저 구성한 뒤, MapReduce 방식으로 여러 워커에 작업을 분산시키는 아키텍처를 직접 구현해 보며 분산 처리 시스템의 동작 원리를 깊이 이해하게 되었습니다.
<br>&emsp;&emsp;지금까지의 공부가 주로 '어떻게 더 똑똑한 AI 모델을 만들 것인가'에 머물러 있었다면, 이번 프로젝트는 **'어떻게 AI 모델에 데이터를 효율적으로 먹일 것인가'**로 시야를 넓혀준 결정적인 계기가 되었습니다. 데이터의 물리적 저장 방식, 하드웨어(CPU vs GPU)의 특성, 그리고 분산 스케줄링 알고리즘까지 아우르는 데이터 엔지니어링 역량은 실제 AI 산업 현장에서 필수적이라는 것을 배웠습니다. 앞으로는 RAG 시스템이나 멀티모달 AI 애플리케이션을 개발할 때에도, 단순히 API를 연결하는 것에 그치지 않고 백엔드의 데이터 파이프라인과 인프라 효율성까지 종합적으로 고려하는 '풀스택 AI 엔지니어'로 성장하고 싶습니다.
