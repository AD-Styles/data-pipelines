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
| 구분 &emsp;&emsp;&emsp;&emsp; | Pandas | RAPIDS cuDF | Dask / dask_cudf |
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

---

## 📊 시각화로 이해하는 병렬 처리와 깨달음 (Insights through Visualization)

### 1. MapReduce DAG 분석
Dask의 지연 평가(Lazy Evaluation) 기능을 통해 생성된 작업 흐름도(DAG)입니다. 4개의 파티션이 동시에 연산되는 과정을 한눈에 볼 수 있습니다.

![MapReduce DAG Visualization](images/mapreduce_dag.png)

| 단계 (Step) | 시각화 노드 &emsp;&emsp; | 핵심 포인트 |
| :--- | :---: | :--- |
| **Data Loading** | 맨 아래 사각형 | 데이터를 4개의 `도시락`처럼 나누어 담는 과정 |
| **Map Step** | `add` 원형 | 각 도시락마다 반찬(`+10`)을 동시에 넣는 과정. 서로 기다릴 필요가 없어 아주 빠름! |
| **Reduce Step** | `sum-tree` 원형 | 각자 만든 반찬들을 한곳에 모아 `Total Sum`을 만드는 과정. 이때는 소통이 필요함! |


### 2. 프로젝트를 통해 얻은 '진짜' 인사이트 (Key Lessons)

* **VRAM Warm-up의 발견:** 처음에 cuDF로 파일을 읽을 때 생각보다 느려서 당황했지만 두 번째 실행부터는 속도가 비약적으로 빨라지는 것을 보고, GPU 라이브러리 초기화에 필요한 **'Warm-up'** 시간의 존재를 몸소 깨달았습니다.
* **그릇(포맷)의 중요성:** 똑같은 데이터라도 CSV보다 Parquet 형식이 훨씬 가볍고 빠르다는 점이 인상적이었습니다. 모델링 기법을 고민하기 전에, 데이터를 담는 **'그릇'** 부터 최적화하는 것이 엔지니어링의 시작임을 배웠습니다.
* **효율적인 기다림, Lazy Evaluation:** 코드를 쓰자마자 실행되는 게 아니라, 전체적인 '계획(DAG)'을 먼저 세우고 마지막에 몰아서 처리하는 방식이 대규모 데이터 처리에서 얼마나 큰 효율을 주는지 이해하게 되었습니다.


---

## 💡 회고록 (Retrospective)
&emsp;&emsp;개인 학습 및 포트폴리오로 CNN이나 Transformer 같은 딥러닝 모델을 구현하다 보면, 항상 예상치 못한 곳에서 거대한 벽을 마주하곤 했습니다. 모델의 구조를 짜고 하이퍼파라미터를 튜닝하는 시간보다, 대용량의 학습 데이터를 메모리에 올리고 전처리하는 데 훨씬 더 많은 시간이 소요된다는 점이었습니다. 그동안 당연하게 사용해 왔던 Pandas는 데이터의 규모가 커질수록 CPU 단일 코어의 한계에 부딪혀 심각한 병목 현상을 일으켰습니다. AI 모델의 성능을 높이기 위해서는 단순히 모델 아키텍처뿐만 아니라, 데이터가 모델로 흘러 들어가는 '파이프라인' 자체를 최적화해야 한다는 것을 깨닫고 이번 가속화 프로젝트를 시작하게 되었습니다
<br>&emsp;&emsp;가장 먼저 바꾼 건 그동안 편하다는 이유로 고집했던 CSV와 JSON 포맷이었습니다. 하지만 행(Row) 기반 저장 방식이 분석 시 불필요한 I/O를 발생시킨다는 것을 알게 되었습니다. 이를 열(Columnar) 기반의 Parquet 포맷으로 전환함으로써, 필요한 피처(Feature)만 선택적으로 로드하여 메모리 대역폭을 획기적으로 아낄 수 있었습니다. 이어서 연산 속도를 높이기 위해 NVIDIA cuDF를 써봤는데, CPU로 끙끙대던 작업이 GPU를 만나자마자 순식간에 끝나는 걸 보고 정말 시원한 기분이 들었습니다. 또한 단일 GPU 메모리를 초과하는 대규모 데이터는 어떻게 처리할 것인가에 대한 해답으로 Dask를 활용했습니다. 연산을 즉시 실행하지 않고 지연 평가(Lazy Evaluation)를 통해 '계획(DAG)'부터 세우는 방식이 마치 복잡한 퍼즐을 미리 맞춰보는 것 같아 매우 흥미로웠습니다.
<br>&emsp;&emsp;지금까지의 공부가 주로 '어떻게 더 똑똑한 AI 모델을 만들 것인가'에 머물러 있었다면, 이번 프로젝트는 **'어떻게 AI 모델에 데이터를 효율적으로 먹일 것인가'** 로 시야를 넓혀준 결정적인 계기가 되었습니다. 이전까지는 똑똑한 모델을 만드는 법만 고민했다면, 이제는 데이터가 담긴 그릇(저장 방식)부터 요리하는 도구(CPU vs GPU)까지 꼼꼼히 살피는 게 진짜 실력이라는 걸 배웠습니다. 앞으로 RAG나 멀티모달 같은 고급 기술을 적용할 때에도, 보이지 않는 곳에서 묵묵히 데이터를 나르는 '파이프라인'의 중요성을 잊지 않는 꼼꼼한 엔지니어가 되고 싶습니다.
