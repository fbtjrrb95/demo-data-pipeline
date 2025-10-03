# Data Pipeline
MySQL -> Kafka -> Elastic Search

## 개요
목표: MySQL 변경사항을 Debezium Kafka Connect로 CDC하여 Kafka 토픽으로 전달하고, Kafka UI/Kibana로 데이터와 지표를 확인하는 로컬 개발용 파이프라인.

### 주요 스택
| 구성요소                  | 이미지/의존성(Artifact)                        | 버전              |
|-----------------------| ---------------------------------------- | --------------- |
| MySQL                 | `mysql`                                  | **8.0.33**      |
| Kafka Broker-00,01,02 | `apache/kafka`                           | **3.7 (KRaft)** |
| Kafka Connect         | `debezium/connect`                       | **2.6**         |
| Kafka UI              | `provectuslabs/kafka-ui:latest`          | **latest**      |
| Elasticsearch         | `docker.elastic.co/elasticsearch/elasticsearch` | **8.13.4**      |
| Kibana                | `docker.elastic.co/kibana/kibana`        | **8.13.4**      |
| Spring Boot           | `org.springframework.boot:spring-boot-starter` | **3.3.x**       |
| Spring for Kafka      | `org.springframework.kafka:spring-kafka` | **3.2.x**       |
| Jackson (Kotlin)      | `com.fasterxml.jackson.module:jackson-module-kotlin` | **2.18.0**      |
| Spring Data ES        | `org.springframework.boot:spring-boot-starter-data-elasticsearch` | **5.3.x**       |
| ES Java Client        | `co.elastic.clients:elasticsearch-java`  | **8.13.x**      |

## 아키텍처
```text
┌────────────┐         CDC (binlog)           ┌──────────────────────┐
│  MySQL 8   │  ────▶  Debezium MySQL ────▶   │  Kafka Connect       │
│  (db:3306) │         (connector)            │                      │
└────┬───────┘                                └──────────┬───────────┘
                                                         │                                              
                                                         │                                                     
                                                         ▼                                                     
┌────────────────────────────────────────────────────────────────────────┐
│                       Apache Kafka (KRaft)                             │
│         ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│         │ broker-00    │  │ broker-01    │  │ broker-02    │           │
│         │              │  │              │  │              │           │ 
│         └──────┬───────┘  └──────┬───────┘  └──────┬───────┘           │
│                │                 │                 │                   │
│                └──── Topic: source.source.log ─────┘                   │
└────────────────────────────────────────────────────────────────────────┘
    ▲                                         │
    │                                         │ 
    │                                         ▼
┌────────────────┐                    ┌────────────────┐
│ Kafka UI       │                    │ Kafka Consumer │
│ For Monitoring │                    │                │
└────────────────┘                    └───────┬────────┘
                                              │
                                              ▼
                                      ┌──────────────────────┐
                                      │ Elasticsearch/Kibana │
                                      │                      │
                                      └──────────────────────┘
```

## CLI
### 1) 컨테이너 기동
docker compose up -d

### 2) Kafka/Connect 헬스 확인
docker compose ps
curl -s http://localhost:8083/connectors   # 빈 배열 []이면 정상 기동

### 3) (선택) Kibana 접속
open http://localhost:5601