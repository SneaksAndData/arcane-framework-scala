```mermaid
classDiagram
    %% Direction of diagram flow
    direction TB

    %% ======================================================
    %% 1. CORE INTERFACES & TRAITS (Teal)
    %% ======================================================
    class StreamDataProvider {
        <<interface>>
        +stream: ZStream[Any, Throwable, StructuredZStream]
    }
    style StreamDataProvider fill:#e0f2f1,stroke:#00897b,stroke-width:2px

    class SchemaProvider~Schema~ {
        <<interface>>
        +getSchema: Task[SchemaType]
        +empty: SchemaType
    }
    style SchemaProvider fill:#e0f2f1,stroke:#00897b,stroke-width:2px

    class ShardProvider {
        <<interface>>
        +deleteShards(prefix: String): Task[Unit]
        +getShards(rangeStart: WatermarkType, rangeEnd: WatermarkType): ZStream[Any, Throwable, ShardMetadata]
    }
    style ShardProvider fill:#e0f2f1,stroke:#00897b,stroke-width:2px

    class StreamingSource {
        <<interface>>
    }
    style StreamingSource fill:#e0f2f1,stroke:#00897b,stroke-width:2px


    %% ======================================================
    %% 2. ORCHESTRATION & EMITTERS (Blue)
    %% ======================================================
    class ChangeCaptureDataProvider~WatermarkType~ {
        <<interface>>
        +hasChanges(previousVersion: WatermarkType): Task[Boolean]
        +getCurrentVersion(previousVersion: WatermarkType): Task[WatermarkType]
        +requestChanges(previous, next): ZStream[Any, Throwable, StructuredZStream]
        +currentWatermark: Task[WatermarkType]
    }
    style ChangeCaptureDataProvider fill:#e3f2fd,stroke:#1e88e5,stroke-width:2px

    class DefaultStreamDataProvider~WatermarkType~ {
        -rng: Random
        -getNextSleepDuration(): Duration
        +stream: ZStream[Any, Throwable, StructuredZStream]
    }
    style DefaultStreamDataProvider fill:#e3f2fd,stroke:#1e88e5,stroke-width:2px

    class DefaultSourceDataProvider~WatermarkType~ {
        <<abstract>>
        #changeStream(previousVersion: WatermarkType): ZStream[Any, Throwable, StructuredZStream]*
        +requestChanges(previous, next): ZStream[Any, Throwable, StructuredZStream]
        +currentWatermark: Task[WatermarkType]
    }
    style DefaultSourceDataProvider fill:#e3f2fd,stroke:#1e88e5,stroke-width:2px


    %% ======================================================
    %% 3. WATERMARKING (Green)
    %% ======================================================
    class Watermark {
        <<interface>>
        +timestamp: OffsetDateTime
        +age: Long
    }
    style Watermark fill:#e8f5e9,stroke:#43a047,stroke-width:2px

    class SourceWatermark~VersionType~ {
        <<interface>>
        +version: VersionType
    }
    style SourceWatermark fill:#e8f5e9,stroke:#43a047,stroke-width:2px

    class JsonWatermark {
        <<interface>>
        +toJson: String
    }
    style JsonWatermark fill:#e8f5e9,stroke:#43a047,stroke-width:2px


    %% ======================================================
    %% 4. CUSTOM IMPLEMENTATION (Orange)
    %% ======================================================
    class CustomWatermark {
        +version: String
        +timestamp: OffsetDateTime
        +toJson(): String
    }
    style CustomWatermark fill:#fff3e0,stroke:#fb8c00,stroke-width:2px

    class CustomStreamingDataProvider {
        +layer: ZLayer
    }
    style CustomStreamingDataProvider fill:#fff3e0,stroke:#fb8c00,stroke-width:2px

    class CustomSourceDataProvider {
        #changeStream(previousVersion: CustomWatermark): ZStream[Any, Throwable, StructuredZStream]
        +hasChanges(previousVersion: CustomWatermark): Task[Boolean]
        +getCurrentVersion(previousVersion: CustomWatermark): Task[CustomWatermark]
    }
    style CustomSourceDataProvider fill:#fff3e0,stroke:#fb8c00,stroke-width:2px

    class CustomStreamingSource {
        +getChanges(previousVersion: CustomWatermark): ZStream[Any, Throwable, StructuredZStream]
        +getCurrentVersion: Task[CustomWatermark]
    }
    style CustomStreamingSource fill:#fff3e0,stroke:#fb8c00,stroke-width:2px


    %% ======================================================
    %% RELATIONSHIPS & INHERITANCE
    %% ======================================================
    
    %% Base Framework Relationships
    StreamDataProvider <|.. DefaultStreamDataProvider : implements
    ChangeCaptureDataProvider <|.. DefaultSourceDataProvider : implements
    DefaultStreamDataProvider --> ChangeCaptureDataProvider : orchestrates

    Watermark <|-- SourceWatermark : extends
    SourceWatermark <|-- WatermarkType : bound
    JsonWatermark <|-- WatermarkType : bound

    SchemaProvider <|-- StreamingSource : extends
    ShardProvider <|-- StreamingSource : extends

    %% Concrete Custom Implementations
    SourceWatermark <|.. CustomWatermark : implements
    JsonWatermark <|.. CustomWatermark : implements

    DefaultStreamDataProvider <|-- CustomStreamingDataProvider : extends
    DefaultSourceDataProvider <|-- CustomSourceDataProvider : extends
    StreamingSource <|.. CustomStreamingSource : implements

    CustomStreamingDataProvider --> CustomSourceDataProvider : injects
    CustomSourceDataProvider --> CustomStreamingSource : queries
```