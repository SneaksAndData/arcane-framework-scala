```mermaid
%%{init: {
  'theme': 'base',
  'themeVariables': {
    'primaryColor': '#fafafa',
    'primaryTextColor': '#2c3e50',
    'primaryBorderColor': '#7f8c8d',
    'lineColor': '#111111',
    'arrowheadColor': '#111111',
    'secondaryColor': '#fdfdfd',
    'tertiaryColor': '#ffffff',
    'fontSize': '14px',
    'fontFamily': 'Fira Code, Menlo, Monaco, Consolas, Courier New, monospace'
  },
  'flowchart': {
    'curve': 'linear'
  },
  'class': {
    'curve': 'linear'
  }
} }%%
classDiagram
    %% Direction of diagram flow
    direction TB

    %% ======================================================
    %% ROW 1: THE FRAMEWORK ARCHITECTURE
    %% ======================================================
    
    %% Column 1: Watermarking Core
    class Watermark {
        <<interface>>
        +timestamp: OffsetDateTime
        +age: Long
    }
    class SourceWatermark~VersionType~ {
        <<interface>>
        +version: VersionType
    }
    class JsonWatermark {
        <<interface>>
        +toJson: String
    }

    %% Column 2: Stream Orchestration
    class StreamDataProvider {
        <<interface>>
        +stream: ZStream[Any, Throwable, StructuredZStream]
    }
    class DefaultStreamDataProvider~WatermarkType~ {
        -rng: Random
        -getNextSleepDuration(): Duration
        +stream: ZStream[Any, Throwable, StructuredZStream]
    }

    %% Column 3: Data Emitters / Providers
    class ChangeCaptureDataProvider~WatermarkType~ {
        <<interface>>
        +hasChanges(previousVersion: WatermarkType): Task[Boolean]
        +getCurrentVersion(previousVersion: WatermarkType): Task[WatermarkType]
        +requestChanges(previous, next): ZStream[Any, Throwable, StructuredZStream]
        +currentWatermark: Task[WatermarkType]
    }
    class DefaultSourceDataProvider~WatermarkType~ {
        <<abstract>>
        #changeStream(previousVersion: WatermarkType): ZStream[Any, Throwable, StructuredZStream]*
        +requestChanges(previous, next): ZStream[Any, Throwable, StructuredZStream]
        +currentWatermark: Task[WatermarkType]
    }

    %% Column 4: Physical Streaming Sources
    class SchemaProvider~Schema~ {
        <<interface>>
        +getSchema: Task[SchemaType]
        +empty: SchemaType
    }
    class ShardProvider {
        <<interface>>
        +deleteShards(prefix: String): Task[Unit]
        +getShards(rangeStart: WatermarkType, rangeEnd: WatermarkType): ZStream[Any, Throwable, ShardMetadata]
    }
    class StreamingSource {
        <<interface>>
    }

    %% ======================================================
    %% ROW 2: YOUR CUSTOM SOURCE IMPLEMENTATION
    %% ======================================================
    
    %% Column 1 implementation
    class CustomWatermark {
        +version: String
        +timestamp: OffsetDateTime
        +toJson(): String
    }

    %% Column 2 implementation
    class CustomStreamingDataProvider {
        +layer: ZLayer
    }

    %% Column 3 implementation
    class CustomSourceDataProvider {
        #changeStream(previousVersion: CustomWatermark): ZStream[Any, Throwable, StructuredZStream]
        +hasChanges(previousVersion: CustomWatermark): Task[Boolean]
        +getCurrentVersion(previousVersion: CustomWatermark): Task[CustomWatermark]
    }

    %% Column 4 implementation
    class CustomStreamingSource {
        +getChanges(previousVersion: CustomWatermark): ZStream[Any, Throwable, StructuredZStream]
        +getCurrentVersion: Task[CustomWatermark]
    }

    %% ======================================================
    %% STYLING DEFINITIONS
    %% ======================================================
    style StreamDataProvider fill:#e0f2f1,stroke:#004d40,stroke-width:2px
    style SchemaProvider fill:#e0f2f1,stroke:#004d40,stroke-width:2px
    style ShardProvider fill:#e0f2f1,stroke:#004d40,stroke-width:2px
    style StreamingSource fill:#e0f2f1,stroke:#004d40,stroke-width:2px

    style ChangeCaptureDataProvider fill:#e3f2fd,stroke:#0d47a1,stroke-width:2px
    style DefaultStreamDataProvider fill:#e3f2fd,stroke:#0d47a1,stroke-width:2px
    style DefaultSourceDataProvider fill:#e3f2fd,stroke:#0d47a1,stroke-width:2px

    style Watermark fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style SourceWatermark fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style JsonWatermark fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style CustomWatermark fill:#fff3e0,stroke:#e65100,stroke-width:2px

    style CustomStreamingDataProvider fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    style CustomSourceDataProvider fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    style CustomStreamingSource fill:#f3e5f5,stroke:#4a148c,stroke-width:2px

    %% ======================================================
    %% RELATIONSHIPS & INHERITANCE
    %% ======================================================
    
    %% Base Framework Relationships
    Watermark <|-- SourceWatermark : extends
    SourceWatermark <|-- WatermarkType : bound
    JsonWatermark <|-- WatermarkType : bound

    StreamDataProvider <|.. DefaultStreamDataProvider : implements
    DefaultStreamDataProvider --> ChangeCaptureDataProvider : orchestrates

    ChangeCaptureDataProvider <|.. DefaultSourceDataProvider : implements

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