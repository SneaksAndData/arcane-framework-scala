# Streaming Componetns Diagram for Developers

This document shows relations between components that are required by streaming graph builders. Developers must implement those when building a custom source for a new plugin.

---

## 1. Watermarking

Concrete implementation of a watermark is used to track stream progress in relation to source.

```mermaid
%%{init: {
  "theme": "base",
  "themeVariables": {
    "primaryColor": "#fafafa",
    "primaryTextColor": "#2c3e50",
    "primaryBorderColor": "#7f8c8d",
    "lineColor": "#111111",
    "arrowheadColor": "#111111",
    "secondaryColor": "#fdfdfd",
    "tertiaryColor": "#ffffff",
    "fontSize": "14px",
    "fontFamily": "Fira Code, Menlo, Monaco, Consolas, Courier New, monospace"
  },
  "class": {
    "curve": "linear"
  }
} }%%
classDiagram
    direction TB

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
    class WatermarkType {
        <<parameter bound>>
    }

    Watermark <|-- SourceWatermark : extends
    SourceWatermark <|-- WatermarkType : bound
    JsonWatermark <|-- WatermarkType : bound

    style Watermark fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style SourceWatermark fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style JsonWatermark fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style WatermarkType fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
```

---

## 2. Change Capture Components

Classes that orchestrate change capture pipeline: extracting changes from source and incorporating them into a Iceberg table.

```mermaid
%%{init: {
  "theme": "base",
  "themeVariables": {
    "primaryColor": "#fafafa",
    "primaryTextColor": "#2c3e50",
    "primaryBorderColor": "#7f8c8d",
    "lineColor": "#111111",
    "arrowheadColor": "#111111",
    "secondaryColor": "#fdfdfd",
    "tertiaryColor": "#ffffff",
    "fontSize": "14px",
    "fontFamily": "Fira Code, Menlo, Monaco, Consolas, Courier New, monospace"
  },
  "class": {
    "curve": "linear"
  }
} }%%
classDiagram
    direction TB

    class DefaultStreamDataProvider~WatermarkType~ {
        +stream: ZStream[Any, Throwable, StructuredZStream]
    }
    class DefaultSourceDataProvider~WatermarkType~ {
        <<abstract>>
        #changeStream(previousVersion: WatermarkType) ZStream[Any, Throwable, StructuredZStream]*
        +requestChanges(previous, next) ZStream[Any, Throwable, StructuredZStream]
        +currentWatermark: Task[WatermarkType]
    }
    class StreamingSource {
        <<interface>>
        +getSchema: Task[SchemaType]
        +empty: SchemaType
        +deleteShards(prefix: String) Task[Unit]
        +getShards(rangeStart: WatermarkType, rangeEnd: WatermarkType) ZStream[Any, Throwable, ShardMetadata]
        +getChanges(startFrom: WatermarkType) ZStream[Any, Throwable, StructuredZStream]
        +getCurrentVersion: Task[WatermarkType]
    }

    DefaultStreamDataProvider --> DefaultSourceDataProvider : orchestrates
    DefaultSourceDataProvider --> StreamingSource : changeStream relies on

    style StreamingSource fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style DefaultStreamDataProvider fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style DefaultSourceDataProvider fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
```

---

## 3. Custom Components Implementation

Concrete implementations of the components above, when provided, will automatically allow continuous data streaming from a custom source.

```mermaid
%%{init: {
  "theme": "base",
  "themeVariables": {
    "primaryColor": "#fafafa",
    "primaryTextColor": "#2c3e50",
    "primaryBorderColor": "#7f8c8d",
    "lineColor": "#111111",
    "arrowheadColor": "#111111",
    "secondaryColor": "#fdfdfd",
    "tertiaryColor": "#ffffff",
    "fontSize": "14px",
    "fontFamily": "Fira Code, Menlo, Monaco, Consolas, Courier New, monospace"
  },
  "class": {
    "curve": "linear"
  }
} }%%
classDiagram
    direction TB

    %% Custom Implementations
    class CustomStreamingDataProvider {
        <<extends DefaultStreamDataProvider>>
        +layer: ZLayer
    }
    class CustomSourceDataProvider {
        <<extends DefaultSourceDataProvider>>
        #changeStream(previousVersion: CustomWatermark) ZStream[Any, Throwable, StructuredZStream]
        +hasChanges(previousVersion: CustomWatermark) Task[Boolean]
        +getCurrentVersion(previousVersion: CustomWatermark) Task[CustomWatermark]
    }
    class CustomStreamingSource {
        <<implements StreamingSource>>
        +getChanges(previousVersion: CustomWatermark) ZStream[Any, Throwable, StructuredZStream]
        +getCurrentVersion: Task[CustomWatermark]
        +deleteShards(prefix: String) Task[Unit]
        +getShards(rangeStart: CustomWatermark, rangeEnd: CustomWatermark) ZStream[Any, Throwable, ShardMetadata]
    }

    %% Relationships
    CustomStreamingDataProvider --> CustomSourceDataProvider : injects
    CustomSourceDataProvider --> CustomStreamingSource : requires

    %% Styling
    style CustomStreamingDataProvider fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style CustomSourceDataProvider fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style CustomStreamingSource fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
```
