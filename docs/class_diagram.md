# Class Diagram

This document contains class diagrams for the Arcane framework, split into three parts: Framework Architecture, Custom Implementation, and Watermarking.

---

## 1. Watermarking Core

The watermarking core defines the types and interfaces for tracking partition/stream progress.

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

## 2. Framework Architecture

The framework orchestrates streaming data ingestion using a hierarchy of providers and change-capture components.

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
    }

    DefaultStreamDataProvider --> DefaultSourceDataProvider : orchestrates
    DefaultSourceDataProvider --> StreamingSource : changeStream relies on

    style StreamingSource fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style DefaultStreamDataProvider fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style DefaultSourceDataProvider fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
```

---

## 3. Custom Implementation

Concrete implementations showing how to extend the framework for custom data sources and watermark schemas.

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
    }

    %% Relationships
    CustomStreamingDataProvider --> CustomSourceDataProvider : injects
    CustomSourceDataProvider --> CustomStreamingSource : queries

    %% Styling
    style CustomStreamingDataProvider fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    style CustomSourceDataProvider fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    style CustomStreamingSource fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
```
