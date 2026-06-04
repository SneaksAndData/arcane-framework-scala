# MemoryBoundShaper

`MemoryBoundShaper` is an implementation of a `ThroughputShaper` designed to: 
 - prevent Java Out-Of-Memory (OOM) crashes during data stream operations. It dynamically restricts stream chunk processing sizes based on the available memory and Garbage Collection (GC) activity of the Java Virtual Machine (JVM).
 - automatically configure stream processing rate based on information about target schema and sizing, and memory available on the host

---

## Core Features

* **OOM Prevention**: Dynamically drops chunk sizes during bursts if available JVM memory shrinks.
* **Row Size Estimation**: Calculates row size in bytes based on target schema.
* **Partition-Aware Throttling**: Forces small chunk sizes on partitioned tables to accelerate downstream file merge speeds, as this reduces number of unique partition keys per chunk.
* **GC-Aware Throughput Sizing**: Models memory consumption using a leaking bucket algorithm powered by Poisson probability distributions.

---

## Technical Architecture

### Memory Pool Allocation Strategy
The shaper dynamically scales the available memory buffer using a custom sigmoid function. Large target tables receive higher allocation limits to permit bigger processing chunks, while smaller tables remain throttled to preserve overhead stability.

### Java Object Cost Estimation Matrix
Memory sizing accounts for native Java object headers, data pointers, and 8-byte alignment constraints:


| Data Type | Byte Allocation Formula / Metadata |
| :--- | :--- |
| **INTEGER** / **TIME** / **FLOAT** | `4L` (Data) + `8L` (Pointer) + `16L` (Header) + `4L` (Padding) = **32 bytes** |
| **LONG** / **DOUBLE** / **TIMESTAMP** | `8L` (Data) + `8L` (Pointer) + `16L` (Header) + `4L` (Padding) = **36 bytes** |
| **BOOLEAN** | `1L` (Data) + `8L` (Pointer) + `16L` (Header) + `11L` (Padding) = **36 bytes** |
| **STRING** | `32L` (Wrapper) + `16L` (Array Header) + `(2L * Average Character Length)` |
| **DECIMAL** | Fixed size breakdown covering wrappers, Scale, Precision, and BigInteger arrays = **136 bytes** |
| **Fallback / Complex Types** | `28L` + Configured object type size fallback estimation |

**STRING** will use fallback value from `StreamContext` when running an initial backfill.

## Key Class Components

### Initialization Parameters

* `tablePropertyManager`: Fetches schema metadata, partition structures, and raw table dimensions.
* `targetTableShortName`: Identifier for the query target table.
* `throughputSettings`: Configuration schema enforcing the usage of the `MemoryBoundImpl` driver.
* `declaredMetrics`: Standard telemetry reporting endpoints capturing runtime chunk adjustments.

### Private Engine Functions

#### `getTotalFreeMemory`
Calculates unallocated memory plus unused memory inside the allocated pool.

#### `getTotalGCCount`
Queries `ManagementFactory.getGarbageCollectorMXBeans` to calculate the total lifetime garbage collection count across all available GC beans.

#### `getUptime`
Fetches the total JVM runtime session uptime converted cleanly into seconds.

#### `scaledSigmoid`
Projects values from `(-inf, inf)` onto a target range of `(0, maxBound)`. It uses a sensitivity scaling multiplier `k` and applies a midpoint shift since the calculated input density is always positive.

#### `estimateMemoryCutoff`
Returns a scaled sigmoid weight between `0.5` and `0.8` mapping target table sizes and row counts to a percentage of free memory that could be taken by chunks in a changeset.

#### `estimateStringLength`
Retrieves average string length using target table statistics. Multiplies the average length by a safety multiplier of `1.5`.

#### `estimateRowSize`
Iterates through all target table columns to construct total structural byte costs matching JVM platform footprints (Java 25).

#### `estimateChunkCost`
Derives a relative runtime cost metric for a specified batch size. It calculates raw cost proportions against the current free memory ceiling and scales the resulting coefficient inside a configured `chunkCostMax` boundary using `scaledSigmoid`.

---

## Overridden Abstract Methods

### `estimateChunkSize`
Returns a tuple task containing calculated `(Elements: Int, ElementSize: Long)`.

* Computes chunk size that guarantees at least **two structural data chunks** can coexist safely within the computed memory boundary.
* If table partitions exist, limits chunk size to a maximum of **half the partition count** to avoid updating too many partitions at once.

### `estimateShapeBurst`
Calculates the source row burst capacity allowed for incoming data spikes.

* Evaluates total maximum rows acceptable by remaining memory headroom.
* Selects the maximum value between the memory headroom row limit, `10%` of calculated chunk size, or the predefined configuration advice burst value (`throughputSettings.advisedBurst`).

### `estimateShapeRate`
Computes the sustained system `FlowRate` limit applied uniformly across 1-second operational intervals.

* Implements a **leaking bucket** mathematical model where Garbage Collection cycles act as probabilistic leaks draining memory back pressure.
* Estimates continuous collection likelihoods via a Poisson probability distribution calculated from current `gcFrequency` metrics and target configuration intervals.
* Dynamically scales safe baseline volume limits upwards when high GC reclamation probability is validated.

### `estimateChunkCost`
Calculates the operational cost boundary score by accepting an un-typed `Chunk[Element]` collection wrapper and routing its length directly into the primary internal sizing function.