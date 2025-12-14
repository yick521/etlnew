# ZG-ETL: 基于 Apache Flink 的实时数据处理系统

ZG-ETL 是一个基于 Apache Flink 构建的可扩展、高性能的数据处理系统，专门用于处理实时数据管道。该系统采用模块化设计，分为通用组件库（etl-flink-common）和业务流水线（etl-flink-pipeline）两大部分。

## 项目架构

### 模块划分

#### etl-flink-common 模块

这是整个系统的通用组件库，提供各种基础功能和服务：

#### etl-flink-pipeline 模块

这是实际的数据处理流水线，包含具体的业务逻辑实现

## 启动命令

根据不同类型的作业，可以使用以下命令启动相应的数据处理流程：

### 启动 Gate 作业
```bash
# 启动 Gate 流水线作业
flink run -c com.zhugeio.etl.pipeline.entry.GateJobEntry etl-flink-pipeline-1.0-SNAPSHOT.jar
```

### 启动 ID 作业
```bash
# 启动 ID 流水线作业
flink run -c com.zhugeio.etl.pipeline.entry.IdJobEntry etl-flink-pipeline-1.0-SNAPSHOT.jar
```

### 启动 DW 作业
```bash
# 启动 DW 流水线作业
flink run -c com.zhugeio.etl.pipeline.entry.DwJobEntry etl-flink-pipeline-1.0-SNAPSHOT.jar
```

### 启动统一作业
```bash
# 启动统一流水线作业
flink run -c com.zhugeio.etl.pipeline.entry.UnifiedJobEntry etl-flink-pipeline-1.0-SNAPSHOT.jar
```

### 使用参数运行作业
```bash
# 带自定义参数启动作业
flink run -c com.zhugeio.etl.pipeline.entry.{JobEntryClass} \
  -p {parallelism} \
  etl-flink-pipeline-1.0-SNAPSHOT.jar \
  --config-file /path/to/config.properties
```

## 开发指南


## 依赖管理

本项目使用 Maven 进行依赖管理，主要依赖包括：
- Apache Flink 核心组件
- Kafka 客户端
- Redis/Kvrocks 客户端
- HTTP 客户端
- Doris/Kudu 连接器
- JSON 处理库
- 加密相关库
- 其他辅助工具库

## 配置管理

系统通过 properties 文件进行配置管理：
- Gate 作业配置：`gate.properties`
- ID 作业配置：`id.properties`
- DW 作业配置：`dw.properties`
- 统一作业配置：`pipeline.properties`

配置项也可以通过命令行参数传递给作业。