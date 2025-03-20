# 数据库校验工具 (DB-Checker)

这个工具用于比对多个数据库中表的金额字段SUM值，支持多数据源配置和各种运行模式。

## 功能特点

- 支持多数据源配置
- 支持断点续跑
- 支持指定表和schema的过滤
- 支持自定义SQL提示和WHERE条件
- 支持多种数据比对公式
- 结果导出为CSV格式

## 项目流程图

```mermaid
graph TD
    A[应用启动] --> B[加载配置]
    B --> C[初始化数据库服务]
    C --> D{检查运行模式}
    
    D -->|新运行| E[清空表信息]
    D -->|断点续跑| F[恢复表信息]
    
    E --> G[获取要处理的数据库]
    F --> G
    
    G --> H[并发获取表信息]
    H --> I[处理所有数据库表信息]
    I --> J[保存断点状态]
    J --> K[创建导出目录]
    K --> L[准备输出数据]
    L --> M[执行公式计算]
    M --> N[导出CSV结果]
    N --> O[结束运行]
    
    subgraph 表信息获取流程
    H1[获取表元数据] --> H2[获取金额字段]
    H2 --> H3[应用SQL提示]
    H3 --> H4[应用WHERE条件]
    H4 --> H5[执行COUNT和SUM查询]
    H5 --> H6[返回表信息]
    end
    
    subgraph 公式计算流程
    M1[读取表金额数据] --> M2[应用公式规则]
    M2 --> M3[判断公式结果]
    M3 --> M4[生成比对结果]
    end
```

## 项目结构

- `DbCheckerApplication.java`: 应用程序入口
- `service/`: 各种服务实现
    - `DatabaseService.java`: 数据库操作核心服务
    - `TableMetadataService.java`: 表元数据服务
    - `FormulaCalculationService.java`: 公式计算服务
- `config/`: 配置相关
    - `DbConfig.java`: 数据库配置
    - `DatabaseInitScriptsProperties.java`: 初始化脚本配置
- `manager/`: 管理器
    - `TableInfoManager.java`: 表信息管理
    - `ResumeStateManager.java`: 断点续跑状态管理
- `model/`: 数据模型
- `util/`: 实用工具
    - `CsvExportUtil.java`: CSV导出工具

## 使用方法

1. 配置`application.properties`中的数据源
2. 配置需要比对的表和规则
3. 运行应用执行比对
4. 查看导出的CSV结果文件 