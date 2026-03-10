# Milvus CDC 模块各版本更新进展调研报告

> 调研时间：2026-03-10  
> 仓库：milvus-io/milvus  
> 覆盖版本：v2.4.x ~ v2.6.x（截至当前 master）

---

## 1. CDC 模块概述

Milvus 的 CDC（Change Data Capture）模块经历了两个截然不同的阶段：

| 阶段 | 时间段 | 形态 |
|------|--------|------|
| **外挂式 CDC（Legacy）** | v2.3.x ~ v2.5.x | 独立仓库 [milvus-io/milvus-cdc](https://github.com/milvus-io/milvus-cdc)，通过订阅消息队列（Pulsar/Kafka）捕获变更，在 Milvus 主仓库中辅以少量适配代码（ReplicateMsg、ReplicateID 等） |
| **内置式 CDC（新架构）** | v2.6.0 起 | 内嵌于 Milvus 主仓库 `internal/cdc/`，基于 Streaming Node / WAL 的原生跨集群复制，无需外部消息队列 |

---

## 2. 旧架构 CDC（v2.4.x ~ v2.5.x）

### 2.1 架构简述（Legacy）

旧架构依赖外部工具 **milvus-io/milvus-cdc**，其工作原理为：

1. CDC 工具连接源集群的消息队列（Pulsar/Kafka）
2. 捕获 DML/DDL 变更消息（通过 `ReplicateMsg` 类型标识）
3. 将消息写入目标集群的 Proxy API

Milvus 主仓库为此提供以下配合机制：
- `ReplicateMsg` 消息类型及 `ReplicateID` 字段
- `TTMsgEnabled`、`CollectionReplicateEnable` 等特性开关
- `ReplicateMsgChannel` 专用 channel
- Dispatcher/Pipeline 中的 replicate-aware 分支逻辑

### 2.2 v2.5.x 版本 CDC 更新记录

#### v2.5.9（2025-04-11）— Bug Fix

| PR | 变更内容 |
|----|----------|
| [#41189](https://github.com/milvus-io/milvus/pull/41189) | 修复无法获取 replicate channel positions 的问题 |

#### v2.5.12（2025-05-19）— Feature Enhancement

| PR | 变更内容 |
|----|----------|
| [#41594](https://github.com/milvus-io/milvus/pull/41594) | CDC 支持同步更多 DDL API（CreatePartition、DropPartition、AlterCollection 等） |
| [#41679](https://github.com/milvus-io/milvus/pull/41679) | 继续扩展 DDL API 的 CDC 同步支持（cherry-pick 到 2.5 分支） |

**说明**：v2.5.12 是旧架构 CDC 中功能增强最为显著的版本，使外挂 CDC 工具能够同步更完整的 schema 变更操作。

#### v2.5.13 ~ v2.5.27（2025-06 至 2026-02）

在此期间 v2.5.x 维护分支没有针对 CDC 模块的专项变更（重心已转向 v2.6.x 的新架构）。

---

## 3. 新架构 CDC（v2.6.x）

### 3.1 架构简述（新架构）

v2.6.0 引入了全新的内置 CDC 模块，与 Streaming Node 深度集成。核心特点：

- **星型拓扑（Star Topology）**：一个 PRIMARY 集群负责所有写入，N 个 SECONDARY 集群作为副本接收 WAL 消息
- **基于 WAL 的复制**：直接读取 Primary WAL，无需依赖 Pulsar/Kafka 等外部消息队列
- **逐 PChannel 复制**：`ChannelReplicator` 按物理 channel 维度进行并发复制
- **检查点机制**：每个 PChannel 维护 `{ClusterID, PChannel, MessageID, TimeTick}` 检查点，支持断点续传
- **事务一致性**：事务消息仅在 CommitTxn 时推进检查点，保证原子性
- **RBAC 集成**：UpdateReplicateConfiguration 操作受权限控制

**代码目录结构**（`internal/cdc/`）：

```
internal/cdc/
├── server.go                          # CDCServer 核心入口
├── controller/controller.go           # 监听 etcd 配置变更，动态创建/销毁 Replicator
├── replication/
│   ├── replicate_manager_client.go    # ReplicateManagerClient 接口
│   └── replicatemanager/
│       ├── replicate_manager.go       # 管理所有 ChannelReplicator 的生命周期
│       ├── channel_replicator.go      # 单 PChannel 复制循环
│       └── replicatestream/
│           ├── replicate_stream_client.go       # 与 Secondary 的 gRPC 流客户端接口
│           ├── replicate_stream_client_impl.go  # 实现（含缓存、重试、指标）
│           └── msg_queue.go                     # 消息队列管理
├── cluster/milvus_client.go           # 目标集群 gRPC 客户端（含 TLS 支持）
├── meta/replicate_meta.go             # ReplicateChannel 元数据结构
└── resource/resource.go               # 依赖注入容器
```

同时在 `internal/streamingnode/` 侧有对应的 Interceptor：

```
internal/streamingnode/server/wal/interceptors/replicate/
├── replicate_interceptor.go           # 校验 replicate header，拒绝非法消息
└── replicates/
    ├── manager.go                     # 管理 Secondary 状态
    ├── secondary_state.go             # Secondary 角色下的状态机（检查点维护）
    └── txn.go                         # 事务 buffer（未提交事务的恢复支持）
```

Prometheus 监控指标（`pkg/metrics/cdc_metrics.go`）：

| 指标名 | 类型 | 描述 |
|--------|------|------|
| `replicated_messages_total` | Counter | 成功转发的消息总数（按 source/target channel 和 msg type 分类） |
| `replicated_bytes_total` | Counter | 成功转发的字节总数 |
| `replicate_end_to_end_latency` | Histogram | 端到端延迟（ms），从读取 Primary WAL 到 Secondary WAL 确认写入 |
| `last_replicated_time_tick` | Gauge | 最近一次复制消息的物理时间（秒），用于计算复制延迟 |
| `stream_rpc_connections` | Gauge | 到目标集群的 gRPC 流连接状态（connected/disconnected） |
| `stream_rpc_reconnect_times` | Counter | 到目标集群的 gRPC 流重连次数 |

### 3.2 v2.6.0-rc1（2025-06-18）— 架构奠基

v2.6.0-rc1 引入了 Streaming Node（GA）、Woodpecker WAL 等重大架构变更，为内置 CDC 奠定基础，但 **本版本未包含 CDC 功能本身**。

### 3.3 v2.6.0（2025-08-05）— 正式发布（不含 CDC）

v2.6.0 正式发布。Release notes 中无 CDC 功能描述，此时 CDC 代码仍在 PR 评审中。

> 注：CDC 核心 PR [#44124](https://github.com/milvus-io/milvus/pull/44124) 于 2025-09-16 合并，晚于 v2.6.0 发布。

### 3.4 v2.6.1（2025-09-01）— 第一个含 CDC 基础的版本

| 变更 | 描述 |
|------|------|
| Streaming Node 稳定性 | WAL-based replication 框架就绪 |

> CDC 代码在 v2.6.1 发布后不久合并，但未被包含在该版本。

### 3.5 v2.6.2（2025-09-19）— CDC 初步稳定

| PR | 变更内容 |
|----|----------|
| [#44124](https://github.com/milvus-io/milvus/pull/44124) | **[feat] 新增 CDC 支持**：实现完整的 CDC service，支持 WAL-based 跨集群日志复制（该 PR 合并于 2025-09-16，位于 v2.6.2 发布之前） |
| [#44456](https://github.com/milvus-io/milvus/pull/44456) | 在 WAL 中支持 replicate 消息；支持 CDC replicate 从 WAL 恢复（2025-09-22 合并） |

### 3.6 v2.6.3（2025-10-10）— Bug Fix

| PR | 变更内容 |
|----|----------|
| [#44531](https://github.com/milvus-io/milvus/pull/44531) | 修复 replicator 在 gRPC 出错时无法停止的问题；增强 replicate config 校验器（cluster 属性不可变、URI 唯一性检查） |

### 3.7 v2.6.4（2025-10-21）— 功能增强

| PR | 变更内容 |
|----|----------|
| [#44564](https://github.com/milvus-io/milvus/pull/44564) | **CDC 调度加速**：改为 watch etcd replicate pchannel meta，替代定时轮询 |
| [#44560](https://github.com/milvus-io/milvus/pull/44560) | **UpdateReplicateConfig 重构**：基于 WAL-broadcast DDL/DCL 框架，使用 AlterReplicateConfig 广播消息更新拓扑配置；BeginTxn 消息改用 commit 的 timetick（避免 CDC 场景下的 timetick 回滚） |
| [#44642](https://github.com/milvus-io/milvus/pull/44642) | **支持从拓扑中移除集群**：更新配置时可移除 replication topology 中的 secondary 集群；修复部分 CDC metric 错误 |

### 3.8 v2.6.5（2025-11-09）— 代码整合

| PR | 变更内容 |
|----|----------|
| [#44898](https://github.com/milvus-io/milvus/pull/44898) | 修复 primary-secondary replication switch 阻塞问题（该 PR 合并于 2025-10-22，包含在 v2.6.4 后的版本中） |

### 3.9 v2.6.6（2025-11-21）— 多项重要改进

| PR | 变更内容 |
|----|----------|
| [#45025](https://github.com/milvus-io/milvus/pull/45025) | Cherry-pick: 新 DDL 框架与 CDC 联动补丁（包含 ChannelReplicator 更优雅的关闭逻辑） |
| [#45217](https://github.com/milvus-io/milvus/pull/45217) | **默认不启动 CDC**：CDC 服务默认禁用，需显式配置方可启动 |
| [#45236](https://github.com/milvus-io/milvus/pull/45236) | **RBAC 支持**：为 UpdateReplicateConfiguration 操作添加权限控制 |
| [#45241](https://github.com/milvus-io/milvus/pull/45241) | Cherry-pick: 新 DDL 框架与 CDC 联动补丁 2（更多稳定性修复） |
| [#45260](https://github.com/milvus-io/milvus/pull/45260) | 修复 CDC 服务停止时等待 replicate stream client 完成的问题 |
| [#45280](https://github.com/milvus-io/milvus/pull/45280) | Cherry-pick: 新 DDL 框架与 CDC 联动补丁 3 |
| [#45095](https://github.com/milvus-io/milvus/pull/45095) | 修复 CDC 优雅停止时的 panic |
| [#45347](https://github.com/milvus-io/milvus/pull/45347) | 修复 replicate stream client 中的 data race |

### 3.10 v2.6.7（2025-12-04）— 无 CDC 变更

本版本无 CDC 相关更新。

### 3.11 v2.6.8（2026-01-04）— 监控修复

| PR | 变更内容 |
|----|----------|
| [#46122](https://github.com/milvus-io/milvus/pull/46122) | **修复 replicate lag 指标计算**：通过时间戳差值（WAL confirmed time - Last replicate time）计算 lag，避免 health check 误报 |

> 同期 master 分支还包含以下改进（最终会包含在后续版本中）：
> - [#46120](https://github.com/milvus-io/milvus/pull/46120) 同上（master 分支版本）
> - [#46369](https://github.com/milvus-io/milvus/pull/46369) 支持延迟 scanner 启动（WAL 写入恢复期间支持 fence 消息持久化）
> - [#46469](https://github.com/milvus-io/milvus/pull/46469) / [#46492](https://github.com/milvus-io/milvus/pull/46492) 对齐 `last_replicated_time_tick` 与 `wal_last_confirm_time_tick` 指标
> - [#46574](https://github.com/milvus-io/milvus/pull/46574) / [#46612](https://github.com/milvus-io/milvus/pull/46612) 修复服务器空闲时的 replicate lag 问题
> - [#46603](https://github.com/milvus-io/milvus/pull/46603) **移除 Legacy CDC/Replication 代码**：清理所有旧架构遗留的 ReplicateMsg 类型、ReplicateID 常量、特性开关等

### 3.12 v2.6.9 ~ v2.6.11（2026-01 ~ 2026-02）— 持续稳定

这三个版本的 release notes 中无显式 CDC 条目，但以下工作已合并到 master：

| PR | 变更内容 |
|----|----------|
| [#47780](https://github.com/milvus-io/milvus/pull/47780) / [#47914](https://github.com/milvus-io/milvus/pull/47914) | **Secondary 集群独立副本数配置**：引入 `use_local_replica_config` 标志，允许 secondary 集群使用自身的副本数配置，而不是照搬 primary（合并于 2026-02-27 / 2026-03-02） |

### 3.13 Master 分支（尚未发布）

以下改进已合并或处于审核中：

| PR | 状态 | 变更内容 |
|----|------|----------|
| [#47933](https://github.com/milvus-io/milvus/pull/47933) | 待审 | 全局 TLS 配置支持 CDC 出站 mTLS 连接（`tls.clientPemPath`/`tls.clientKeyPath` 参数） |
| [#47819](https://github.com/milvus-io/milvus/pull/47819) | 待审（2.6 分支） | 允许 ReplicateConfiguration 中增加 PChannel 数量 |

---

## 4. 关键特性里程碑汇总

| 版本 | 日期 | CDC 关键里程碑 |
|------|------|----------------|
| v2.5.9 | 2025-04-11 | 修复 replicate channel position 获取失败（旧架构） |
| v2.5.12 | 2025-05-19 | 旧架构 CDC 支持更多 DDL API（CreatePartition、AlterCollection 等） |
| v2.6.0-rc1 | 2025-06-18 | Streaming Node GA，为新架构 CDC 奠基 |
| v2.6.0 | 2025-08-05 | Streaming Node 正式发布，WAL 基础设施就绪 |
| **v2.6.2** | **2025-09-19** | **✅ 新架构 CDC 首次引入**（PR #44124 合并于 2025-09-16）；支持 WAL replicate 消息与恢复 |
| v2.6.3 | 2025-10-10 | 修复 replicator 停止 bug；增强 replicate config 校验 |
| v2.6.4 | 2025-10-21 | CDC 调度加速（etcd watch）；UpdateReplicateConfig 重构；支持移除集群 |
| v2.6.6 | 2025-11-21 | RBAC 权限控制；默认禁用 CDC；修复 data race 和 panic；DDL 框架集成 |
| v2.6.8 | 2026-01-04 | 修复 replicate lag 指标误报 |
| master | 2026-03 | Secondary 集群独立副本数；全局 TLS；PChannel 数量扩展（开发中） |

---

## 5. 架构演进路线总结

```
v2.3 ~ v2.5.x   外挂 CDC 工具（milvus-cdc 仓库）
                  └── 通过 Pulsar/Kafka 订阅 ReplicateMsg
                  └── v2.5.9: 修复 position 获取
                  └── v2.5.12: 支持更多 DDL 同步

v2.6.0-rc1       Streaming Node GA（WAL 架构基础）

v2.6.0           新 WAL 架构正式发布（Woodpecker 替代 Pulsar/Kafka）

v2.6.2           ✅ 内置 CDC 首次引入
                  └── 基于 WAL 的跨集群复制（star topology）
                  └── controller + replicatemanager + replicatestream
                  └── Prometheus 监控指标

v2.6.3           Bug Fix: replicator 停止逻辑

v2.6.4           Enhancement: 性能优化 + 拓扑管理
                  └── etcd watch 替代轮询
                  └── WAL-broadcast 配置更新
                  └── 支持动态移除集群

v2.6.6           Enhancement: 安全性 + 稳定性
                  └── RBAC 权限控制
                  └── 默认禁用 CDC
                  └── 修复多个并发 bug

v2.6.8           Monitoring: 修复 lag 指标计算

master (v2.6.12+) 独立副本数 + TLS + PChannel 扩展（进行中）
                  └── 移除全部旧架构 CDC 代码（PR #46603）
```

---

## 6. 与 milvus-io/milvus-cdc 外部工具的关系

- [milvus-io/milvus-cdc](https://github.com/milvus-io/milvus-cdc) 是适用于 Milvus **v2.3.x ~ v2.5.x** 的独立 CDC 工具
- Milvus **v2.6.x** 起，CDC 功能已内置于主仓库，外部工具不再适用于新架构
- 旧架构的 `ReplicateMsg`、`ReplicateID` 等代码已于 2025-12-30 通过 [PR #46603](https://github.com/milvus-io/milvus/pull/46603) 完全移除

---

## 7. 当前 CDC 模块能力（v2.6.11 / master）

| 能力 | 状态 |
|------|------|
| 跨集群 WAL 复制（DML：Insert/Delete/Upsert） | ✅ 支持 |
| 跨集群 DDL 复制（CreateCollection/DropCollection 等） | ✅ 支持 |
| AlterCollection / AlterPartition 等 Schema 变更复制 | ✅ 支持 |
| DCL 复制（RBAC 操作） | ✅ 支持 |
| 断点续传（Checkpoint 恢复） | ✅ 支持 |
| 事务一致性（CommitTxn 推进检查点） | ✅ 支持 |
| 动态添加/移除 secondary 集群 | ✅ 支持 |
| Primary/Secondary 角色切换（SwitchOver/FailOver） | ✅ 支持 |
| RBAC 权限控制（UpdateReplicateConfiguration） | ✅ 支持（v2.6.6+） |
| Prometheus 监控（延迟、吞吐、连接状态） | ✅ 支持 |
| Secondary 集群独立副本数配置 | ✅ 支持（master，v2.6.12+） |
| 全局 mTLS（TLS 出站连接） | 🔄 开发中（PR #47933） |
| PChannel 数量在线扩展 | 🔄 开发中（PR #47819） |
| 默认启用 CDC | ❌ 默认禁用，需显式配置 |
