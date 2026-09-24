# 人形机器人试验统计准入服务

本仓库是一套已经实现并可直接运行的服务端项目，用于把人形机器人在递送、讲解和灵巧操作等场景中的结构化试验记录转成可复核的统计准入结论。系统以 SQLite 保存机器人、软件构建、不可变协议版本、试验批次、原始观测、排除申请、分析快照、准入决定和审计事件，不依赖机器人设备、图片、音频、视频或外部基础设施。

现有代码按职责分为：

- `api.py`：标准库实现的 HTTP JSON 接口与无网络路由测试边界；
- `service.py`：角色权限、批次状态机、幂等导入、排除复核、任务租约、审批和报告；
- `analysis.py`：分层覆盖、Wilson 区间、描述性统计、确定性 bootstrap 和准入规则；
- `contracts.py`：协议、指标、分层、权重、随机种子和单次观测的数据契约；
- `jsonio.py`：严格 JSON/JSONL 读取、规范化序列化与内容摘要；
- `numeric.py`：不依赖第三方库的描述性统计和 Wilson 区间；
- `storage.py`：完整 SQLite 业务模式、约束、索引和事务辅助；
- `clock.py`：生产时钟与可确定性推进的测试时钟；
- `acceptance.py`：贯通建档、导入、封存、分析、审批和报告的离线验收。

系统已经实现以下主流程：协议发布后不可原地覆盖；批次按版本从草稿进入运行、封存、分析和决定状态；观测分片同时受请求幂等键和来源行唯一身份保护；排除请求必须由不同角色复核；分析任务使用 SQLite 租约避免重复执行并支持过期接管；同一输入快照使用固定算法版本和随机种子得到一致结果；分析者与审批人职责分离，报告保留输入摘要、统计规则和批次审计链。

## 环境

- Linux
- Python 3.11 或更高版本
- 无需安装第三方 Python 包

如需安装到隔离环境，可在依赖已经准备好的容器中执行：

```bash
python3 -m pip install --no-index --no-deps .
```

## 测试

在 `project/` 目录执行：

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

测试使用临时目录和内存数据库，不依赖外部服务。`tests/test_concurrency.py` 会在本机回环地址上真正起停 HTTP 服务，通过真实 socket 并发执行读写，验证每请求独立连接、事务隔离、关闭排空与命令行启停。

## 构建检查

本项目是纯 Python 源码包，构建检查采用字节码编译：

```bash
python3 -m compileall -q src tests
```

## 无浏览器验收

下面的命令会读取 `fixtures/` 中的协议与观测记录，在临时 SQLite 数据库中完成用户与设备建档、协议发布、批次启动、观测导入、批次封存、任务领取、统计分析、准入审批和审计报告导出，随后输出一行 JSON 结果：

```bash
PYTHONPATH=src python3 -m robot_trials.acceptance --workspace .
```

成功时退出码为 `0`，输出中的 `status` 为 `ok`。验收过程不会写入仓库，也不需要浏览器或外部服务。

## 启动 HTTP 服务

```bash
PYTHONPATH=src python3 -m robot_trials.api --database robot_trials.sqlite3 --host 127.0.0.1 --port 8080
```

接口使用 `X-Actor-Id` 表示当前操作人，写入观测时还需提供 `Idempotency-Key`。正式使用前应先创建操作员、统计负责人、审批人和审计人员，再登记机器人、软件构建与协议版本。服务进程可以停止后重新启动，SQLite 中的业务状态、分析任务和租约信息会保留。
