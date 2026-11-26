"""备份校验调度演练台。

模块划分：
- config:   常量与路径
- timeutil: 演练时钟的格式化
- cluster:  模拟备份集群，独立 SQLite 库 cluster.db 保存 Job
- store:    演练控制库 drill.db（时钟/班次/事件/故障的调度侧记录）
- engine:   调度状态机与用例编排（推进时钟、触发调度、结束 Job、查回回执）
- web:      Flask 页面与 JSON API
"""
