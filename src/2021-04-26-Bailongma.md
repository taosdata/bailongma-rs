1. 获取了客户环境下生产Prometheus数据，发现了一些错误并做了修复
2. 对Prometheus的请求返回模式做了修改，插入完成后才返回，相当于限制Prometheus写入速率
3. 目前已确认不会引发大量内存使用