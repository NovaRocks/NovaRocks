---
name: starust-ossutil
description: "Use when you need to access or manage Aliyun OSS objects or buckets during Starust or StarRocks debugging (list/stat/cat/upload/download/delete/sync) via ~/ossutilmac64."
---

# OSSUtil

## 概览

使用 `~/ossutilmac64` 访问阿里云 OSS，支持列举、查看、下载、上传、删除、同步对象。此 skill 不假设任何默认 bucket 或根路径，示例统一使用占位符路径，实际执行前必须替换为当前任务对应的目标。完整参数见 `~/ossutilmac64 -h`。

## 快速开始（只读）

1. 确认配置文件
   - 交互式创建：`~/ossutilmac64 config`
   - 默认配置路径：`$HOME/.ossutilconfig`
   - 使用自定义配置文件时，在命令中加 `-c <config_path>`
2. 先确认目标 bucket / prefix
   - 从用户提供的信息、配置、日志或 SQL 元数据中定位实际 OSS 路径
   - 示例占位符：`oss://<bucket>/<prefix>/`
3. 列举目标目录
   - `~/ossutilmac64 ls oss://<bucket>/<prefix>/ -s`
4. 查看文件详情或内容
   - `~/ossutilmac64 stat oss://<bucket>/<path>/<file>`
   - `~/ossutilmac64 cat oss://<bucket>/<path>/<file>`

## 常用任务

- 列举指定前缀
  - 短格式：`~/ossutilmac64 ls oss://<bucket>/<prefix>/ -s`
  - 只看第一层：`~/ossutilmac64 ls oss://<bucket>/<prefix>/ -d`
- 下载单个对象
  - `~/ossutilmac64 cp oss://<bucket>/<path>/<file> ./local-file`
- 上传文件到 OSS
  - `~/ossutilmac64 cp ./local-file oss://<bucket>/<path>/`
- 同步目录（谨慎使用）
  - `~/ossutilmac64 sync ./local-dir oss://<bucket>/<path>/`
  - 删除目标端多余对象：`~/ossutilmac64 sync ./local-dir oss://<bucket>/<path>/ --delete --backup-dir ./backup`
- 删除对象（高风险）
  - 删除单个对象：`~/ossutilmac64 rm oss://<bucket>/<path>/<file>`
  - 批量删除前缀（谨慎）：`~/ossutilmac64 rm oss://<bucket>/<prefix>/ -r`

## 重要注意事项

- 先定位真实路径：不要默认使用历史 bucket、个人目录或固定前缀。
- 先只读后写入：优先 `ls/stat/cat`，确认路径无误再 `cp/rm/sync`。
- 高风险操作（`rm -r`、`sync --delete`）必须明确确认目标前缀。
- 若对象名包含特殊字符，使用 `--encoding-type url`。
- 如需临时覆盖配置，使用 `-e`、`-i`、`-k`、`-t` 传入临时 endpoint/AK/STSToken。
- 不要在日志或文档中写入明文 AK/SK。

## 常见排查

- 认证失败：检查 `~/ossutilmac64 config` 的 `endpoint/accessKeyID/accessKeySecret`。
- 访问者付费：在命令中添加 `--payer requester`。
- 版本化对象：在 `stat/cat/cp/rm` 等命令中使用 `--version-id` 或 `--all-versions`。
