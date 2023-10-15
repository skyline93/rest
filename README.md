# rest

仓库地址：`/rest-repo`
备份源路径：`/testdata`

初始化仓库
```bash
go run cmd/rest/*.go init --password 123456 --uri local:/rest-repo
```

备份
```bash
go run cmd/rest/*.go backup --password 123456 --uri local:/rest-repo --path /testdata
```
