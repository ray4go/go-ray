# GoRay 集成测试

由于 GoRay 应用需要特殊启动方式，**无法通过go test 启动**。
因此，需要手动启动测试用例。

## 手动启动测试用例

```bash
bash ../examples/run.sh
```

## 测试框架说明

测试框架基于 Go 的 `testing` 包构建，使用自定义的 `addTestCase` 函数注册测试用例。

Ray task 测试: 被测试ray任务通过 `testTask` 结构体的方法定义，所有公共方法都会自动注册为 Ray 任务。测试用例中可以通过 `ray.RemoteCall` 调用。
Ray actor 测试：通过 `registerActor(actorFactoryFunction) string` 传入工厂函数注册 actor。测试用例中可以通过 `ray.NewActor` 创建 actor 实例。

