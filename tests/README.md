# GoRay 集成测试

由于 GoRay 应用需要特殊启动方式，**无法通过go test 启动**。
因此，需要手动启动测试用例。

## 手动启动测试用例

```bash
bash ../examples/run.sh local --import cases/registry.py
```

## 测试用例编写指南

测试用例存放在 `cases` 目录下， 使用 `AddTestCase` 函数注册测试用例。

- Ray task 测试: 被测试ray任务通过 `testTask` 结构体的方法定义，所有公共方法都会自动注册为 Ray 任务。测试用例中可以通过 `ray.RemoteCall` 调用。
- Ray actor 测试：通过 `RegisterActor(actorFactoryFunction) string` 传入工厂函数注册 actor。测试用例中可以通过 `ray.NewActor` 创建 actor 实例。
- Go跨语言调用python测试：在 cases/go2py.py 中添加python task/actor定义，在 cases/go2py.go 中添加测试用例。
- Python跨语言调用go测试：在 cases/py2go.go 中添加go task/actor定义，在 cases/py2go.py 中使用添加测试用例 (pytest语法，以 test_ 开头的函数为测试用例)。

