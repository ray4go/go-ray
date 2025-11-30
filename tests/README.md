# GoRay Integration Tests

Since the GoRay application requires to built as a shared library, tests cannot be run with go test suits.
Instead, we provide a minimal test framework that allows manual execution of test cases.

## Run tests

```bash
pip install -r requirements.txt
bash ../run.sh . local --py-defs tests/cases/registry.py
```

Set the `TEST_PATTEN` environment variable to specify test cases to run (supports regex pattens).

## Writing test cases

Place test cases in the `cases` directory and register them with the `AddTestCase` function.

- Ray task tests: Define the Ray tasks under test as methods on the `testTask` struct. All exported methods are
  automatically registered as Ray tasks. In test cases, invoke them with `ray.RemoteCall`.
- Ray actor tests: Register an actor by passing a factory function to `RegisterActor(actorFactoryFunction) string`.
  `RegisterActor` will return the actor type name. In test cases, create actor instances with `ray.NewActor`.
- Go-call-Python tests: Add Python task/actor definitions in `cases/go2py.py` and add the corresponding
  Go test cases in `cases/go2py.go`.
- Python-call-Go tests: Add Go task/actor definitions in `cases/py2go.go` and write the Python tests in
  `cases/py2go.py` (python functions starting with `test_` are test cases).