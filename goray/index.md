# GoRay API Reference

GoRay is a Python driver for the GoRay project, enabling seamless integration between Go and Ray.

## Installation

```bash
pip install goray
```

For Ray support:
```bash
pip install goray[ray]
```

## Quick Start

```python
import goray

# Initialize GoRay with your Go library
goray.init(libpath="/path/to/go-ray/library")

# Use Go tasks from Python
task = goray.golang_task("TaskName")
result = task.remote(arg1, arg2)

# Use Go actors from Python
Counter = goray.golang_actor_class("Counter")
counter = Counter.remote(0)
counter.Inc.remote(1)
```

## API Documentation

::: goray
    options:
      show_root_heading: false
      show_source: true
      members_order: source
      group_by_category: false
      show_category_heading: false
      heading_level: 2
