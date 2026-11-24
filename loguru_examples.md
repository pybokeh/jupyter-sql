# Loguru Example Usage

Loguru is a drop-in replacement for the standard `logging` module with much less setup. Install with:

```bash
pip install loguru
```

## Basics

```python
from loguru import logger

logger.debug("Debugging details")
logger.info("Application started")
logger.success("Operation completed")
logger.warning("Disk space is low")
logger.error("Something failed")
logger.critical("System is going down")
```

Output is colorized to stderr by default, with timestamp, level, module, function, and line number.

## Formatting and Variables

```python
name, count = "Alice", 3
logger.info("User {} logged in {} times", name, count)
logger.info("User {name} logged in", name=name)
```

## Configuring Sinks

```python
import sys
from loguru import logger

logger.remove()  # remove the default handler

# Console with custom format and level
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
)

# File with rotation, retention, and compression
logger.add(
    "logs/app_{time:YYYY-MM-DD}.log",
    level="DEBUG",
    rotation="10 MB",      # or "00:00" (midnight), "1 week"
    retention="14 days",
    compression="zip",
    enqueue=True,          # thread/process-safe, non-blocking
)

# JSON structured logs
logger.add("logs/app.json", serialize=True)
```

## Capturing Exceptions

```python
# Decorator: logs any exception, then re-raises (or swallows with reraise=False)
@logger.catch
def divide(a, b):
    return a / b

divide(1, 0)

# Context manager
with logger.catch(message="Failed to process"):
    int("not a number")

# Inside an except block, with full traceback
try:
    1 / 0
except ZeroDivisionError:
    logger.exception("Math error")
```

For richer tracebacks that show variable values, add the sink with `diagnose=True` and `backtrace=True`. Set `diagnose=False` in production to avoid leaking sensitive data.

## Context with `bind` and `contextualize`

```python
# Attach extra data permanently to a logger instance
user_logger = logger.bind(user_id=42, request_id="abc123")
user_logger.info("Profile updated")

# Show extra fields in the format
logger.add(
    sys.stderr,
    format="{time} | {level} | {extra[request_id]} | {message}",
    filter=lambda r: "request_id" in r["extra"],
)

# Temporary context (works well with async/threads)
with logger.contextualize(request_id="xyz789"):
    logger.info("Handling request")
    logger.info("Done")
```

## Filtering by Level or Module

```python
# Separate file for errors only
logger.add("logs/errors.log", level="ERROR")

# Only messages from a specific module
logger.add("logs/db.log", filter="myapp.database")

# Custom filter function
logger.add("logs/slow.log", filter=lambda r: r["extra"].get("slow") is True)
logger.bind(slow=True).warning("Query took 5s")
```

## Custom Levels

```python
logger.level("AUDIT", no=25, color="<blue><bold>", icon="🔍")
logger.log("AUDIT", "User exported data")
```

## Lazy Evaluation (Avoid Costly Computation)

```python
logger.opt(lazy=True).debug("Result: {x}", x=lambda: expensive_function())
```

## Intercepting Standard `logging`

Useful so third-party libraries also route through loguru:

```python
import logging
from loguru import logger

class InterceptHandler(logging.Handler):
    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())

logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
```

## A Compact Real-World Setup

```python
import sys
from loguru import logger

def setup_logging(debug: bool = False):
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if debug else "INFO")
    logger.add("logs/app.log", rotation="1 day", retention="7 days",
               enqueue=True, level="INFO")
    logger.add("logs/errors.log", level="ERROR", backtrace=True,
               diagnose=False, rotation="10 MB")

setup_logging()

@logger.catch
def main():
    logger.info("Starting up")
    ...

if __name__ == "__main__":
    main()
```
