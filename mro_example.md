# Using `__mro__` to Understand Library Source Code

Python's standard library has a good example in `http.server.ThreadingHTTPServer`. It's a short class that gets almost all of its behavior from parents, which is the situation that makes library source hard to read.

If you open the source for `ThreadingHTTPServer`, it's about two lines:

```python
class ThreadingHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads = True
```

So where does the server actually handle a request? Ask Python:

```python
from http.server import ThreadingHTTPServer

for cls in ThreadingHTTPServer.__mro__:
    print(cls)
```

```
<class 'http.server.ThreadingHTTPServer'>
<class 'socketserver.ThreadingMixIn'>
<class 'http.server.HTTPServer'>
<class 'socketserver.TCPServer'>
<class 'socketserver.BaseServer'>
<class 'object'>
```

That list is the order Python searches when you call a method. It looks in `ThreadingHTTPServer` first, then `ThreadingMixIn`, and so on down. The first class that defines the method wins.

## A small tool: which class does a method come from?

```python
def where_defined(cls, method_name):
    return [c.__name__ for c in cls.__mro__ if method_name in vars(c)]

for name in ["process_request", "server_bind", "serve_forever"]:
    print(name, "->", where_defined(ThreadingHTTPServer, name))
```

```
process_request -> ['ThreadingMixIn', 'BaseServer']
server_bind     -> ['HTTPServer', 'TCPServer']
serve_forever   -> ['BaseServer']
```

## How to read the output

- **`serve_forever`** is defined only in `BaseServer`, so that's the only place to look.
- **`process_request`** is defined in two places, but `ThreadingMixIn` comes first in the MRO, so its version runs. `BaseServer`'s version, which handles the request directly, is overridden. That one fact explains why this server handles each request in a new thread. Without the MRO you might read `BaseServer.process_request` and wrongly conclude requests run one at a time.
- **`server_bind`** is defined in both `HTTPServer` and `TCPServer`. If you open `HTTPServer.server_bind`, you'll see it calls `super().server_bind()` and then does a little extra work. That `super()` call means "continue to the next class in the MRO," which is `TCPServer`.

That last point is what makes `__mro__` most useful. `super()` doesn't mean "my parent class." It means "the next class in the MRO," and with multiple inheritance or mixins those can differ. When you see `super()` in library code, the MRO tells you where it goes next.

## Workflow for reading unfamiliar OOP source

1. Find the class you're using and print its `__mro__`.
2. Pick a method you care about and run something like `where_defined` on it.
3. Open only the first class in that list, since it's the one that runs. Read the others only if it calls `super()`.

You can also use `inspect.getsourcefile(cls)` and `inspect.getsourcelines(cls.method)` to jump straight to the code from a REPL.
