# uv Cheat Sheet

[uv](https://docs.astral.sh/uv/) is a fast Python package and project manager. It can be used in two distinct ways:

1. **Non-project workflow**: you manage a virtual environment and install packages yourself (`uv venv` + `uv pip`). No `pyproject.toml`, no lockfile.
2. **Project-oriented workflow**: you declare dependencies in `pyproject.toml`, and uv manages the environment and lockfile for you (`uv init`, `uv add`, `uv run`, `uv sync`).

---

## Part 1: Non-Project Workflow (`uv venv` + `uv pip`)

Use this when you just want a virtual environment, with no project files or folders created.

### Create a virtual environment

`uv venv` creates only the virtual environment (a `.venv` folder by default). It does not create a `pyproject.toml`, lockfile, or any other project files.

```bash
uv venv                  # creates ./.venv
uv venv myenv            # custom name/path
uv venv --python 3.12    # specific Python version (uv downloads it if needed)
uv venv ~/.venvs/scratch # central location instead of inside a folder
```

### Activate it

```bash
source .venv/bin/activate      # macOS/Linux
.venv\Scripts\activate         # Windows
```

### Install packages

Use the `uv pip` interface, a drop-in replacement for `pip`, rather than `uv add`:

```bash
uv pip install requests pandas
uv pip install -r requirements.txt
uv pip list
uv pip uninstall requests
```

### Things to avoid in this workflow

`uv init`, `uv add`, `uv sync`, and `uv run` (inside a project directory) all work with a `pyproject.toml`. Avoid them if you want to stay project-free.

---

## Part 2: Project-Oriented Workflow

Use this when you want reproducible, shareable projects. The workflow centers on a `pyproject.toml` and a lockfile (`uv.lock`), and uv manages the environment for you.

### 1. Create a project

```bash
uv init myproject        # new folder with pyproject.toml, .python-version, main.py, README.md
cd myproject
```

Or run `uv init` inside an existing folder to turn it into a project. Useful flags:

- `--app`: application layout (default)
- `--lib`: library layout with `src/`
- `--package`: packaged project
- `--python 3.12`: choose the Python version

### 2. Add dependencies

```bash
uv add requests pandas           # runtime dependencies
uv add --dev pytest ruff         # dev dependency group
uv add "httpx>=0.27"             # with a version constraint
uv remove requests               # remove a dependency
```

`uv add` edits `pyproject.toml`, resolves versions, updates `uv.lock`, and installs into `.venv` (creating it if needed). You never have to create or activate the environment manually.

### 3. Run things

```bash
uv run main.py
uv run pytest
uv run python -c "import pandas"
```

`uv run` first makes sure the environment matches the lockfile, then runs your command inside it. Activation is optional.

### 4. Sync and lock

```bash
uv sync                          # install exactly what's in uv.lock (creates .venv if missing)
uv sync --no-dev                 # skip dev dependencies
uv lock                          # re-resolve and update uv.lock without installing
uv lock --upgrade                # upgrade all locked versions
uv lock --upgrade-package requests
```

### 5. Inspect

```bash
uv tree                          # dependency tree
uv pip list                      # what's installed in the project env
```

### 6. Manage Python versions

```bash
uv python install 3.12
uv python pin 3.12               # writes .python-version
```

### What to commit

Commit these:

- `pyproject.toml`
- `uv.lock`
- `.python-version`

Don't commit `.venv/`. `uv init` adds it to `.gitignore` for you.

A teammate (or CI) clones the repo and runs `uv sync` to get an identical environment.

### Handy extras

- `uv run --with rich script.py` runs with a temporary extra package without adding it to the project.
- `uv build` and `uv publish` build and upload a package to PyPI.
- `uv export --format requirements-txt > requirements.txt` generates a requirements file if another tool needs one.

---

## Quick Comparison

| | Non-project | Project-oriented |
|---|---|---|
| Create environment | `uv venv` | automatic (`uv add` / `uv sync` / `uv run`) |
| Install packages | `uv pip install ...` | `uv add ...` |
| Dependency record | none (or `requirements.txt` you maintain) | `pyproject.toml` + `uv.lock` |
| Activation | manual (`source .venv/bin/activate`) | optional (use `uv run`) |
| Reproducibility | manual | built in via lockfile |
| Best for | quick experiments, scratch envs, legacy `requirements.txt` workflows | applications, libraries, anything shared or deployed |

The key mental shift: in the non-project workflow you install things into an environment by hand, whereas in the project workflow you **declare** dependencies with `uv add` and let uv own the environment.

---

## Cheat Sheet

```bash
# Non-project
uv venv
source .venv/bin/activate
uv pip install <pkg>

# Project
uv init myproject && cd myproject
uv add <pkg>
uv run main.py
uv sync
```
