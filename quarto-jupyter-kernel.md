SOURCE: https://quarto.org/docs/computations/python.html#kernel-selection

# Using Quarto with a Specific Jupyter Kernel

There are a few ways to tell Quarto which Jupyter kernel to use, depending on whether you want to set it per document, per project, or per run.

## 1. Set it in the document's YAML header

For a `.qmd` file, use the `jupyter` key with the kernel name:

```yaml
---
title: "My Document"
jupyter: myenv
---
```

The value is the **kernel name** (not the display name). You can also use the full spec form if you need more control:

```yaml
jupyter:
  kernelspec:
    name: myenv
    language: python
    display_name: Python (myenv)
```

For a `.ipynb` notebook, the kernel is taken from the notebook's own metadata, so just select the kernel in Jupyter/VS Code and save.

## 2. Find the right kernel name

```bash
quarto check jupyter
# or
jupyter kernelspec list
```

Use the name in the left column (e.g., `python3`, `myenv`, `ir`).

## 3. Register your environment as a kernel (if it's not listed)

Activate the environment, then:

```bash
pip install ipykernel
python -m ipykernel install --user --name myenv --display-name "Python (myenv)"
```

Now `jupyter: myenv` will work in your documents.

## 4. Set it for a whole project

In `_quarto.yml`:

```yaml
project:
  type: website

jupyter: myenv
```

Every `.qmd` in the project will use that kernel unless it overrides it in its own header.

## 5. Point Quarto at a specific Python/venv instead

If you just want Quarto to use a particular Python installation (rather than selecting by kernel name), set the `QUARTO_PYTHON` environment variable:

```bash
QUARTO_PYTHON=/path/to/venv/bin/python quarto render doc.qmd
```

Quarto also auto-detects an activated virtual environment (or `.venv` in the project directory) in many setups, so activating the env before running `quarto render` often does the job.

## 6. Other languages

The same approach works for non-Python kernels, e.g. `jupyter: julia-1.10` or `jupyter: ir`. Make sure the kernel is installed and shows up in `jupyter kernelspec list`.

## Troubleshooting

- **"Kernel not found"**: the name in YAML doesn't match `jupyter kernelspec list`. Names are case-sensitive.
- **Wrong packages being imported**: the kernel was registered from a different environment than you expected. Reinstall it from the correct activated env.
- **VS Code / RStudio**: these editors may use their own interpreter selection for interactive runs, but `quarto render` still follows the YAML/`QUARTO_PYTHON` rules above.
- Run `quarto check` to confirm which Python and Jupyter Quarto is actually using.
