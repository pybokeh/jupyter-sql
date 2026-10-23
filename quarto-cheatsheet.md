# Quarto Cheat Sheet: Execution & Cell Options

Applies to Python (Jupyter engine). Precedence, highest to lowest: **cell `#|` options > document YAML > `_quarto.yml`**.

---

## 1. Where to set options

**Cell level** (top of a code cell, `#|` comments, YAML syntax):

````markdown
```{python}
#| echo: false
#| warning: false
df.describe()
```
````

**Document level** (YAML front matter):

```yaml
---
title: "My Report"
execute:
  echo: false
  warning: false
---
```

**Project level** (`_quarto.yml`):

```yaml
project:
  type: website

execute:
  freeze: auto
  echo: false
```

---

## 2. Core execution options

These work at **cell**, **document** (`execute:`), and **project** level.

| Option | Values | Default | Effect |
|---|---|---|---|
| `eval` | `true` / `false` | `true` | Run the cell. `false` shows code without running it. |
| `echo` | `true` / `false` / `fenced` | `true` | Show source code. `fenced` also shows the ```` ```{python} ```` fence line. |
| `output` | `true` / `false` / `asis` | `true` | Include output. `asis` treats output as raw markdown/HTML. |
| `warning` | `true` / `false` | `true` | Include warnings in output. |
| `error` | `true` / `false` | `false` | `true` keeps rendering after an error and shows it. |
| `include` | `true` / `false` | `true` | `false` hides code, output, messages and warnings (cell still runs). |

### Quick recipes

| Goal | Option |
|---|---|
| Show code, don't run it | `eval: false` |
| Run it, hide code, show output | `echo: false` |
| Run it, show code, hide output | `output: false` |
| Run it, hide everything | `include: false` |
| Suppress warnings only | `warning: false` |
| Keep rendering despite errors | `error: true` |
| Emit raw markdown/HTML from a cell | `output: asis` |
| Show the fence line in the rendered code | `echo: fenced` |

---

## 3. Document/project-level only options

Set under `execute:` in YAML (not meaningful per cell).

| Option | Values | Effect |
|---|---|---|
| `enabled` | `true` / `false` | `false` disables execution for the whole document. |
| `cache` | `true` / `false` | Cache cell results so unchanged cells don't re-run. |
| `freeze` | `true` / `auto` / `false` | Skip re-execution on project-wide renders. `auto` re-runs only if the source file changed. |
| `daemon` | `true` / `false` / seconds | Keep the Jupyter kernel alive between renders. |
| `keep-ipynb` | `true` / `false` | Keep the intermediate `.ipynb` after rendering. |
| `engine` / `jupyter` | e.g. `jupyter: python3` | Choose the engine or kernel. |

```yaml
execute:
  enabled: true
  cache: true
  freeze: auto
  daemon: 300
```

---

## 4. Code appearance options

| Option | Values | Effect |
|---|---|---|
| `code-fold` | `true` / `false` / `show` | Collapse code behind a toggle (HTML). `show` starts expanded. |
| `code-summary` | text | Label for the folded code toggle. |
| `code-overflow` | `scroll` / `wrap` | Handle long lines. |
| `code-line-numbers` | `true` / `false` | Show line numbers. |
| `code-tools` | `true` / `false` | Show the code tools menu (document-level, HTML). |
| `lst-label` / `lst-cap` | text | Label and caption a code listing for cross-references. |
| `filename` | text | Show a filename above the code block. |

```python
#| code-fold: true
#| code-summary: "Show the code"
```

---

## 5. Figure options

| Option | Example | Effect |
|---|---|---|
| `label` | `fig-scatter` | Cross-reference ID (prefix `fig-`). |
| `fig-cap` | `"Scatter plot"` | Figure caption. |
| `fig-subcap` | `["A", "B"]` | Subcaptions for multi-figure cells. |
| `fig-alt` | `"Alt text"` | Alt text for accessibility. |
| `fig-width` / `fig-height` | `6` / `4` | Size in inches. |
| `fig-format` | `png`, `svg`, `pdf`, `retina` | Image format. |
| `fig-dpi` | `300` | Resolution. |
| `fig-align` | `left` / `center` / `right` | Alignment. |
| `fig-env` / `fig-pos` | `figure`, `h` | LaTeX figure environment / position. |

```python
#| label: fig-hist
#| fig-cap: "Distribution of values"
#| fig-width: 6
#| fig-height: 4
```

Reference it in text with `@fig-hist`.

---

## 6. Table options

| Option | Example | Effect |
|---|---|---|
| `label` | `tbl-summary` | Cross-reference ID (prefix `tbl-`). |
| `tbl-cap` | `"Summary stats"` | Table caption. |
| `tbl-subcap` | `["A", "B"]` | Subcaptions. |
| `tbl-colwidths` | `[30, 70]` or `auto` | Column widths (percent). |

Reference with `@tbl-summary`. To render a DataFrame as markdown table output, pair with `output: asis` or use `IPython.display.Markdown`.

---

## 7. Layout options

| Option | Example | Effect |
|---|---|---|
| `layout-ncol` | `2` | Arrange outputs in N columns. |
| `layout-nrow` | `2` | Arrange outputs in N rows. |
| `layout` | `[[1, 1], [2]]` | Custom grid layout (relative widths). |
| `column` | `page`, `body-outset`, `screen` | Page column the output spans. |
| `fig-column` / `tbl-column` | `margin` | Place just figures or tables in a column. |
| `cap-location` | `top` / `bottom` / `margin` | Caption position. |

```python
#| layout-ncol: 2
#| fig-cap: ["Plot A", "Plot B"]
```

---

## 8. Output & presentation options

| Option | Values | Effect |
|---|---|---|
| `output-location` | `default` / `fragment` / `slide` / `column` / `column-fragment` | Where output appears in revealjs slides. |
| `classes` | text | Add CSS classes to the cell. |
| `attr-output` / `attr-source` | attributes | Attributes for output / source blocks. |
| `html-table-processing` | `none` | Skip Quarto's HTML table post-processing. |
| `lightbox` | `true` / `auto` | Click-to-zoom for figures (HTML). |

---

## 9. Labels, IDs & misc

| Option | Effect |
|---|---|
| `label` | Cell ID, also used for cross-references. Prefixes: `fig-`, `tbl-`, `lst-`. |
| `output-file` | Name of the output file (document level). |
| `cache` | Per-cell caching also supported with the Jupyter cache option. |

---

## 10. Cell options in a Python cell: full example

````markdown
```{python}
#| label: fig-sales
#| echo: false
#| warning: false
#| fig-cap: "Monthly sales"
#| fig-width: 7
#| fig-height: 4
#| fig-alt: "Line chart of monthly sales"
#| code-fold: true
#| code-summary: "Show code"

import matplotlib.pyplot as plt
plt.plot(months, sales)
plt.show()
```
````

---

## 11. Handy shell commands

```bash
quarto render doc.qmd                    # render
quarto render doc.qmd --execute          # force execution
quarto render doc.qmd --no-execute       # skip execution
quarto render doc.qmd --cache-refresh    # refresh cached results
quarto render doc.qmd --execute-daemon-restart   # restart kernel daemon
quarto preview doc.qmd                   # live preview
```

---

## 12. Notes & gotchas

- Cell options use **YAML syntax** after `#|` (`echo: false`, not `echo=False`).
- `include: false` still **runs** the code. To skip running, use `eval: false`.
- `error: true` is useful for teaching material that demonstrates errors.
- `freeze` only matters for **project** renders (websites, books), not single-file renders.
- `message` is mainly relevant for R/Knitr; Jupyter handles most messages via `warning`/`output`.
- Some options only affect certain formats (e.g., `code-fold` is HTML-only, `output-location` is revealjs-only).

**Docs:** https://quarto.org/docs/computations/execution-options.html
**Jupyter cell options reference:** https://quarto.org/docs/reference/cells/cells-jupyter.html
