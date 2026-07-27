# Legacy Sheet Writer Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the unused openpyxl-era `src/excel/sheet_writers.py` module and align docs with the actual `FastSheetWriter` path.

**Architecture:** Use import/reference tests to prove the module is not part of the production path, delete the dead module, and update README's Excel directory summary. Do not refactor `fast_writer.py` or change workbook semantics in this task.

**Tech Stack:** Python 3.11, pytest architecture tests, ripgrep evidence, README docs.

---

## File Map

- Delete: `src/excel/sheet_writers.py`
- Modify: `README.md`
- Modify: `tests/architecture/test_import_rules.py`
  - Add a regression that no project code imports `src.excel.sheet_writers`.

## Task 1: Guard Against Legacy Writer Imports

**Files:**
- Modify: `tests/architecture/test_import_rules.py`

- [ ] **Step 1: Write architecture test**

Add:

```python
def test_project_no_longer_imports_legacy_sheet_writer() -> None:
    for module_path in _project_python_files():
        imports = _collect_imports(module_path)
        assert 'src.excel.sheet_writers' not in imports, module_path
```

- [ ] **Step 2: Run architecture test**

Run:

```powershell
conda run -n test python -m pytest tests/architecture/test_import_rules.py::test_project_no_longer_imports_legacy_sheet_writer -q
```

Expected: PASS, proving no current imports.

- [ ] **Step 3: Commit guard test**

Run:

```powershell
git add tests/architecture/test_import_rules.py
git commit -m "test(architecture): guard against legacy sheet writer imports"
```

## Task 2: Delete Legacy Module And Update README

**Files:**
- Delete: `src/excel/sheet_writers.py`
- Modify: `README.md`

- [ ] **Step 1: Delete unused module**

Delete `src/excel/sheet_writers.py`.

- [ ] **Step 2: Update README Excel module list**

Replace:

```markdown
- `src/excel/` - Excel 写出与样式模块
  - `styles.py` / `sheet_writers.py` / `workbook_writer.py`
```

With:

```markdown
- `src/excel/` - Excel 写出与样式模块
  - `styles.py` / `fast_writer.py` / `workbook_writer.py`
```

- [ ] **Step 3: Run targeted checks**

Run:

```powershell
conda run -n test python -m pytest tests/architecture/test_import_rules.py tests/test_costing_etl.py::test_workbook_writer_routes_hot_sheets_to_fast_writer -q
```

Expected: PASS.

- [ ] **Step 4: Commit cleanup**

Run:

```powershell
git add README.md tests/architecture/test_import_rules.py
git rm src/excel/sheet_writers.py
git commit -m "refactor(excel): remove unused legacy sheet writer"
```

## Task 3: Verification

**Files:**
- No code changes unless verification exposes a defect.

- [ ] **Step 1: Run full tests**

Run:

```powershell
conda run -n test python -m pytest tests -q
```

Expected: PASS.

- [ ] **Step 2: Confirm no references remain**

Run:

```powershell
rg -n "sheet_writers|SheetWriter" src tests README.md
```

Expected: no references to `src/excel/sheet_writers.py` or `SheetWriter`; historical docs under `docs/superpowers/` may still mention it and do not need rewriting.

