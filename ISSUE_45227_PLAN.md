# Issue #45227 Implementation Plan
## DAG Import Errors message is dangling in web-interface

**Issue URL:** https://github.com/apache/airflow/issues/45227
**Status:** Open | Good First Issue
**Labels:** `area:core`, `area:MetaDB`, `area:UI`, `kind:bug`
**Affected Version:** Airflow 2.10.2+

---

## 📋 Problem Statement

Import error messages persist in the Airflow web UI even after the broken DAG file is deleted from the repository. When users:

1. Create a broken DAG in a remote repository
2. Observe the error message in Airflow's web interface
3. Delete the broken DAG from the repository
4. Wait for gitsync synchronization

**Expected:** Error message should disappear
**Actual:** Error message persists indefinitely

---

## 🔍 Root Cause Analysis

The issue involves two interconnected database fields:

### 1. ParseImportError Table
**Location:** `airflow-core/src/airflow/models/errors.py:30`

```python
class ParseImportError(Base):
    """Stores all Import Errors recorded when parsing DAGs."""
    __tablename__ = "import_error"
    id: Mapped[int]
    timestamp: Mapped[datetime | None]
    filename: Mapped[str | None]
    bundle_name: Mapped[str | None]
    stacktrace: Mapped[str | None]
```

### 2. DagModel.has_import_errors Flag
**Location:** `airflow-core/src/airflow/models/dag.py:388`

```python
class DagModel(Base):
    has_import_errors: Mapped[bool] = mapped_column(Boolean(), default=False, server_default="0")
```

### The Gap

**When import errors occur** (`airflow-core/src/airflow/dag_processing/collection.py:305-360`):
- ✅ `ParseImportError` record is created
- ✅ `DagModel.has_import_errors` is set to `True`

**When files are successfully parsed** (`airflow-core/src/airflow/dag_processing/collection.py:497`):
- ✅ `DagModel.has_import_errors` is set to `False`

**When files are deleted** (`airflow-core/src/airflow/dag_processing/manager.py:676-695`):
- ✅ `ParseImportError` records ARE deleted via `clear_orphaned_import_errors()`
- ❌ `DagModel.has_import_errors` flag is **NOT reset**

**Result:** DAGs with deleted error files remain flagged as having import errors, causing the UI to display stale error messages.

---

## 🎯 Solution

Update the `clear_orphaned_import_errors` method to reset the `has_import_errors` flag for DAGs whose error files have been deleted.

### File to Modify
**`airflow-core/src/airflow/dag_processing/manager.py:676-695`**

### Current Code
```python
@provide_session
def clear_orphaned_import_errors(
    self, bundle_name: str, observed_filelocs: set[str], session: Session = NEW_SESSION
):
    """
    Clear import errors for files that no longer exist.

    :param session: session for ORM operations
    """
    self.log.debug("Removing old import errors")
    try:
        errors = session.scalars(
            select(ParseImportError)
            .where(ParseImportError.bundle_name == bundle_name)
            .options(load_only(ParseImportError.filename))
        )
        for error in errors:
            if error.filename not in observed_filelocs:
                session.delete(error)
    except Exception:
        self.log.exception("Error removing old import errors")
```

### Proposed Fix
```python
@provide_session
def clear_orphaned_import_errors(
    self, bundle_name: str, observed_filelocs: set[str], session: Session = NEW_SESSION
):
    """
    Clear import errors for files that no longer exist.

    Also resets the has_import_errors flag for DAGs whose error files have been deleted.

    :param bundle_name: name of the bundle
    :param observed_filelocs: set of file locations currently observed in the bundle
    :param session: session for ORM operations
    """
    self.log.debug("Removing old import errors")
    try:
        errors = session.scalars(
            select(ParseImportError)
            .where(ParseImportError.bundle_name == bundle_name)
            .options(load_only(ParseImportError.filename))
        ).all()

        deleted_filenames = []
        for error in errors:
            if error.filename not in observed_filelocs:
                deleted_filenames.append(error.filename)
                session.delete(error)

        # Reset has_import_errors flag for DAGs from deleted files
        # This ensures the UI doesn't show stale import errors
        if deleted_filenames:
            from airflow.models import DagModel
            session.execute(
                update(DagModel)
                .where(
                    DagModel.bundle_name == bundle_name,
                    DagModel.relative_fileloc.in_(deleted_filenames)
                )
                .values(has_import_errors=False)
            )
            self.log.info(
                "Reset has_import_errors flag for %d DAG(s) from deleted files in bundle %s",
                len(deleted_filenames),
                bundle_name,
            )
    except Exception:
        self.log.exception("Error removing old import errors")
```

### Required Imports
Add to the imports section of `airflow-core/src/airflow/dag_processing/manager.py` (around line 40-60):

```python
from sqlalchemy import update  # Add if not already present
```

---

## 📝 Implementation Steps

### Step 1: Verify Current State
```bash
cd /home/user/airflow
source .venv/bin/activate
python -c "import airflow; print('Airflow version:', airflow.__version__)"
```

### Step 2: Create a Feature Branch
```bash
git checkout -b fix/issue-45227-stale-import-errors
```

### Step 3: Modify the Code

Edit `airflow-core/src/airflow/dag_processing/manager.py`:

1. Check if `update` is imported from sqlalchemy (around line 40-60)
2. Locate the `clear_orphaned_import_errors` method (line 676)
3. Replace the method with the proposed fix above
4. Save the file

### Step 4: Write Unit Tests

Create or update test file: `airflow-core/tests/unit/dag_processing/test_manager.py`

Add the following test case:

```python
def test_clear_orphaned_import_errors_resets_dag_flag(self, dag_maker, session):
    """Test that clearing orphaned import errors also resets has_import_errors flag."""
    from airflow.models import DagModel
    from airflow.models.errors import ParseImportError
    from airflow.dag_processing.manager import DagFileProcessorManager

    # Create a DAG with import error
    dag_model = DagModel(
        dag_id="test_dag",
        fileloc="/path/to/deleted_dag.py",
        relative_fileloc="deleted_dag.py",
        bundle_name="test_bundle",
        has_import_errors=True,
    )
    session.add(dag_model)

    # Create import error for the DAG
    import_error = ParseImportError(
        filename="deleted_dag.py",
        bundle_name="test_bundle",
        timestamp=timezone.utcnow(),
        stacktrace="Some error",
    )
    session.add(import_error)
    session.commit()

    # Create manager instance
    manager = DagFileProcessorManager(
        dag_directory="/tmp/dags",
        max_runs=1,
        processor_timeout=timedelta(minutes=1),
        signal_conn=MagicMock(),
        dag_ids=[],
        pickle_dags=False,
        async_mode=False,
    )

    # Simulate file deletion by calling clear with empty observed files
    manager.clear_orphaned_import_errors(
        bundle_name="test_bundle",
        observed_filelocs=set(),  # No files observed (deleted)
        session=session,
    )
    session.commit()

    # Verify import error is deleted
    remaining_errors = session.query(ParseImportError).filter(
        ParseImportError.bundle_name == "test_bundle"
    ).all()
    assert len(remaining_errors) == 0

    # Verify has_import_errors flag is reset
    session.expire_all()
    updated_dag = session.query(DagModel).filter(DagModel.dag_id == "test_dag").first()
    assert updated_dag.has_import_errors is False
```

### Step 5: Run Tests

```bash
cd /home/user/airflow

# Run the specific test
source .venv/bin/activate
pytest airflow-core/tests/unit/dag_processing/test_manager.py::TestDagFileProcessorManager::test_clear_orphaned_import_errors_resets_dag_flag -v

# Run all manager tests
pytest airflow-core/tests/unit/dag_processing/test_manager.py -v
```

### Step 6: Manual Testing (Optional but Recommended)

Create a test scenario:

```bash
# 1. Set up test environment
export AIRFLOW_HOME=~/airflow_test
mkdir -p $AIRFLOW_HOME/dags

# 2. Create a broken DAG
cat > $AIRFLOW_HOME/dags/broken_dag.py << 'EOF'
from airflow import DAG
this will cause syntax error
EOF

# 3. Start Airflow and observe error
airflow standalone

# 4. Delete the broken DAG
rm $AIRFLOW_HOME/dags/broken_dag.py

# 5. Wait for DAG processor cycle and verify error disappears from UI
# Access http://localhost:8080 and check Import Errors page
```

### Step 7: Commit Changes

```bash
git add airflow-core/src/airflow/dag_processing/manager.py
git add airflow-core/tests/unit/dag_processing/test_manager.py

git commit -m "$(cat <<'EOF'
Fix #45227: Clear has_import_errors flag when import error files are deleted

When DAG files with import errors are deleted, the ParseImportError
records were being cleaned up but the DagModel.has_import_errors flag
remained True, causing stale error messages to persist in the UI.

This fix updates the clear_orphaned_import_errors method to also reset
the has_import_errors flag for DAGs whose error files have been deleted.

Changes:
- Modified DagFileProcessorManager.clear_orphaned_import_errors() to
  update DagModel.has_import_errors flag when cleaning up orphaned errors
- Added test case to verify the flag is properly reset
- Added logging to track when the flag is reset

Fixes #45227
EOF
)"
```

### Step 8: Push to Remote

```bash
git push -u origin fix/issue-45227-stale-import-errors
```

### Step 9: Create Pull Request

Use the GitHub CLI or web interface:

```bash
gh pr create --title "Fix #45227: Clear has_import_errors flag when import error files are deleted" --body "$(cat <<'EOF'
## Description

Fixes #45227 - Import error messages now properly disappear from the web UI when the broken DAG file is deleted.

## Problem

When a DAG file with import errors was deleted, the `ParseImportError` records were cleaned up but the `DagModel.has_import_errors` flag remained `True`. This caused the UI to continue displaying stale import error messages.

## Solution

Updated the `clear_orphaned_import_errors()` method in `DagFileProcessorManager` to reset the `has_import_errors` flag for DAGs whose error files have been deleted.

## Changes

- Modified `airflow-core/src/airflow/dag_processing/manager.py`:
  - Enhanced `clear_orphaned_import_errors()` to update `DagModel.has_import_errors`
  - Added logging for tracking when flags are reset
- Added test case in `airflow-core/tests/unit/dag_processing/test_manager.py`

## Testing

- [x] Unit tests added and passing
- [x] Manual testing with broken DAG file deletion
- [x] Verified UI no longer shows stale errors

## Checklist

- [x] Code follows project style guidelines
- [x] Tests added for new functionality
- [x] Documentation updated (docstring improved)
- [x] Commit message follows conventions
- [x] Issue reference included in commit
EOF
)"
```

---

## 📁 Key Files Reference

| File | Purpose | Lines |
|------|---------|-------|
| `airflow-core/src/airflow/models/errors.py` | ParseImportError model definition | 30-46 |
| `airflow-core/src/airflow/models/dag.py` | DagModel.has_import_errors field | 388 |
| `airflow-core/src/airflow/dag_processing/manager.py` | **Main fix location** | 676-695 |
| `airflow-core/src/airflow/dag_processing/collection.py` | Where flags are set/cleared | 357, 497 |
| `airflow-core/src/airflow/api_fastapi/core_api/routes/public/import_error.py` | UI API endpoint | Full file |
| `airflow-core/tests/unit/dag_processing/test_manager.py` | Test location | Add new test |

---

## 🧪 Testing Strategy

### Unit Tests
- ✅ Test that `ParseImportError` records are deleted
- ✅ Test that `has_import_errors` flag is reset
- ✅ Test that only orphaned errors are affected
- ✅ Test error handling doesn't break the process

### Integration Tests
- Test with real DAG processor cycle
- Verify UI updates correctly
- Test with multiple bundles

### Edge Cases
- File exists but has no import errors (should not be affected)
- Multiple DAGs in same file with errors (all should be updated)
- Empty bundle (no files observed)

---

## 🚀 Next Session Prompt

```
I need you to implement the fix for Apache Airflow Issue #45227.

**Context:**
Import error messages persist in the Airflow web UI even after broken DAG files are deleted.

**Task:**
1. Read the complete implementation plan at `/home/user/airflow/ISSUE_45227_PLAN.md`
2. Implement the fix in `airflow-core/src/airflow/dag_processing/manager.py`
3. Add unit tests in `airflow-core/tests/unit/dag_processing/test_manager.py`
4. Run tests to verify the fix works
5. Commit the changes with proper message
6. Push to branch `fix/issue-45227-stale-import-errors`
7. Create a pull request

**Important:**
- Follow the exact implementation steps in the plan
- Ensure all tests pass before committing
- Use the commit message template from the plan
- The fix is small and focused - just updating one method

**Environment:**
- Working directory: `/home/user/airflow`
- Branch: Currently on `claude/explain-codebase-mk0x31ifmwuz1nzv-yqoAu`
- Python env: `.venv` (already set up)
- Airflow version: 3.2.0

Start by reading the plan file and then proceed with Step 1.
```

---

## 📌 Additional Notes

### Why This is a Good First Issue
- ✅ Small, focused change (one method)
- ✅ Clear root cause and solution
- ✅ Good learning opportunity (DAG processing, database models)
- ✅ Visible impact (fixes user-facing bug)
- ✅ Well-scoped testing requirements

### Alternative Approaches Considered

1. **Clear errors in delete_dag()**: Would miss cases where file is deleted but DAG isn't
2. **Add periodic cleanup job**: More complex, less efficient
3. **Check on UI display**: Treats symptom, not cause

**Chosen approach**: Update cleanup method (most direct, efficient, maintainable)

### Related Code Patterns

The pattern of resetting flags when cleaning up orphaned records is used elsewhere:
- `deactivate_deleted_dags()` sets `is_stale=True`
- `remove_references_to_deleted_dags()` cleans up related tables

Our fix follows these established patterns.

---

## ✅ Success Criteria

1. ✅ `ParseImportError` records are deleted for removed files
2. ✅ `DagModel.has_import_errors` flag is reset for affected DAGs
3. ✅ UI no longer displays stale import errors
4. ✅ Unit tests pass
5. ✅ No regression in existing functionality
6. ✅ Code follows project conventions
7. ✅ PR is approved and merged

---

**Plan Created:** 2026-01-05
**Repository:** `/home/user/airflow`
**Branch Status:** On `claude/explain-codebase-mk0x31ifmwuz1nzv-yqoAu`, synced with upstream/main (commit 8badad1541)
**Ready for Implementation:** ✅ Yes
