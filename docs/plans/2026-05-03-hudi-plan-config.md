# HudiPlanConfig Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move `ListingParallelism` from `HudiReadConfig` to a new `HudiPlanConfig` enum under the `hoodie.plan.*` namespace, fixing the bug where the `TableBuilder` silently drops this config.

**Architecture:** The `TableBuilder` filters all `hoodie.read.*` keys before creating `HudiConfigs`, but `FileLister` reads `ListingParallelism` from those configs. Since listing parallelism is not a persisted table property (`hoodie.properties`), a new `HudiPlanConfig` type under `hoodie.plan.*` is the right home. The builder's existing filter naturally passes `hoodie.plan.*` keys through.

**Tech Stack:** Rust (edition 2024), PyO3 Python bindings, `strum` derive macros, `ConfigParser` trait.

---

### Task 1: Create `HudiPlanConfig` enum

**Files:**
- Create: `crates/core/src/config/plan.rs`
- Modify: `crates/core/src/config/mod.rs:31-36` (add `pub mod plan;`)

**Step 1: Create `crates/core/src/config/plan.rs`**

Follow the exact pattern from `internal.rs`. The file needs the ASF license header, the enum with one variant (`ListingParallelism`), and the `ConfigParser` impl.

```rust
// ASF license header (copy from internal.rs)
//! Hudi plan configurations.

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

use strum_macros::{EnumIter, IntoStaticStr};

use crate::config::Result;
use crate::config::error::ConfigError::{NotFound, ParseInt};
use crate::config::{ConfigParser, HudiConfigValue};

/// Configurations for query planning in Hudi.
///
/// These control how the engine plans and executes operations like file listing.
/// Unlike [`super::table::HudiTableConfig`], these are not persisted in `hoodie.properties`.
/// Unlike [`super::read::HudiReadConfig`], these are not per-query read semantics.
#[derive(Clone, Debug, PartialEq, Eq, Hash, EnumIter, IntoStaticStr)]
pub enum HudiPlanConfig {
    /// Parallelism for listing files on storage.
    ListingParallelism,
}

impl HudiPlanConfig {
    pub const fn key_str(&self) -> &'static str {
        match self {
            Self::ListingParallelism => "hoodie.plan.listing.parallelism",
        }
    }
}

impl AsRef<str> for HudiPlanConfig {
    fn as_ref(&self) -> &str {
        self.key_str()
    }
}

impl Display for HudiPlanConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl ConfigParser for HudiPlanConfig {
    type Output = HudiConfigValue;

    fn default_value(&self) -> Option<HudiConfigValue> {
        match self {
            Self::ListingParallelism => Some(HudiConfigValue::UInteger(10usize)),
        }
    }

    fn parse_value(&self, configs: &HashMap<String, String>) -> Result<Self::Output> {
        let get_result = configs
            .get(self.as_ref())
            .map(|v| v.as_str())
            .ok_or(NotFound(self.key()));

        match self {
            Self::ListingParallelism => get_result
                .and_then(|v| {
                    usize::from_str(v).map_err(|e| ParseInt(self.key(), v.to_string(), e))
                })
                .map(HudiConfigValue::UInteger),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::plan::HudiPlanConfig::ListingParallelism;

    #[test]
    fn parse_valid_config_value() {
        let options = HashMap::from([
            (ListingParallelism.as_ref().to_string(), "20".to_string()),
        ]);
        let actual: usize = ListingParallelism.parse_value(&options).unwrap().into();
        assert_eq!(actual, 20);
    }

    #[test]
    fn parse_invalid_config_value() {
        let options = HashMap::from([
            (ListingParallelism.as_ref().to_string(), "_100".to_string()),
        ]);
        assert!(matches!(
            ListingParallelism.parse_value(&options).unwrap_err(),
            ParseInt(_, _, _)
        ));
        let actual: usize = ListingParallelism.parse_value_or_default(&options).into();
        assert_eq!(actual, 10);
    }

    #[test]
    fn default_value() {
        let empty = HashMap::new();
        let actual: usize = ListingParallelism.parse_value_or_default(&empty).into();
        assert_eq!(actual, 10);
    }

    #[test]
    fn display_matches_key() {
        assert_eq!(
            format!("{ListingParallelism}"),
            "hoodie.plan.listing.parallelism"
        );
    }
}
```

**Step 2: Register the module in `crates/core/src/config/mod.rs`**

Add `pub mod plan;` after the existing module declarations (line 32, between `internal` and `read`).

**Step 3: Run tests to verify the new module compiles and passes**

Run: `cargo test -p hudi-core config::plan::tests`
Expected: 4 tests PASS

---

### Task 2: Remove `ListingParallelism` from `HudiReadConfig`

**Files:**
- Modify: `crates/core/src/config/read.rs:77-196`

**Step 1: Remove `ListingParallelism` variant from the enum**

Remove line 98 (`ListingParallelism,`) and its doc comment (line 97).

**Step 2: Remove from `key_str()`**

Remove line 120 (`Self::ListingParallelism => "hoodie.read.listing.parallelism",`).

**Step 3: Remove from `default_value()`**

Remove line 148 (`HudiReadConfig::ListingParallelism => Some(HudiConfigValue::UInteger(10usize)),`).

**Step 4: Remove from `parse_value()`**

Remove lines 173-177 (the `Self::ListingParallelism => ...` arm).

**Step 5: Update tests in `read.rs`**

In `parse_valid_config_value`:
- Remove `ListingParallelism` from the import (line 202).
- Remove the `ListingParallelism` entry from the options HashMap (line 214).
- Remove the `ListingParallelism` parse assertion (lines 231).

In `parse_invalid_config_value`:
- Remove the `ListingParallelism` entry from the options HashMap (line 244).
- Remove the `ListingParallelism` error assertion (lines 261-265).

**Step 6: Run tests**

Run: `cargo test -p hudi-core config::read::tests`
Expected: all read config tests PASS

---

### Task 3: Update `FileLister` to use `HudiPlanConfig`

**Files:**
- Modify: `crates/core/src/table/listing.rs:21,218`

**Step 1: Change the import**

Replace line 21:
```rust
// before
use crate::config::read::HudiReadConfig::ListingParallelism;
// after
use crate::config::plan::HudiPlanConfig::ListingParallelism;
```

Line 218 (`self.hudi_configs.get_or_default(ListingParallelism)`) stays the same — only the import path changes.

**Step 2: Run tests**

Run: `cargo test -p hudi-core`
Expected: PASS (listing now reads from `hoodie.plan.*` namespace which is not filtered by the builder)

---

### Task 4: Update Python bindings to expose `HudiPlanConfig`

**Files:**
- Modify: `python/src/internal.rs:34,988`
- Modify: `python/hudi/_config.py:43-52`
- Modify: `python/hudi/__init__.py:19,34-47`
- Modify: `python/hudi/table/builder.py:21,24`
- Modify: `python/hudi/_internal.pyi` (add `HudiPlanConfig` to type stubs if needed)

**Step 1: Add `HudiPlanConfig` to `_config_keys()` in `python/src/internal.rs`**

At line 34, add:
```rust
use hudi::config::plan::HudiPlanConfig;
```

At line 988, add after the `HudiReadConfig` entry:
```rust
out.insert("HudiPlanConfig".to_string(), collect::<HudiPlanConfig>());
```

**Step 2: Create the Python enum in `python/hudi/_config.py`**

After the `HudiReadConfig` block (line 50), add:
```python
HudiPlanConfig = Enum(
    "HudiPlanConfig",
    _keys["HudiPlanConfig"],
    type=str,
    module=__name__,
    qualname="HudiPlanConfig",
)
HudiPlanConfig.__doc__ = "Configurations for query planning in Hudi."
```

Update `__all__` to include `"HudiPlanConfig"`.

**Step 3: Export from `python/hudi/__init__.py`**

Add `HudiPlanConfig` to the import and `__all__` list.

**Step 4: Update `python/hudi/table/builder.py` `ConfigKey` type alias**

Add `HudiPlanConfig` to the import and the `Union` type:
```python
from hudi._config import HudiPlanConfig, HudiReadConfig, HudiTableConfig

ConfigKey = Union[str, HudiTableConfig, HudiReadConfig, HudiPlanConfig]
```

**Step 5: Build and verify**

Run: `make develop` (builds Rust + installs Python binding via maturin)
Expected: builds successfully

---

### Task 5: Update Python tests

**Files:**
- Modify: `python/tests/test_table_builder.py:21,130-151`
- Modify: `python/tests/test_config_enums.py:24-67`

**Step 1: Update `test_table_builder.py`**

At line 21, add `HudiPlanConfig` to the import:
```python
from hudi import HudiPlanConfig, HudiReadConfig, HudiTableBuilder, HudiTableConfig
```

In `test_with_option_enum` (line 129), change `HudiReadConfig.LISTING_PARALLELISM` to `HudiPlanConfig.LISTING_PARALLELISM` and update the expected key to `"hoodie.plan.listing.parallelism"`.

In `test_enum_values_match_expected_strings` (line 138), move the `LISTING_PARALLELISM` assertion from `HudiReadConfig` to `HudiPlanConfig`:
```python
assert HudiPlanConfig.LISTING_PARALLELISM.value == "hoodie.plan.listing.parallelism"
```

**Step 2: Update `test_config_enums.py`**

Update the expected set of keys to include `"HudiPlanConfig"` (line 24).
Add assertions for `HudiPlanConfig` members and length.

**Step 3: Run Python tests**

Run: `pytest python/tests/test_table_builder.py python/tests/test_config_enums.py -v`
Expected: PASS

---

### Task 6: Full test suite verification

**Step 1: Run full Rust tests**

Run: `cargo test`
Expected: PASS

**Step 2: Run format and check**

Run: `make format check`
Expected: PASS

**Step 3: Run full Python tests**

Run: `pytest python/tests/ -v`
Expected: PASS
