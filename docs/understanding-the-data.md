# Understanding the Data

This page provides an overview of the data structure, common field conventions, and useful interpretation tips.
The dataset originates from **BUFR** format but has been converted to **Parquet** for easier analysis.
While the core information remains unchanged, some fields retain naming conventions from BUFR, which may require additional explanation.

---

## 📌 Dot-Joined Naming Convention for Struct Fields

### Why Are Some Column Names Dot-Joined?
When the original **BUFR** data contained structured fields (e.g., nested subfields within a record), these have been **flattened** into a single column name using **dot notation**.

### Example:
Suppose a BUFR record had a structured field like this:
```
WNDSQ1 (struct)
├── WSPD (Wind Speed)
├── WDIR (Wind Direction)
```

This structure is **flattened** into the following column names:

| Column name | Meaning |
|------------------------|----------------------|
| `WNDSQ1.WSPD`  | Wind Speed for `WNDSQ1` |
| `WNDSQ1.WDIR`  | Wind Direction for `WNDSQ1` |

### How to Interpret These Fields?
- The **prefix** (`WNDSQ1`) corresponds to a category or grouping in BUFR.
- The **suffix** (`WSPD`, `WDIR`, etc.) is the actual measured variable.
- If multiple similar structures exist (e.g., `WNDSQ2`), they follow the same convention (`WNDSQ2.WSPD`, `WNDSQ2.WDIR`).

### Why This Matters
- **Maintains clarity** while flattening structured data.
- **Prevents collisions** between similarly named subfields.
- **Allows for easy selection** of related variables depending on query tool (`WNDSQ1.*` to get all wind-related fields).


---

## ⏳ Understanding `.DT` and `.RH` Fields

### What Does `.DT...` Mean?
Fields prefixed with `.DT` represent **time durations associated with a specific measurement**. The unit of these is indicated by the first letter following `.DT`,and the measurement is question is the remainder of the field name (e.g. `.DTMMXGS` modifies `MXGS` and is a duration in minutes).

#### Example: `.DTMMXGS`
- **Full Name:** `WNDSQ1.WNDSQ2..DTMMXGS`
- **Meaning:** Time duration (in minutes) over which the maximum wind gust (`WNDSQ1.WNDSQ2.MXGS`) was recorded.
- **Example Value:**
  - `10` → The max gust was measured over a **10-minute period**.
  - `30` → The max gust was measured over a **30-minute period**.

#### Other `.DT...` Fields
| Name  | Meaning |
|-------------|------------------------------------------------|
| `.DTHMXTM`   | Time duration for which a max temperature (`MXTM`) is valid (e.g. previous 12 vs 24 hours) |
| `.DTHTOPC`   | Time period total precipitation accumulation |

If you encounter a `.DT...` field, you can assume it represents a time period for the associated measurement (usually the following column). Note that this field **can be missing** (null-value); in this case it is up to the user to decide whether to assume a time duration or discard the data.

### What Does `.RE...` Mean?

Fields prefixed with `.RE` are general modifiers for other fields, which are given by the remainder of the field name (e.g. `.REHOVI` is a modifier of `HOVI`). The meaning of the `.RE` flag is given by this [code table](https://www.nco.ncep.noaa.gov/sib/jeff/CodeFlag_0_STDv31_LOC7.html#008201).

#### Example: `.REHOVI`=0, `HOVI`=200
- **Meaning:** from table, a `.RE...` value of 0 means "the true value of the following parameter is below the minimum value which can be assessed with the system in use". This indicates that the horizontal visibility is less than 200 meters.

Different airports may have different minimum visibility. This field can also be missing (null value), in which case a reasonable interpretation would be that the `HOVI` field, if not also null, indicates the actual visibility.

---

