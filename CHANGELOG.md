# Changelog

## Unreleased

- Run Alteryx TitleCase, Median and pick-any aggregations on StarRocks workspaces ([@inviscid](https://github.com/inviscid)).
- Compile keyword-based `date_add` expressions to StarRocks-compatible SQL so week-based dashboard filters run without parser errors.
- Run concatenated aggregations, Text To Columns overflow fields, and array-to-text joins on StarRocks: `string_agg` now compiles to `group_concat` with a `SEPARATOR` clause (passing the delimiter as an argument appended it to every value instead of joining with it), and `array_to_string`/`array_tail` compile to `array_join`/`array_slice` ([@inviscid](https://github.com/inviscid)).
- Add StarRocks specializations for the SQL functions the Alteryx expression converter emits, so converted expressions run on StarRocks as well as Databend. Each keeps the existing Databend/default SQL unchanged and adds only a StarRocks form: regular-expression match/extract, `to_string`/`try_to_float64` casts, `modulo`, `ord`, `today`, the date-part extractors (`to_year`/`to_month`/…), the `add_*` date-add family, and unit-parameterized `date_diff` ([@inviscid](https://github.com/inviscid)).
