# Changelog

## Unreleased

- Add StarRocks specializations for the SQL functions the Alteryx expression converter emits, so converted expressions run on StarRocks as well as Databend. Each keeps the existing Databend/default SQL unchanged and adds only a StarRocks form: regular-expression match/extract, `to_string`/`try_to_float64` casts, `modulo`, `ord`, `today`, the date-part extractors (`to_year`/`to_month`/…), the `add_*` date-add family, and unit-parameterized `date_diff`. As a side effect, the numeric `import_col` guard now casts to `CHAR` instead of the nonexistent `to_string()` on StarRocks ([@inviscid](https://github.com/inviscid)).
