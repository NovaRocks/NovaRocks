# Iceberg REST view fixtures

`mv-spec.load-view.json` wraps the unchanged Materialized View Example from
[Iceberg view spec Appendix A at 04113031](https://github.com/JanKaul/iceberg/blob/04113031e7eb728f9102a3d3cfe0fcea5588c70e/format/view-spec.md#materialized-view-example)
in the REST response envelope. `mv-with-catalog.load-view.json` adds the
optional storage table catalog field.

The `trino-mv.create-view.json` and `spark-mv.create-view.json` requests
exercise that same storage identifier shape against the Java catalog.
The connector wire tests and SQL fixture directive share the requests.
Substitute `__VIEW__` for the view name, `__NAMESPACE__` for the case's
single namespace component, and `__STORAGE_TABLE__` for the storage table
name. Both queries read an INTEGER column `id` from a table named
`mv_base` in that namespace. The requests omit the optional catalog field
because the fixture predates it.
