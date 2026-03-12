ADMIN SET FRONTEND CONFIG ("enable_fast_schema_evolution"="true");
DROP DATABASE IF EXISTS sc_add_col_${uuid0} FORCE;
DROP DATABASE IF EXISTS sc_struct_${uuid0} FORCE;
DROP DATABASE IF EXISTS sc_array_${uuid0} FORCE;
DROP DATABASE IF EXISTS sc_no_fse_${uuid0} FORCE;
DROP DATABASE IF EXISTS sc_last_field_${uuid0} FORCE;
DROP DATABASE IF EXISTS sc_same_name_${uuid0} FORCE;
