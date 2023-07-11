#include "scalardb.h"

#include "condition.h"
#include "jni.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "postgres.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"

#define JNI_VERSION JNI_VERSION_1_8

#define Str(arg) #arg
#define StrValue(arg) Str(arg)
#define STR_SCALARDB_JAR_PATH StrValue(SCALARDB_JAR_PATH)

/* a length of "-Djava.class.path=" */
#define JAVA_CLASS_PATH_STR_LEN 18
/* a length of ":" */
#define CLASSPATH_SEP_STR_LEN 1
/* a length of "\0" */
#define NULL_STR_LEN 1
/* a length of "-Xmx" */
#define MAX_HEAP_SIZE_STR_LEN 4
/* a length of "file:/" */
#define FILE_URL_PREFIX_LEN 5

#define DEFAULT_MAX_HEAP_SIZE "1g"

#define LOCAL_FRAME_CAPACITY 128

static __thread JNIEnv *env = NULL;
static JavaVM *jvm;

static jclass Object_class;
static jmethodID Object_toString;

static jclass String_class;

static jclass List_class;
static jmethodID List_size;
static jmethodID List_iterator;

static jclass Iterator_class;
static jmethodID Iterator_hasNext;
static jmethodID Iterator_next;

static jclass Optional_class;
static jmethodID Optional_isPresent;
static jmethodID Optional_get;

static jclass Closeable_class;
static jmethodID Closeable_close;

static jclass ScalarDbUtils_class;
static jmethodID ScalarDbUtils_initialize;
static jmethodID ScalarDbUtils_closeStorage;
static jmethodID ScalarDbUtils_scan;
static jmethodID ScalarDbUtils_buildableScan;
static jmethodID ScalarDbUtils_buildableScanWithIndex;
static jmethodID ScalarDbUtils_buildableScanAll;
static jmethodID ScalarDbUtils_keyBuilder;
static jmethodID ScalarDbUtils_getResultColumnsSize;
static jmethodID ScalarDbUtils_getPartitionKeyNames;
static jmethodID ScalarDbUtils_getClusteringKeyNames;
static jmethodID ScalarDbUtils_getSecondaryIndexNames;

static jclass Result_class;
static jmethodID Result_isNull;
static jmethodID Result_getBoolean;
static jmethodID Result_getInt;
static jmethodID Result_getBigInt;
static jmethodID Result_getFloat;
static jmethodID Result_getDouble;
static jmethodID Result_getText;
static jmethodID Result_getBlobAsBytes;

static jclass Scanner_class;
static jmethodID Scanner_one;

static jclass BuildableScan_class;
static jmethodID BuildableScan_projections;
static jmethodID BuildableScan_start;
static jmethodID BuildableScan_end;
static jmethodID BuildableScan_build;

static jclass BuildableScanWithIndex_class;
static jmethodID BuildableScanWithIndex_projections;
static jmethodID BuildableScanWithIndex_projections;
static jmethodID BuildableScanWithIndex_build;

static jclass BuildableScanAll_class;
static jmethodID BuildableScanAll_projections;
static jmethodID BuildableScanAll_build;

static jclass KeyBuilder_class;
static jmethodID KeyBuilder_addBoolean;
static jmethodID KeyBuilder_addInt;
static jmethodID KeyBuilder_addBigInt;
static jmethodID KeyBuilder_addFloat;
static jmethodID KeyBuilder_addDouble;
static jmethodID KeyBuilder_addText;
static jmethodID KeyBuilder_addBlob;
static jmethodID KeyBuilder_build;

static void initialize_jvm(ScalarDbFdwOptions *opts);
static void destroy_jvm(void);
static void attach_jvm(void);
static void get_jni_env(void);
static void initialize_standard_references(void);
static void initialize_scalardb_references(void);
static void add_classpath_to_system_class_loader(char *classpath);

static jobject get_key_from_conds(ScalarDbFdwScanCondition *scan_conds,
				  size_t num_scan_conds);
static jobject get_key_for_boundary(List *names, Datum *values,
				    List *value_types, size_t num_values);
static void add_datum_value_to_key(jobject key_builder, jstring name,
				   Datum value, Oid value_type);
static void apply_column_pruning(jobject buildable_scan, List *attnames,
				 jmethodID projections_method);
static void apply_clustering_key_boundary(jobject buildable_scan,
					  ScalarDbFdwScanBoundary *boundary);

static void clear_exception(void);
static void catch_exception(void);

static char *convert_string_to_cstring(jobject java_cstring);
static text *convert_string_to_text(jobject java_cstring);
static bytea *convert_jbyteArray_to_bytea(jbyteArray bytes);
static jobjectArray convert_string_list_to_jarray_of_string(List *strings);

static void on_proc_exit_cb(int code, Datum arg);

static char *get_class_name(jclass class);

Datum scalardb_fdw_get_jar_file_path(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(scalardb_fdw_get_jar_file_path);

#define register_java_class(jclass_ref, class_fqdn)                       \
	{                                                                 \
		jclass class = (*env)->FindClass(env, (class_fqdn));      \
		if (class == NULL) {                                      \
			ereport(ERROR,                                    \
				errmsg("%s is not found", (class_fqdn))); \
		}                                                         \
		jclass_ref = (jclass)((*env)->NewGlobalRef(env, class));  \
	}

#define register_java_class_method(jmethod_ref, jclass_ref, name, sig)      \
	jmethod_ref = (*env)->GetMethodID(env, jclass_ref, (name), (sig));  \
	if (jmethod_ref == NULL) {                                          \
		ereport(ERROR, errmsg("%s.%s is not found",                 \
				      get_class_name(jclass_ref), (name))); \
	}

#define register_java_static_method(jmethod_ref, jclass_ref, name, sig)     \
	jmethod_ref =                                                       \
		(*env)->GetStaticMethodID(env, jclass_ref, (name), (sig));  \
	if (jmethod_ref == NULL) {                                          \
		ereport(ERROR, errmsg("%s.%s is not found",                 \
				      get_class_name(jclass_ref), (name))); \
	}

void scalardb_initialize(ScalarDbFdwOptions *opts)
{
	static bool already_initialized = false;
	jstring config_file_path;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	if (already_initialized == true) {
		ereport(DEBUG3,
			errmsg("scalardb has already been initialized"));
		return;
	}

	initialize_jvm(opts);
	initialize_standard_references();
	add_classpath_to_system_class_loader(STR_SCALARDB_JAR_PATH);
	initialize_scalardb_references();

	// TODO: skip after second call
	config_file_path = (*env)->NewStringUTF(env, opts->config_file_path);
	clear_exception();
	(*env)->CallStaticObjectMethod(env, ScalarDbUtils_class,
				       ScalarDbUtils_initialize,
				       config_file_path);
	catch_exception();

	already_initialized = true;
}

/*
 * Retruns Scan (all) object built with the specified parameters.
 *
 * The returned object is a global reference. It is caller's responsibility to
 * release the object 
 *
 * If `attnames` is specified, only the columns with the names in `attnames`
 * will be returned. (i.e. calls projections())
 * The type of `attnames` must be a List of String.
 */
extern jobject scalardb_scan_all(char *namespace, char *table_name,
				 List *attnames)
{
	jstring namespace_str;
	jstring table_name_str;
	jobject buildable_scan;
	jobject scan;

	ereport(DEBUG5, errmsg("entering function %s", __func__));

	namespace_str = (*env)->NewStringUTF(env, namespace);
	table_name_str = (*env)->NewStringUTF(env, table_name);

	buildable_scan = (*env)->CallStaticObjectMethod(
		env, ScalarDbUtils_class, ScalarDbUtils_buildableScanAll,
		namespace_str, table_name_str);

	apply_column_pruning(buildable_scan, attnames,
			     BuildableScanAll_projections);

	scan = (*env)->CallObjectMethod(env, buildable_scan,
					BuildableScanAll_build);
	scan = (*env)->NewGlobalRef(env, scan);
	return scan;
}

/*
 * Retruns Scan (partitionKey) object built with the specified parameters.
 *
 * The returned object is a global reference. It is caller's responsibility to
 * release the object 
 *
 * If `attnames` is specified, only the columns with the names in `attnames`
 * will be returned. (i.e. calls projections())
 * The type of `attnames` must be a List of String.
 */
extern jobject scalardb_scan(char *namespace, char *table_name, List *attnames,
			     ScalarDbFdwScanCondition *scan_conds,
			     size_t num_scan_conds,
			     ScalarDbFdwScanBoundary *boundary)
{
	jstring namespace_str;
	jstring table_name_str;
	jobject buildable_scan;
	jobject scan;
	jobject key;

	ereport(DEBUG5, errmsg("entering function %s", __func__));

	namespace_str = (*env)->NewStringUTF(env, namespace);
	table_name_str = (*env)->NewStringUTF(env, table_name);

	key = get_key_from_conds(scan_conds, num_scan_conds);

	buildable_scan = (*env)->CallStaticObjectMethod(
		env, ScalarDbUtils_class, ScalarDbUtils_buildableScan,
		namespace_str, table_name_str, key);

	apply_column_pruning(buildable_scan, attnames,
			     BuildableScan_projections);

	apply_clustering_key_boundary(buildable_scan, boundary);

	scan = (*env)->CallObjectMethod(env, buildable_scan,
					BuildableScan_build);
	scan = (*env)->NewGlobalRef(env, scan);

	ereport(DEBUG5, errmsg("scan %s", scalardb_to_string(scan)));

	return scan;
}

/*
 * Retruns Scan (indexKey) object built with the specified parameters.
 *
 * The returned object is a global reference. It is caller's responsibility to
 * release the object 
 *
 * If `attnames` is specified, only the columns with the names in `attnames`
 * will be returned. (i.e. calls projections())
 * The type of `attnames` must be a List of String.
 */
extern jobject scalardb_scan_with_index(char *namespace, char *table_name,
					List *attnames,
					ScalarDbFdwScanCondition *scan_conds,
					size_t num_scan_conds)
{
	jstring namespace_str;
	jstring table_name_str;
	jobject buildable_scan;
	jobject scan;
	jobject key;

	ereport(DEBUG5, errmsg("entering function %s", __func__));

	namespace_str = (*env)->NewStringUTF(env, namespace);
	table_name_str = (*env)->NewStringUTF(env, table_name);

	key = get_key_from_conds(scan_conds, num_scan_conds);

	buildable_scan = (*env)->CallStaticObjectMethod(
		env, ScalarDbUtils_class, ScalarDbUtils_buildableScanWithIndex,
		namespace_str, table_name_str, key);

	apply_column_pruning(buildable_scan, attnames,
			     BuildableScanWithIndex_projections);

	scan = (*env)->CallObjectMethod(env, buildable_scan,
					BuildableScanWithIndex_build);
	scan = (*env)->NewGlobalRef(env, scan);

	ereport(DEBUG5, errmsg("scan %s", scalardb_to_string(scan)));

	return scan;
}

static jobject get_key_from_conds(ScalarDbFdwScanCondition *scan_conds,
				  size_t num_scan_conds)
{
	jobject key_builder;

	key_builder = (*env)->CallStaticObjectMethod(env, ScalarDbUtils_class,
						     ScalarDbUtils_keyBuilder);

	for (int i = 0; i < num_scan_conds; i++) {
		ScalarDbFdwScanCondition *cond = &scan_conds[i];
		jstring key_name_str;

		key_name_str = (*env)->NewStringUTF(env, cond->name);
		add_datum_value_to_key(key_builder, key_name_str, cond->value,
				       cond->value_type);
	}

	return (*env)->CallObjectMethod(env, key_builder, KeyBuilder_build);
}

static jobject get_key_for_boundary(List *names, Datum *values,
				    List *value_types, size_t num_values)
{
	jobject key_builder;

	key_builder = (*env)->CallStaticObjectMethod(env, ScalarDbUtils_class,
						     ScalarDbUtils_keyBuilder);
	for (size_t i = 0; i < num_values; i++) {
		jstring name_str =
			(*env)->NewStringUTF(env, strVal(list_nth(names, i)));
		Datum value = values[i];
		Oid type = list_nth_oid(value_types, i);

		add_datum_value_to_key(key_builder, name_str, value, type);
	}
	return (*env)->CallObjectMethod(env, key_builder, KeyBuilder_build);
}

static void add_datum_value_to_key(jobject key_builder, jstring name,
				   Datum value, Oid value_type)
{
	switch (value_type) {
	case BOOLOID: {
		key_builder = (*env)->CallObjectMethod(env, key_builder,
						       KeyBuilder_addBoolean,
						       name,
						       DatumGetBool(value));
		break;
	}
	case INT4OID: {
		key_builder = (*env)->CallObjectMethod(env, key_builder,
						       KeyBuilder_addInt, name,
						       DatumGetInt32(value));
		break;
	}
	case INT8OID: {
		key_builder = (*env)->CallObjectMethod(env, key_builder,
						       KeyBuilder_addBigInt,
						       name,
						       DatumGetInt64(value));
		break;
	}
	case FLOAT4OID: {
		key_builder = (*env)->CallObjectMethod(env, key_builder,
						       KeyBuilder_addFloat,
						       name,
						       DatumGetFloat4(value));
		break;
	}
	case FLOAT8OID: {
		key_builder = (*env)->CallObjectMethod(env, key_builder,
						       KeyBuilder_addDouble,
						       name,
						       DatumGetFloat8(value));
		break;
	}
	case TEXTOID: {
		jstring value_str =
			(*env)->NewStringUTF(env, TextDatumGetCString(value));
		key_builder = (*env)->CallObjectMethod(
			env, key_builder, KeyBuilder_addText, name, value_str);
		(*env)->DeleteLocalRef(env, value_str);
		break;
	}
	case BYTEAOID: {
		bytea *unpacked = pg_detoast_datum_packed(
			(bytea *)(DatumGetPointer(value)));
		int len = VARSIZE_ANY_EXHDR(unpacked);

		jbyteArray value_ary = (*env)->NewByteArray(env, len);
		(*env)->SetByteArrayRegion(env, value_ary, 0, len,
					   (jbyte *)VARDATA_ANY(unpacked));
		if (unpacked != (bytea *)value)
			pfree(unpacked);

		key_builder = (*env)->CallObjectMethod(
			env, key_builder, KeyBuilder_addBlob, name, value_ary);

		(*env)->DeleteLocalRef(env, value_ary);
		break;
	}
	default:
		ereport(ERROR, errmsg("Unsupported data type: %d", value_type));
	}
}
static void apply_column_pruning(jobject buildable_scan, List *attnames,
				 jmethodID projections_method)
{
	if (attnames != NIL) {
		jobjectArray attrnames_array =
			convert_string_list_to_jarray_of_string(attnames);
		buildable_scan = (*env)->CallObjectMethod(env, buildable_scan,
							  projections_method,
							  attrnames_array);
	}
}

static void apply_clustering_key_boundary(jobject buildable_scan,
					  ScalarDbFdwScanBoundary *boundary)
{
	if (boundary->num_start_values > 0) {
		jobject key = get_key_for_boundary(boundary->names,
						   boundary->start_values,
						   boundary->start_value_types,
						   boundary->num_start_values);
		buildable_scan = (*env)->CallObjectMethod(
			env, buildable_scan, BuildableScan_start, key,
			boundary->start_inclusive);
	}

	if (boundary->num_end_values > 0) {
		jobject key = get_key_for_boundary(boundary->names,
						   boundary->end_values,
						   boundary->end_value_types,
						   boundary->num_end_values);
		ereport(DEBUG5, errmsg("key %s", scalardb_to_string(key)));
		ereport(DEBUG5, errmsg("key %s", scalardb_to_string(key)));
		buildable_scan = (*env)->CallObjectMethod(
			env, buildable_scan, BuildableScan_end, key,
			boundary->end_inclusive);
	}
}

/*
 * Release the specified Scan object.
 */
extern void scalardb_release_scan(jobject scan)
{
	(*env)->DeleteGlobalRef(env, scan);
}

/*
 * Returns Scanner object started from the specified Scan object.
 */
extern jobject scalardb_start_scan(jobject scan)
{
	jobject scanner;
	clear_exception();
	scanner = (*env)->CallStaticObjectMethod(env, ScalarDbUtils_class,
						 ScalarDbUtils_scan, scan);
	catch_exception();
	return scanner;
}

extern jobject scalardb_scanner_one(jobject scanner)
{
	jobject o;

	ereport(DEBUG5, errmsg("entering function %s", __func__));

	clear_exception();
	(*env)->PushLocalFrame(env, LOCAL_FRAME_CAPACITY);
	catch_exception();

	clear_exception();
	o = (*env)->CallObjectMethod(env, scanner, Scanner_one);
	catch_exception();
	return o;
}

/*
 * Release all object references created from Result object by popping the
 * current local references in JVM.
 * It is caller's responsibility to ensure that the local frame for the Result
 * object has been created in scalardb_scanner_one in ahead
 */
extern void scalardb_scanner_release_result()
{
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	(*env)->PopLocalFrame(env, NULL);
}

extern void scalardb_scanner_close(jobject scanner)
{
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	clear_exception();
	(*env)->CallVoidMethod(env, scanner, Closeable_close);
	catch_exception();

	(*env)->DeleteLocalRef(env, scanner);
}

extern int scalardb_list_size(jobject list)
{
	jint size;
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	size = (*env)->CallIntMethod(env, list, List_size);
	return (int)size;
}

extern jobject scalardb_list_iterator(jobject list)
{
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	return (*env)->CallObjectMethod(env, list, List_iterator);
}

extern bool scalardb_iterator_has_next(jobject iterator)
{
	jboolean b;
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	b = (*env)->CallBooleanMethod(env, iterator, Iterator_hasNext);
	return b == JNI_TRUE;
}

extern bool scalardb_optional_is_present(jobject optional)
{
	jboolean b;
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	b = (*env)->CallBooleanMethod(env, optional, Optional_isPresent);
	return b == JNI_TRUE;
}

extern jobject scalardb_optional_get(jobject optional)
{
	jobject o;
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	clear_exception();
	o = (*env)->CallObjectMethod(env, optional, Optional_get);
	catch_exception();
	return o;
}

extern jobject scalardb_iterator_next(jobject iterator)
{
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	return (*env)->CallObjectMethod(env, iterator, Iterator_next);
}

extern bool scalardb_result_is_null(jobject result, char *attname)
{
	jstring attname_str;
	jboolean b;
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	attname_str = (*env)->NewStringUTF(env, attname);
	b = (*env)->CallBooleanMethod(env, result, Result_isNull, attname_str);
	return b == JNI_TRUE;
}

extern bool scalardb_result_get_boolean(jobject result, char *attname)
{
	jstring attname_str;
	jboolean b;
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	attname_str = (*env)->NewStringUTF(env, attname);
	b = (*env)->CallBooleanMethod(env, result, Result_getBoolean,
				      attname_str);
	return b == JNI_TRUE;
}

extern int scalardb_result_get_int(jobject result, char *attname)
{
	jstring attname_str;
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	attname_str = (*env)->NewStringUTF(env, attname);
	return (int)(*env)->CallIntMethod(env, result, Result_getInt,
					  attname_str);
}

extern long scalardb_result_get_bigint(jobject result, char *attname)
{
	jstring attname_str;
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	attname_str = (*env)->NewStringUTF(env, attname);
	return (long)(*env)->CallLongMethod(env, result, Result_getBigInt,
					    attname_str);
}

extern float scalardb_result_get_float(jobject result, char *attname)
{
	jstring attname_str;
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	attname_str = (*env)->NewStringUTF(env, attname);
	return (float)(*env)->CallFloatMethod(env, result, Result_getFloat,
					      attname_str);
}

extern double scalardb_result_get_double(jobject result, char *attname)
{
	jstring attname_str;
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	attname_str = (*env)->NewStringUTF(env, attname);
	return (double)(*env)->CallDoubleMethod(env, result, Result_getDouble,
						attname_str);
}

extern text *scalardb_result_get_text(jobject result, char *attname)
{
	jstring attname_str;
	jstring str;
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	attname_str = (*env)->NewStringUTF(env, attname);
	str = (*env)->CallObjectMethod(env, result, Result_getText,
				       attname_str);
	return convert_string_to_text(str);
}

extern bytea *scalardb_result_get_blob(jobject result, char *attname)
{
	jstring attname_str;
	jbyteArray bytes;
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	attname_str = (*env)->NewStringUTF(env, attname);
	bytes = (jbyteArray)(*env)->CallObjectMethod(
		env, result, Result_getBlobAsBytes, attname_str);
	return convert_jbyteArray_to_bytea(bytes);
}

extern int scalardb_result_columns_size(jobject result)
{
	ereport(DEBUG5, errmsg("entering function %s", __func__));
	return (int)(*env)->CallStaticIntMethod(
		env, ScalarDbUtils_class, ScalarDbUtils_getResultColumnsSize,
		result);
}

/*
 * Returns a string representation of the given object by calling its toString()
 */
extern char *scalardb_to_string(jobject obj)
{
	jstring str =
		(jstring)(*env)->CallObjectMethod(env, obj, Object_toString);
	char *cstr = convert_string_to_cstring(str);
	return cstr;
}

/*
 * Retrieve the partition key names for the given table.
 */
extern void scalardb_get_paritition_key_names(char *namespace, char *table_name,
					      List **partition_key_names)
{
	jstring namespace_str;
	jstring table_name_str;

	jsize len;
	jstring elem;

	jobjectArray partition_key_names_array;

	ereport(DEBUG5, errmsg("entering function %s", __func__));

	Assert(metadata->partition_key_names == NIL);

	namespace_str = (*env)->NewStringUTF(env, namespace);
	table_name_str = (*env)->NewStringUTF(env, table_name);

	clear_exception();
	partition_key_names_array = (*env)->CallStaticObjectMethod(
		env, ScalarDbUtils_class, ScalarDbUtils_getPartitionKeyNames,
		namespace_str, table_name_str);
	catch_exception();

	len = (*env)->GetArrayLength(env, partition_key_names_array);
	for (jsize i = 0; i < len; i++) {
		elem = (jstring)(*env)->GetObjectArrayElement(
			env, partition_key_names_array, i);
		*partition_key_names =
			lappend(*partition_key_names,
				makeString(convert_string_to_cstring(elem)));
	}
}

/*
 * Retrieve the clustering key names for the given table.
 */
extern void scalardb_get_clustering_key_names(char *namespace, char *table_name,
					      List **clustering_key_names)
{
	jstring namespace_str;
	jstring table_name_str;

	jsize len;
	jstring elem;

	jobjectArray clustering_key_names_array;

	ereport(DEBUG5, errmsg("entering function %s", __func__));

	Assert(metadata->clustering_key_names == NIL);

	namespace_str = (*env)->NewStringUTF(env, namespace);
	table_name_str = (*env)->NewStringUTF(env, table_name);

	clear_exception();
	clustering_key_names_array = (*env)->CallStaticObjectMethod(
		env, ScalarDbUtils_class, ScalarDbUtils_getClusteringKeyNames,
		namespace_str, table_name_str);
	catch_exception();

	len = (*env)->GetArrayLength(env, clustering_key_names_array);
	for (jsize i = 0; i < len; i++) {
		elem = (jstring)(*env)->GetObjectArrayElement(
			env, clustering_key_names_array, i);
		*clustering_key_names =
			lappend(*clustering_key_names,
				makeString(convert_string_to_cstring(elem)));
	}
}

/*
 * Retrieve the secondary index names for the given table.
 */
extern void scalardb_get_secondary_index_names(char *namespace,
					       char *table_name,
					       List **secondary_index_names)
{
	jstring namespace_str;
	jstring table_name_str;

	jsize len;
	jstring elem;

	jobjectArray secondary_index_names_array;

	ereport(DEBUG5, errmsg("entering function %s", __func__));

	Assert(metadata->secondary_index_names == NIL);

	namespace_str = (*env)->NewStringUTF(env, namespace);
	table_name_str = (*env)->NewStringUTF(env, table_name);

	clear_exception();
	secondary_index_names_array = (*env)->CallStaticObjectMethod(
		env, ScalarDbUtils_class, ScalarDbUtils_getSecondaryIndexNames,
		namespace_str, table_name_str);
	catch_exception();

	len = (*env)->GetArrayLength(env, secondary_index_names_array);
	for (jsize i = 0; i < len; i++) {
		elem = (jstring)(*env)->GetObjectArrayElement(
			env, secondary_index_names_array, i);
		*secondary_index_names =
			lappend(*secondary_index_names,
				makeString(convert_string_to_cstring(elem)));
	}
}

static void initialize_jvm(ScalarDbFdwOptions *opts)
{
	char *max_heap_size;
	size_t max_heap_size_option_len;
	char *max_heap_size_option;
	JavaVMOption *options;
	JavaVMInitArgs vm_args;
	jint res;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	max_heap_size = opts->max_heap_size ? opts->max_heap_size :
					      DEFAULT_MAX_HEAP_SIZE;
	max_heap_size_option_len =
		MAX_HEAP_SIZE_STR_LEN + strlen(max_heap_size) + NULL_STR_LEN;
	max_heap_size_option = (char *)palloc0(max_heap_size_option_len);
	snprintf(max_heap_size_option, max_heap_size_option_len, "-Xmx%s",
		 max_heap_size);

	options = (JavaVMOption *)palloc0(sizeof(JavaVMOption) * 1);
	options[0].optionString = max_heap_size_option;

	vm_args.nOptions = 1;
	vm_args.version = JNI_VERSION;
	vm_args.options = options;
	vm_args.ignoreUnrecognized = JNI_FALSE;

	res = JNI_CreateJavaVM(&jvm, (void **)&env, &vm_args);
	if (res == 0) {
		ereport(DEBUG3,
			errmsg("Successfully created a JVM with %s heapsize",
			       max_heap_size));
	} else if (res == JNI_EEXIST) {
		ereport(DEBUG3, errmsg("Java VM has already been created. "
				       "Re-use the existing Java VM."));
		res = JNI_GetCreatedJavaVMs(&jvm, 1, NULL);
		if (res < 0) {
			ereport(ERROR,
				errmsg("Failed to get created Java VM. JNI error code %d",
				       res));
		}
		get_jni_env();
	} else if (res < 0) {
		ereport(ERROR,
			errmsg("Failed to create Java VM. JNI error code: %d",
			       res));
	}
	on_proc_exit(on_proc_exit_cb, 0);
}

static void destroy_jvm()
{
	ereport(DEBUG3, errmsg("entering function %s", __func__));
	(*jvm)->DestroyJavaVM(jvm);
}

static void attach_jvm()
{
	ereport(DEBUG3, errmsg("entering function %s", __func__));
	(*jvm)->AttachCurrentThread(jvm, (void **)&env, NULL);
}

static void get_jni_env()
{
	jint GetEnvStat = (*jvm)->GetEnv(jvm, (void **)&env, JNI_VERSION);
	if (GetEnvStat == JNI_EDETACHED) {
		ereport(DEBUG3, errmsg("GetEnv: JNI_EDETACHED; the current "
				       "thread is not attached to the VM"));
		attach_jvm();
	} else if (GetEnvStat == JNI_OK) {
		ereport(DEBUG3, errmsg("GetEnv: JNI_OK"));
	} else if (GetEnvStat == JNI_EVERSION) {
		ereport(ERROR, errmsg("GetEnv: JNI_EVERSION; the specified "
				      "version is not supported"));
	} else {
		ereport(ERROR,
			errmsg("GetEnv returned unknown code: %d", GetEnvStat));
	}
}

static void initialize_standard_references()
{
	ereport(DEBUG3, errmsg("entering function %s", __func__));

	// java.lang.Object
	register_java_class(Object_class, "java/lang/Object");
	register_java_class_method(Object_toString, Object_class, "toString",
				   "()Ljava/lang/String;");

	// java.lang.String
	register_java_class(String_class, "java/lang/String");

	// java.util.List
	register_java_class(List_class, "java/util/List");
	register_java_class_method(List_size, List_class, "size", "()I");
	register_java_class_method(List_iterator, List_class, "iterator",
				   "()Ljava/util/Iterator;");
	// java.util.Iterator
	register_java_class(Iterator_class, "java/util/Iterator");
	register_java_class_method(Iterator_hasNext, Iterator_class, "hasNext",
				   "()Z");
	register_java_class_method(Iterator_next, Iterator_class, "next",
				   "()Ljava/lang/Object;");

	// java.util.Optional
	register_java_class(Optional_class, "java/util/Optional");
	register_java_class_method(Optional_isPresent, Optional_class,
				   "isPresent", "()Z");
	register_java_class_method(Optional_get, Optional_class, "get",
				   "()Ljava/lang/Object;");

	// java.io.Closeable
	register_java_class(Closeable_class, "java/io/Closeable");
	register_java_class_method(Closeable_close, Closeable_class, "close",
				   "()V");
}

static void initialize_scalardb_references()
{
	ereport(DEBUG3, errmsg("entering function %s", __func__));

	// ScalarDbUtils
	register_java_class(ScalarDbUtils_class,
			    "com/scalar/db/analytics/postgresql/ScalarDbUtils");
	register_java_static_method(ScalarDbUtils_initialize,
				    ScalarDbUtils_class, "initialize",
				    "(Ljava/lang/String;)V");
	register_java_static_method(ScalarDbUtils_closeStorage,
				    ScalarDbUtils_class, "closeStorage", "()V");
	register_java_static_method(
		ScalarDbUtils_scan, ScalarDbUtils_class, "scan",
		"(Lcom/scalar/db/api/Scan;)Lcom/scalar/db/api/Scanner;");
	register_java_static_method(
		ScalarDbUtils_buildableScan, ScalarDbUtils_class,
		"buildableScan",
		"(Ljava/lang/String;Ljava/lang/String;Lcom/scalar/db/io/Key;)Lcom/scalar/db/api/ScanBuilder$BuildableScan;");
	register_java_static_method(
		ScalarDbUtils_buildableScanWithIndex, ScalarDbUtils_class,
		"buildableScanWithIndex",
		"(Ljava/lang/String;Ljava/lang/String;Lcom/scalar/db/io/Key;)Lcom/scalar/db/api/ScanBuilder$BuildableScanWithIndex;");
	register_java_static_method(
		ScalarDbUtils_buildableScanAll, ScalarDbUtils_class,
		"buildableScanAll",
		"(Ljava/lang/String;Ljava/lang/String;)Lcom/scalar/db/api/ScanBuilder$BuildableScanAll;");
	register_java_static_method(ScalarDbUtils_keyBuilder,
				    ScalarDbUtils_class, "keyBuilder",
				    "()Lcom/scalar/db/io/Key$Builder;");
	register_java_static_method(ScalarDbUtils_getResultColumnsSize,
				    ScalarDbUtils_class, "getResultColumnsSize",
				    "(Lcom/scalar/db/api/Result;)I");
	register_java_static_method(
		ScalarDbUtils_getPartitionKeyNames, ScalarDbUtils_class,
		"getPartitionKeyNames",
		"(Ljava/lang/String;Ljava/lang/String;)[Ljava/lang/String;");
	register_java_static_method(
		ScalarDbUtils_getClusteringKeyNames, ScalarDbUtils_class,
		"getClusteringKeyNames",
		"(Ljava/lang/String;Ljava/lang/String;)[Ljava/lang/String;");
	register_java_static_method(
		ScalarDbUtils_getSecondaryIndexNames, ScalarDbUtils_class,
		"getSecondaryIndexNames",
		"(Ljava/lang/String;Ljava/lang/String;)[Ljava/lang/String;");

	// com.scalar.db.api.Result
	register_java_class(Result_class, "com/scalar/db/api/Result");
	register_java_class_method(Result_isNull, Result_class, "isNull",
				   "(Ljava/lang/String;)Z");
	register_java_class_method(Result_getBoolean, Result_class,
				   "getBoolean", "(Ljava/lang/String;)Z");
	register_java_class_method(Result_getInt, Result_class, "getInt",
				   "(Ljava/lang/String;)I");
	register_java_class_method(Result_getBigInt, Result_class, "getBigInt",
				   "(Ljava/lang/String;)J");
	register_java_class_method(Result_getFloat, Result_class, "getFloat",
				   "(Ljava/lang/String;)F");
	register_java_class_method(Result_getDouble, Result_class, "getDouble",
				   "(Ljava/lang/String;)D");
	register_java_class_method(Result_getText, Result_class, "getText",
				   "(Ljava/lang/String;)Ljava/lang/String;");
	register_java_class_method(Result_getBlobAsBytes, Result_class,
				   "getBlobAsBytes", "(Ljava/lang/String;)[B");

	// com.scalar.db.api.Scanner
	register_java_class(Scanner_class, "com/scalar/db/api/Scanner");
	register_java_class_method(Scanner_one, Scanner_class, "one",
				   "()Ljava/util/Optional;");

	// com.scalar.db.api.ScanBuilder$BuildableScan
	register_java_class(BuildableScan_class,
			    "Lcom/scalar/db/api/ScanBuilder$BuildableScan;");
	register_java_class_method(
		BuildableScan_projections, BuildableScan_class, "projections",
		"([Ljava/lang/String;)Lcom/scalar/db/api/ScanBuilder$BuildableScan;");
	register_java_class_method(
		BuildableScan_start, BuildableScan_class, "start",
		"(Lcom/scalar/db/io/Key;Z)Lcom/scalar/db/api/ScanBuilder$BuildableScan;");
	register_java_class_method(
		BuildableScan_end, BuildableScan_class, "end",
		"(Lcom/scalar/db/io/Key;Z)Lcom/scalar/db/api/ScanBuilder$BuildableScan;");
	register_java_class_method(BuildableScan_build, BuildableScan_class,
				   "build", "()Lcom/scalar/db/api/Scan;");

	// com.scalar.db.api.ScanBuilder$BuildableScanWithIndex
	register_java_class(
		BuildableScanWithIndex_class,
		"Lcom/scalar/db/api/ScanBuilder$BuildableScanWithIndex;");
	register_java_class_method(
		BuildableScanWithIndex_projections,
		BuildableScanWithIndex_class, "projections",
		"([Ljava/lang/String;)Lcom/scalar/db/api/ScanBuilder$BuildableScanWithIndex;");
	register_java_class_method(BuildableScanWithIndex_build,
				   BuildableScanWithIndex_class, "build",
				   "()Lcom/scalar/db/api/Scan;");

	// com.scalar.db.api.ScanBuilder$BuildableScanAll
	register_java_class(BuildableScanAll_class,
			    "Lcom/scalar/db/api/ScanBuilder$BuildableScanAll;");
	register_java_class_method(
		BuildableScanAll_projections, BuildableScanAll_class,
		"projections",
		"([Ljava/lang/String;)Lcom/scalar/db/api/ScanBuilder$BuildableScanAll;");
	register_java_class_method(BuildableScanAll_build,
				   BuildableScanAll_class, "build",
				   "()Lcom/scalar/db/api/Scan;");

	// com.scalar.db.io.Key$Builder
	register_java_class(KeyBuilder_class, "Lcom/scalar/db/io/Key$Builder;");
	register_java_class_method(
		KeyBuilder_addBoolean, KeyBuilder_class, "addBoolean",
		"(Ljava/lang/String;Z)Lcom/scalar/db/io/Key$Builder;");
	register_java_class_method(
		KeyBuilder_addInt, KeyBuilder_class, "addInt",
		"(Ljava/lang/String;I)Lcom/scalar/db/io/Key$Builder;");
	register_java_class_method(
		KeyBuilder_addBigInt, KeyBuilder_class, "addBigInt",
		"(Ljava/lang/String;J)Lcom/scalar/db/io/Key$Builder;");
	register_java_class_method(
		KeyBuilder_addFloat, KeyBuilder_class, "addFloat",
		"(Ljava/lang/String;F)Lcom/scalar/db/io/Key$Builder;");
	register_java_class_method(
		KeyBuilder_addDouble, KeyBuilder_class, "addDouble",
		"(Ljava/lang/String;D)Lcom/scalar/db/io/Key$Builder;");
	register_java_class_method(
		KeyBuilder_addText, KeyBuilder_class, "addText",
		"(Ljava/lang/String;Ljava/lang/String;)Lcom/scalar/db/io/Key$Builder;");
	register_java_class_method(
		KeyBuilder_addBlob, KeyBuilder_class, "addBlob",
		"(Ljava/lang/String;[B)Lcom/scalar/db/io/Key$Builder;");
	register_java_class_method(KeyBuilder_build, KeyBuilder_class, "build",
				   "()Lcom/scalar/db/io/Key;");
}

static void add_classpath_to_system_class_loader(char *classpath)
{
	int url_classpath_len;
	char *url_classpath;
	jclass ClassLoader_class;
	jmethodID ClassLoader_getSystemClassLoader;
	jobject system_class_loader;
	jclass URLClassLoader_class;
	jmethodID URLClassLoader_addURL;
	jclass URL_class;
	jmethodID URL_constructor;
	jobject url;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	url_classpath_len =
		FILE_URL_PREFIX_LEN + strlen(classpath) + NULL_STR_LEN;
	url_classpath = (char *)palloc0(url_classpath_len);
	snprintf(url_classpath, url_classpath_len, "file:%s", classpath);

	ereport(DEBUG3, errmsg("Add classpath to System Class Loader: %s",
			       url_classpath));

	register_java_class(ClassLoader_class, "java/lang/ClassLoader");
	register_java_static_method(ClassLoader_getSystemClassLoader,
				    ClassLoader_class, "getSystemClassLoader",
				    "()Ljava/lang/ClassLoader;");
	register_java_class(URLClassLoader_class, "java/net/URLClassLoader");
	register_java_class_method(URLClassLoader_addURL, URLClassLoader_class,
				   "addURL", "(Ljava/net/URL;)V");
	register_java_class(URL_class, "java/net/URL");
	register_java_class_method(URL_constructor, URL_class, "<init>",
				   "(Ljava/lang/String;)V");

	clear_exception();
	system_class_loader = (*env)->CallStaticObjectMethod(
		env, ClassLoader_class, ClassLoader_getSystemClassLoader);
	catch_exception();

	clear_exception();
	url = (*env)->NewObject(env, URL_class, URL_constructor,
				(*env)->NewStringUTF(env, url_classpath));
	catch_exception();

	clear_exception();
	(*env)->CallVoidMethod(env, system_class_loader, URLClassLoader_addURL,
			       url);
	catch_exception();

	(*env)->DeleteGlobalRef(env, ClassLoader_class);
	(*env)->DeleteGlobalRef(env, URLClassLoader_class);
	(*env)->DeleteGlobalRef(env, URL_class);
}

static void clear_exception()
{
	(*env)->ExceptionClear(env);
}

static void catch_exception()
{
	if ((*env)->ExceptionCheck(env)) {
		jthrowable exc = (*env)->ExceptionOccurred(env);
		char *msg = scalardb_to_string(exc);
		ereport(ERROR, errcode(ERRCODE_FDW_ERROR),
			errmsg("Exception occurred in JVM: %s", msg));
	}
	return;
}

static char *convert_string_to_cstring(jstring java_string)
{
	char *ret = NULL;

	if (!((*env)->IsInstanceOf(env, java_string, String_class))) {
		ereport(ERROR, errmsg("Not an instance of String class"));
	}

	if (java_string != NULL) {
		const char *str = (char *)(*env)->GetStringUTFChars(
			env, (jstring)java_string, 0);
		ret = pstrdup(str);
		(*env)->ReleaseStringUTFChars(env, (jstring)java_string, str);
		(*env)->DeleteLocalRef(env, java_string);
	}
	return ret;
}

static text *convert_string_to_text(jstring java_string)
{
	text *ret = NULL;

	if (!((*env)->IsInstanceOf(env, java_string, String_class))) {
		ereport(ERROR, errmsg("Not an instance of String class"));
	}

	if (java_string != NULL) {
		const char *str = (char *)(*env)->GetStringUTFChars(
			env, (jstring)java_string, 0);

		jsize nbytes = (*env)->GetStringUTFLength(env, java_string);

		ret = (bytea *)palloc(nbytes + VARHDRSZ);
		SET_VARSIZE(ret, nbytes + VARHDRSZ);
		memcpy(VARDATA(ret), str, nbytes);

		(*env)->ReleaseStringUTFChars(env, (jstring)java_string, str);
		(*env)->DeleteLocalRef(env, java_string);
	}
	return ret;
}

static bytea *convert_jbyteArray_to_bytea(jbyteArray bytes)
{
	bytea *ret = NULL;

	if (bytes != NULL) {
		jbyte *elems = (*env)->GetByteArrayElements(env, bytes, 0);

		jsize len = (*env)->GetArrayLength(env, bytes);
		size_t nbytes = sizeof(jbyte) * len;

		ret = (bytea *)palloc(nbytes + VARHDRSZ);
		SET_VARSIZE(ret, nbytes + VARHDRSZ);
		memcpy(VARDATA(ret), elems, nbytes);

		(*env)->ReleaseByteArrayElements(env, bytes, elems, JNI_ABORT);
	}
	return ret;
}

/*
* Convert a list of strings to a Java String array.
* The type of strs must be a List of String;
*/
static jobjectArray convert_string_list_to_jarray_of_string(List *strs)
{
	jobjectArray ret = (*env)->NewObjectArray(env, list_length(strs),
						  String_class, NULL);

	for (int i = 0; i < list_length(strs); i++) {
		jstring str =
			(*env)->NewStringUTF(env, strVal(list_nth(strs, i)));
		(*env)->SetObjectArrayElement(env, ret, i, str);
		(*env)->DeleteLocalRef(env, str);
	}
	return ret;
}

static void on_proc_exit_cb(int code, Datum arg)
{
	(*env)->CallStaticObjectMethod(env, ScalarDbUtils_class,
				       ScalarDbUtils_closeStorage);
	destroy_jvm();
}

static char *get_class_name(jclass class)
{
	jclass cls = (*env)->FindClass(env, "java/lang/Class");
	jmethodID getName = (*env)->GetMethodID(env, cls, "getName",
						"()Ljava/lang/String;");
	jstring name = (*env)->CallObjectMethod(env, class, getName);
	return convert_string_to_cstring(name);
}

Datum scalardb_fdw_get_jar_file_path(PG_FUNCTION_ARGS)
{
	return CStringGetTextDatum(STR_SCALARDB_JAR_PATH);
}
