#include "scalardb.h"

#include "jni.h"
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
static jmethodID ScalarDbUtils_scanAll;
static jmethodID ScalarDbUtils_getResultColumnsSize;

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

static void initialize_jvm(ScalarDbFdwOptions *opts);
static void destroy_jvm(void);
static void attach_jvm(void);
static void get_jni_env(void);
static void initialize_standard_references(void);
static void initialize_scalardb_references(void);
static void add_classpath_to_system_class_loader(char *classpath);

static void clear_exception(void);
static void catch_exception(void);

static char *convert_string_to_cstring(jobject java_cstring);
static text *convert_string_to_text(jobject java_cstring);
static bytea *convert_jbyteArray_to_bytea(jbyteArray bytes);

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

extern jobject scalardb_scan_all(char *namespace, char *table_name)
{
	jstring namespace_str;
	jstring table_name_str;
	jobject scanner;

	ereport(DEBUG5, errmsg("entering function %s", __func__));

	namespace_str = (*env)->NewStringUTF(env, namespace);
	table_name_str = (*env)->NewStringUTF(env, table_name);
	clear_exception();
	scanner = (*env)->CallStaticObjectMethod(env, ScalarDbUtils_class,
						 ScalarDbUtils_scanAll,
						 namespace_str, table_name_str);
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
		ScalarDbUtils_scanAll, ScalarDbUtils_class, "scanAll",
		"(Ljava/lang/String;Ljava/lang/String;)Lcom/scalar/db/api/Scanner;");
	register_java_static_method(ScalarDbUtils_getResultColumnsSize,
				    ScalarDbUtils_class, "getResultColumnsSize",
				    "(Lcom/scalar/db/api/Result;)I");

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

		jstring exception_message = (jstring)(*env)->CallObjectMethod(
			env, exc, Object_toString);
		char *msg = convert_string_to_cstring(exception_message);

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
