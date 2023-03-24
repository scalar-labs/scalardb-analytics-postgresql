#include "scalardb.h"

#include "storage/ipc.h"
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

#define DEFAULT_MAX_HEAP_SIZE "1g"

#define LOCAL_FRAME_CAPACITY 128

static __thread JNIEnv* env = NULL;
static JavaVM* jvm;

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

static void initialize_jvm(ScalarDbFdwOptions* opts);
static void destroy_jvm();
static void attach_jvm();
static void initialize_references();

static void clear_exception();
static void catch_exception();

static char* convert_string_to_cstring(jobject java_cstring);
static text* convert_string_to_text(jobject java_cstring);
static bytea* convert_jbyteArray_to_bytea(jbyteArray bytes);

static void on_proc_exit_cb();

static char* get_class_name(jclass class);

#define register_java_class(jclass_ref, class_fqdn)                            \
    {                                                                          \
        jclass class = (*env)->FindClass(env, (class_fqdn));                   \
        if (class == NULL) {                                                   \
            ereport(ERROR, errmsg("%s is not found", (class_fqdn)));           \
        }                                                                      \
        jclass_ref = (jclass)((*env)->NewGlobalRef(env, class));               \
    }

#define register_java_class_method(jmethod_ref, jclass_ref, name, sig)         \
    jmethod_ref = (*env)->GetMethodID(env, jclass_ref, (name), (sig));         \
    if (jmethod_ref == NULL) {                                                 \
        ereport(ERROR, errmsg("%s.%s is not found",                            \
                              get_class_name(jclass_ref), (name)));            \
    }

#define register_java_static_method(jmethod_ref, jclass_ref, name, sig)        \
    jmethod_ref = (*env)->GetStaticMethodID(env, jclass_ref, (name), (sig));   \
    if (jmethod_ref == NULL) {                                                 \
        ereport(ERROR, errmsg("%s.%s is not found",                            \
                              get_class_name(jclass_ref), (name)));            \
    }

void scalardb_initialize(ScalarDbFdwOptions* opts) {
    ereport(DEBUG3, errmsg("entering function %s", __func__));

    static bool already_initialized = false;

    if (already_initialized == true) {
        ereport(DEBUG3, errmsg("scalardb has already been initialized"));
        return;
    }

    initialize_jvm(opts);
    initialize_references();

    // TODO: skip after second call
    jstring config_file_path =
        (*env)->NewStringUTF(env, opts->config_file_path);
    clear_exception();
    (*env)->CallStaticObjectMethod(env, ScalarDbUtils_class,
                                   ScalarDbUtils_initialize, config_file_path);
    catch_exception();

    already_initialized = true;
}

extern jobject scalardb_scan_all(char* namespace, char* table_name) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    jstring namespace_str = (*env)->NewStringUTF(env, namespace);
    jstring table_name_str = (*env)->NewStringUTF(env, table_name);
    clear_exception();
    jobject scanner = (*env)->CallStaticObjectMethod(
        env, ScalarDbUtils_class, ScalarDbUtils_scanAll, namespace_str,
        table_name_str);
    catch_exception();
    return scanner;
}

extern jobject scalardb_scanner_one(jobject scanner) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));

    clear_exception();
    (*env)->PushLocalFrame(env, LOCAL_FRAME_CAPACITY);
    catch_exception();

    clear_exception();
    jobject o = (*env)->CallObjectMethod(env, scanner, Scanner_one);
    catch_exception();
    return o;
}

/*
 * Release all object references created from Result object by popping the
 * current local references in JVM.
 * It is caller's responsibility to ensure that the local frame for the Result
 * object has been created in scalardb_scanner_one in ahead
 */
extern void scalardb_scanner_release_result() {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    (*env)->PopLocalFrame(env, NULL);
}

extern void scalardb_scanner_close(jobject scanner) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    clear_exception();
    (*env)->CallVoidMethod(env, scanner, Closeable_close);
    catch_exception();

    (*env)->DeleteLocalRef(env, scanner);
}

extern int scalardb_list_size(jobject list) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    jint size = (*env)->CallIntMethod(env, list, List_size);
    return (int)size;
}

extern jobject scalardb_list_iterator(jobject list) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    return (*env)->CallObjectMethod(env, list, List_iterator);
}

extern bool scalardb_iterator_has_next(jobject iterator) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    jboolean b = (*env)->CallBooleanMethod(env, iterator, Iterator_hasNext);
    return b == JNI_TRUE;
}

extern bool scalardb_optional_is_present(jobject optional) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    jboolean b = (*env)->CallBooleanMethod(env, optional, Optional_isPresent);
    return b == JNI_TRUE;
}

extern jobject scalardb_optional_get(jobject optional) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    clear_exception();
    jobject o = (*env)->CallObjectMethod(env, optional, Optional_get);
    catch_exception();
    return o;
}

extern jobject scalardb_iterator_next(jobject iterator) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    return (*env)->CallObjectMethod(env, iterator, Iterator_next);
}

extern bool scalardb_result_is_null(jobject result, char* attname) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    jboolean b =
        (*env)->CallBooleanMethod(env, result, Result_isNull, attname_str);
    return b == JNI_TRUE;
}

extern bool scalardb_result_get_boolean(jobject result, char* attname) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    jboolean b =
        (*env)->CallBooleanMethod(env, result, Result_getBoolean, attname_str);
    return b == JNI_TRUE;
}

extern int scalardb_result_get_int(jobject result, char* attname) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    return (int)(*env)->CallIntMethod(env, result, Result_getInt, attname_str);
}

extern long scalardb_result_get_bigint(jobject result, char* attname) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    return (long)(*env)->CallLongMethod(env, result, Result_getBigInt,
                                        attname_str);
}

extern float scalardb_result_get_float(jobject result, char* attname) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    return (float)(*env)->CallFloatMethod(env, result, Result_getFloat,
                                          attname_str);
}

extern double scalardb_result_get_double(jobject result, char* attname) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    return (double)(*env)->CallDoubleMethod(env, result, Result_getDouble,
                                            attname_str);
}

extern text* scalardb_result_get_text(jobject result, char* attname) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    jstring str =
        (*env)->CallObjectMethod(env, result, Result_getText, attname_str);
    return convert_string_to_text(str);
}

extern bytea* scalardb_result_get_blob(jobject result, char* attname) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    jbyteArray bytes = (jbyteArray)(*env)->CallObjectMethod(
        env, result, Result_getBlobAsBytes, attname_str);
    return convert_jbyteArray_to_bytea(bytes);
}

extern int scalardb_result_columns_size(jobject result) {
    ereport(DEBUG5, errmsg("entering function %s", __func__));
    return (int)(*env)->CallStaticIntMethod(
        env, ScalarDbUtils_class, ScalarDbUtils_getResultColumnsSize, result);
}

static void initialize_jvm(ScalarDbFdwOptions* opts) {
    ereport(DEBUG3, errmsg("entering function %s", __func__));

    static bool already_initialized = false;

    if (already_initialized == false) {
        size_t classpath_len = JAVA_CLASS_PATH_STR_LEN +
                               strlen(STR_SCALARDB_JAR_PATH) + NULL_STR_LEN;
        char* classpath = (char*)palloc0(classpath_len);
        snprintf(classpath, classpath_len, "-Djava.class.path=%s",
                 STR_SCALARDB_JAR_PATH);

        ereport(DEBUG3, errmsg("classpath: %s", classpath));

        char* max_heap_size =
            opts->max_heap_size ? opts->max_heap_size : DEFAULT_MAX_HEAP_SIZE;

        JavaVMOption* options =
            (JavaVMOption*)palloc0(sizeof(JavaVMOption) * 2);
        size_t max_heap_size_option_len =
            MAX_HEAP_SIZE_STR_LEN + strlen(max_heap_size) + NULL_STR_LEN;
        char* max_heap_size_option = (char*)palloc0(max_heap_size_option_len);
        snprintf(max_heap_size_option, max_heap_size_option_len, "-Xmx%s",
                 max_heap_size);

        options[0].optionString = classpath;
        options[1].optionString = max_heap_size_option;

        JavaVMInitArgs vm_args;
        vm_args.nOptions = 2;
        vm_args.version = JNI_VERSION;
        vm_args.options = options;
        vm_args.ignoreUnrecognized = JNI_FALSE;

        jint res = JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args);
        if (res < 0) {
            ereport(
                ERROR,
                errmsg("Failed to create Java VM. JNI error code: %d", res));
        }
        ereport(DEBUG3, errmsg("Successfully created a JVM with %s heapsize",
                               max_heap_size));
        on_proc_exit(on_proc_exit_cb, 0);
        already_initialized = true;
    } else {
        jint GetEnvStat = (*jvm)->GetEnv(jvm, (void**)&env, JNI_VERSION);
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
}

static void destroy_jvm() {
    ereport(DEBUG3, errmsg("entering function %s", __func__));
    (*jvm)->DestroyJavaVM(jvm);
}

static void attach_jvm() {
    ereport(DEBUG3, errmsg("entering function %s", __func__));
    (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);
}

static void initialize_references() {
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
    register_java_class_method(Optional_isPresent, Optional_class, "isPresent",
                               "()Z");
    register_java_class_method(Optional_get, Optional_class, "get",
                               "()Ljava/lang/Object;");

    // java.io.Closeable
    register_java_class(Closeable_class, "java/io/Closeable");
    register_java_class_method(Closeable_close, Closeable_class, "close",
                               "()V");

    // ScalarDbUtils
    register_java_class(ScalarDbUtils_class, "ScalarDbUtils");
    register_java_static_method(ScalarDbUtils_initialize, ScalarDbUtils_class,
                                "initialize", "(Ljava/lang/String;)V");
    register_java_static_method(ScalarDbUtils_closeStorage, ScalarDbUtils_class,
                                "closeStorage", "()V");
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
    register_java_class_method(Result_getBoolean, Result_class, "getBoolean",
                               "(Ljava/lang/String;)Z");
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

static void clear_exception() { (*env)->ExceptionClear(env); }

static void catch_exception() {
    if ((*env)->ExceptionCheck(env)) {
        jthrowable exc = (*env)->ExceptionOccurred(env);

        jstring exception_message =
            (jstring)(*env)->CallObjectMethod(env, exc, Object_toString);
        char* msg = convert_string_to_cstring(exception_message);

        ereport(ERROR, errcode(ERRCODE_FDW_ERROR),
                errmsg("Exception occurred in JVM: %s", msg));
    }
    return;
}

static char* convert_string_to_cstring(jstring java_string) {
    char* ret = NULL;

    if (!((*env)->IsInstanceOf(env, java_string, String_class))) {
        ereport(ERROR, errmsg("Not an instance of String class"));
    }

    if (java_string != NULL) {
        const char* str =
            (char*)(*env)->GetStringUTFChars(env, (jstring)java_string, 0);
        ret = pstrdup(str);
        (*env)->ReleaseStringUTFChars(env, (jstring)java_string, str);
        (*env)->DeleteLocalRef(env, java_string);
    }
    return ret;
}

static text* convert_string_to_text(jstring java_string) {
    text* ret = NULL;

    if (!((*env)->IsInstanceOf(env, java_string, String_class))) {
        ereport(ERROR, errmsg("Not an instance of String class"));
    }

    if (java_string != NULL) {
        const char* str =
            (char*)(*env)->GetStringUTFChars(env, (jstring)java_string, 0);

        jsize nbytes = (*env)->GetStringUTFLength(env, java_string);

        ret = (bytea*)palloc(nbytes + VARHDRSZ);
        SET_VARSIZE(ret, nbytes + VARHDRSZ);
        memcpy(VARDATA(ret), str, nbytes);

        (*env)->ReleaseStringUTFChars(env, (jstring)java_string, str);
        (*env)->DeleteLocalRef(env, java_string);
    }
    return ret;
}

static bytea* convert_jbyteArray_to_bytea(jbyteArray bytes) {
    bytea* ret = NULL;

    if (bytes != NULL) {
        jbyte* elems = (*env)->GetByteArrayElements(env, bytes, 0);

        jsize len = (*env)->GetArrayLength(env, bytes);
        size_t nbytes = sizeof(jbyte) * len;

        ret = (bytea*)palloc(nbytes + VARHDRSZ);
        SET_VARSIZE(ret, nbytes + VARHDRSZ);
        memcpy(VARDATA(ret), elems, nbytes);

        (*env)->ReleaseByteArrayElements(env, bytes, elems, JNI_ABORT);
    }
    return ret;
}

static void on_proc_exit_cb() {
    (*env)->CallStaticObjectMethod(env, ScalarDbUtils_class,
                                   ScalarDbUtils_closeStorage);
    destroy_jvm();
}

static char* get_class_name(jclass class) {
    jclass cls = (*env)->FindClass(env, "java/lang/Class");
    jmethodID getName =
        (*env)->GetMethodID(env, cls, "getName", "()Ljava/lang/String;");
    jstring name = (*env)->CallObjectMethod(env, class, getName);
    return convert_string_to_cstring(name);
}
