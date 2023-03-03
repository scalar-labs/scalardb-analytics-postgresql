#include "scalardb.h"

#include "storage/ipc.h"
#include "utils/elog.h"
#include "utils/errcodes.h"

#define JNI_VERSION JNI_VERSION_1_8

#define Str(arg) #arg
#define StrValue(arg) Str(arg)
#define STR_DATA_MODULE_DIR StrValue(DATA_MODULE_DIR)

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

static jclass ScalarDBUtils_class;
static jmethodID ScalarDBUtils_initialize;
static jmethodID ScalarDBUtils_closeStorage;
static jmethodID ScalarDBUtils_scanAll;
static jmethodID ScalarDBUtils_getResultColumnsSize;

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

static void initialize_jvm(ScalarDBFdwOptions* opts);
static void destroy_jvm();
static void attach_jvm();
static void detach_jvm();
static void initialize_references();

static void clear_exception();
static void catch_exception();

static char* convert_string_to_cstring(jobject java_cstring);
static char* convert_jbyteArray_to_c_byte_array(jbyteArray bytes);

static void on_proc_exit_cb();

void scalardb_initialize(ScalarDBFdwOptions* opts) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));

    initialize_jvm(opts);

    // TODO: skip after second call
    jstring config_file_path =
        (*env)->NewStringUTF(env, opts->config_file_path);
    clear_exception();
    (*env)->CallStaticObjectMethod(env, ScalarDBUtils_class,
                                   ScalarDBUtils_initialize, config_file_path);
    catch_exception();
}

extern jobject scalardb_scan_all(char* namespace, char* table_name) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    jstring namespace_str = (*env)->NewStringUTF(env, namespace);
    jstring table_name_str = (*env)->NewStringUTF(env, table_name);
    clear_exception();
    jobject scanner = (*env)->CallStaticObjectMethod(
        env, ScalarDBUtils_class, ScalarDBUtils_scanAll, namespace_str,
        table_name_str);
    catch_exception();
    return scanner;
}

extern jobject scalardb_scanner_one(jobject scanner) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    clear_exception();
    jobject o = (*env)->CallObjectMethod(env, scanner, Scanner_one);
    catch_exception();
    return o;
}

extern void scalardb_scanner_close(jobject scanner) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    clear_exception();
    (*env)->CallVoidMethod(env, scanner, Closeable_close);
    catch_exception();
}

extern int scalardb_list_size(jobject list) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    jint size = (*env)->CallIntMethod(env, list, List_size);
    return (int)size;
}

extern jobject scalardb_list_iterator(jobject list) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    return (*env)->CallObjectMethod(env, list, List_iterator);
}

extern bool scalardb_iterator_has_next(jobject iterator) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    jboolean b = (*env)->CallBooleanMethod(env, iterator, Iterator_hasNext);
    return b == JNI_TRUE;
}

extern bool scalardb_optional_is_present(jobject optional) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    jboolean b = (*env)->CallBooleanMethod(env, optional, Optional_isPresent);
    return b == JNI_TRUE;
}

extern jobject scalardb_optional_get(jobject optional) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    clear_exception();
    jobject o = (*env)->CallObjectMethod(env, optional, Optional_get);
    catch_exception();
    return o;
}

extern jobject scalardb_iterator_next(jobject iterator) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    return (*env)->CallObjectMethod(env, iterator, Iterator_next);
}

extern bool scalardb_result_is_null(jobject result, char* attname) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    jboolean b =
        (*env)->CallBooleanMethod(env, result, Result_isNull, attname_str);
    return b == JNI_TRUE;
}

extern bool scalardb_result_get_boolean(jobject result, char* attname) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    jboolean b =
        (*env)->CallBooleanMethod(env, result, Result_getBoolean, attname_str);
    return b == JNI_TRUE;
}

extern int scalardb_result_get_int(jobject result, char* attname) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    return (int)(*env)->CallIntMethod(env, result, Result_getInt, attname_str);
}

extern long scalardb_result_get_bigint(jobject result, char* attname) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    return (long)(*env)->CallLongMethod(env, result, Result_getBigInt,
                                        attname_str);
}

extern float scalardb_result_get_float(jobject result, char* attname) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    return (float)(*env)->CallFloatMethod(env, result, Result_getFloat,
                                          attname_str);
}

extern double scalardb_result_get_double(jobject result, char* attname) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    return (double)(*env)->CallDoubleMethod(env, result, Result_getDouble,
                                            attname_str);
}

extern char* scalardb_result_get_text(jobject result, char* attname) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    jstring str =
        (*env)->CallObjectMethod(env, result, Result_getText, attname_str);
    return convert_string_to_cstring(str);
}

extern char* scalardb_result_get_blob(jobject result, char* attname) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    jstring attname_str = (*env)->NewStringUTF(env, attname);
    jbyteArray bytes = (jbyteArray)(*env)->CallObjectMethod(
        env, result, Result_getBlobAsBytes, attname_str);
    return convert_jbyteArray_to_c_byte_array(bytes);
}

extern int scalardb_result_columns_size(jobject result) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    return (int)(*env)->CallStaticIntMethod(
        env, ScalarDBUtils_class, ScalarDBUtils_getResultColumnsSize, result);
}

static void initialize_jvm(ScalarDBFdwOptions* opts) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));

    static bool already_initialized = false;

    if (already_initialized == false) {
        size_t classpath_len = 18 + strlen(opts->jar_file_path) + 1 +
                               strlen(STR_DATA_MODULE_DIR) + 1;
        char* classpath = (char*)palloc0(classpath_len);
        snprintf(classpath, classpath_len, "-Djava.class.path=%s:%s",
                 opts->jar_file_path, STR_DATA_MODULE_DIR);

        char* max_heap_size = opts->max_heap_size ? opts->max_heap_size : "1g";

        JavaVMOption* options =
            (JavaVMOption*)palloc0(sizeof(JavaVMOption) * 2);
        size_t max_heap_size_option_len = 4 + strlen(max_heap_size) + 1;
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
            ereport(ERROR, (errmsg("Failed to create Java VM")));
        }
        ereport(DEBUG1, (errmsg("Successfully created a JVM with %s heapsize",
                                max_heap_size)));
        on_proc_exit(on_proc_exit_cb, 0);
        already_initialized = true;

        initialize_references();
    } else {
        jint GetEnvStat = (*jvm)->GetEnv(jvm, (void**)&env, JNI_VERSION);
        if (GetEnvStat == JNI_EDETACHED) {
            ereport(DEBUG1, (errmsg("GetEnv: JNI_EDETACHED; the current "
                                    "thread is not attached to the VM")));
            attach_jvm();
        } else if (GetEnvStat == JNI_OK) {
            ereport(DEBUG1, (errmsg("GetEnv: JNI_OK")));
        } else if (GetEnvStat == JNI_EVERSION) {
            ereport(ERROR, (errmsg("GetEnv: JNI_EVERSION; the specified "
                                   "version is not supported")));
        }
    }
}

static void destroy_jvm() {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    (*jvm)->DestroyJavaVM(jvm);
}

static void attach_jvm() {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);
}

static void detach_jvm() {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    (*jvm)->DetachCurrentThread(jvm);
}

static void initialize_references() {
    ereport(DEBUG1, errmsg("entering function %s", __func__));

    jclass class;

    // java.lang.Object
    class = (*env)->FindClass(env, "java/lang/Object");
    if (class == NULL) {
        ereport(ERROR, (errmsg("java/lang/Object is not found")));
    }
    Object_class = (jclass)((*env)->NewGlobalRef(env, class));
    Object_toString = (*env)->GetMethodID(env, Object_class, "toString",
                                          "()Ljava/lang/String;");
    if (Object_toString == NULL) {
        ereport(ERROR, (errmsg("Object.toString is not found")));
    }

    // java.lang.String
    class = (*env)->FindClass(env, "java/lang/String");
    if (class == NULL) {
        ereport(ERROR, (errmsg("java/lang/String is not found")));
    }
    String_class = (jclass)((*env)->NewGlobalRef(env, class));

    // java.util.List
    class = (*env)->FindClass(env, "java/util/List");
    if (class == NULL) {
        ereport(ERROR, (errmsg("java/util/List is not found")));
    }
    List_class = (jclass)((*env)->NewGlobalRef(env, class));
    List_size = (*env)->GetMethodID(env, List_class, "size", "()I");
    if (List_size == NULL) {
        ereport(ERROR, (errmsg("List.size is not found")));
    }

    List_iterator = (*env)->GetMethodID(env, List_class, "iterator",
                                        "()Ljava/util/Iterator;");
    if (List_iterator == NULL) {
        ereport(ERROR, (errmsg("List.iterator is not found")));
    }

    // java.util.Iterator
    class = (*env)->FindClass(env, "java/util/Iterator");
    if (class == NULL) {
        ereport(ERROR, (errmsg("java/util/Iterator is not found")));
    }
    Iterator_class = (jclass)((*env)->NewGlobalRef(env, class));
    Iterator_hasNext =
        (*env)->GetMethodID(env, Iterator_class, "hasNext", "()Z");
    if (Iterator_hasNext == NULL) {
        ereport(ERROR, (errmsg("Iterator.hasNext is not found")));
    }

    Iterator_next = (*env)->GetMethodID(env, Iterator_class, "next",
                                        "()Ljava/lang/Object;");
    if (Iterator_next == NULL) {
        ereport(ERROR, (errmsg("Iterator.next is not found")));
    }

    // java.util.Optional
    class = (*env)->FindClass(env, "java/util/Optional");
    if (class == NULL) {
        ereport(ERROR, (errmsg("java/util/Optional is not found")));
    }
    Optional_class = (jclass)((*env)->NewGlobalRef(env, class));
    Optional_isPresent =
        (*env)->GetMethodID(env, Optional_class, "isPresent", "()Z");
    if (Optional_isPresent == NULL) {
        ereport(ERROR, (errmsg("Optional.isPresent is not found")));
    }
    Optional_get =
        (*env)->GetMethodID(env, Optional_class, "get", "()Ljava/lang/Object;");
    if (Optional_get == NULL) {
        ereport(ERROR, (errmsg("Optional.get is not found")));
    }

    // java.io.Closeable
    class = (*env)->FindClass(env, "java/io/Closeable");
    if (class == NULL) {
        ereport(ERROR, (errmsg("java/io/Closeable is not found")));
    }
    Closeable_class = (jclass)((*env)->NewGlobalRef(env, class));
    Closeable_close = (*env)->GetMethodID(env, Closeable_class, "close", "()V");
    if (Closeable_close == NULL) {
        ereport(ERROR, (errmsg("Closeable.close is not found")));
    }

    // ScalarDBUtils
    class = (*env)->FindClass(env, "ScalarDBUtils");
    if (class == NULL) {
        ereport(ERROR, (errmsg("ScalarDBUtils is not found")));
    }

    ScalarDBUtils_class = (jclass)((*env)->NewGlobalRef(env, class));
    ScalarDBUtils_initialize = (*env)->GetStaticMethodID(
        env, ScalarDBUtils_class, "initialize", "(Ljava/lang/String;)V");
    if (ScalarDBUtils_initialize == NULL) {
        ereport(ERROR, (errmsg("ScalarDBUtils.initialize is not found")));
    }
    ScalarDBUtils_closeStorage = (*env)->GetStaticMethodID(
        env, ScalarDBUtils_class, "closeStorage", "()V");
    if (ScalarDBUtils_closeStorage == NULL) {
        ereport(ERROR, (errmsg("ScalarDBUtils.closeStorage is not found")));
    }
    ScalarDBUtils_scanAll = (*env)->GetStaticMethodID(
        env, ScalarDBUtils_class, "scanAll",
        "(Ljava/lang/String;Ljava/lang/String;)Lcom/scalar/db/api/Scanner;");
    if (ScalarDBUtils_scanAll == NULL) {
        ereport(ERROR, (errmsg("ScalarDBUtils.scanAll is not found")));
    }
    ScalarDBUtils_getResultColumnsSize = (*env)->GetStaticMethodID(
        env, ScalarDBUtils_class, "getResultColumnsSize",
        "(Lcom/scalar/db/api/Result;)I");
    if (ScalarDBUtils_getResultColumnsSize == NULL) {
        ereport(ERROR,
                (errmsg("ScalarDBUtils.getResultColumnsSize  is not found")));
    }

    // com.scalar.db.api.Result
    class = (*env)->FindClass(env, "com/scalar/db/api/Result");
    if (class == NULL) {
        ereport(ERROR, (errmsg("com/scalar/db/api/Result is not found")));
    }
    Result_class = (jclass)((*env)->NewGlobalRef(env, class));
    Result_isNull = (*env)->GetMethodID(env, Result_class, "isNull",
                                        "(Ljava/lang/String;)Z");
    if (Result_isNull == NULL) {
        ereport(ERROR, (errmsg("Result.isNull is not found")));
    }
    Result_getBoolean = (*env)->GetMethodID(env, Result_class, "getBoolean",
                                            "(Ljava/lang/String;)Z");
    if (Result_getBoolean == NULL) {
        ereport(ERROR, (errmsg("Result.getBoolean is not found")));
    }
    Result_getInt = (*env)->GetMethodID(env, Result_class, "getInt",
                                        "(Ljava/lang/String;)I");
    if (Result_getInt == NULL) {
        ereport(ERROR, (errmsg("Result.getInt is not found")));
    }
    Result_getBigInt = (*env)->GetMethodID(env, Result_class, "getBigInt",
                                           "(Ljava/lang/String;)J");
    if (Result_getBigInt == NULL) {
        ereport(ERROR, (errmsg("Result.getBigInt is not found")));
    }
    Result_getFloat = (*env)->GetMethodID(env, Result_class, "getFloat",
                                          "(Ljava/lang/String;)F");
    if (Result_getFloat == NULL) {
        ereport(ERROR, (errmsg("Result.getFloat is not found")));
    }
    Result_getDouble = (*env)->GetMethodID(env, Result_class, "getDouble",
                                           "(Ljava/lang/String;)D");
    if (Result_getDouble == NULL) {
        ereport(ERROR, (errmsg("Result.getDouble is not found")));
    }
    Result_getText = (*env)->GetMethodID(
        env, Result_class, "getText", "(Ljava/lang/String;)Ljava/lang/String;");
    if (Result_getText == NULL) {
        ereport(ERROR, (errmsg("Result.getText is not found")));
    }
    Result_getBlobAsBytes = (*env)->GetMethodID(
        env, Result_class, "getBlobAsBytes", "(Ljava/lang/String;)[B");
    if (Result_getBlobAsBytes == NULL) {
        ereport(ERROR, (errmsg("Result_getBlobAsBytes is not found")));
    }

    // com.scalar.db.api.Scanner
    class = (*env)->FindClass(env, "com/scalar/db/api/Scanner");
    if (class == NULL) {
        ereport(ERROR, (errmsg("com/scalar/db/api/Scanner is not found")));
    }
    Scanner_class = (jclass)((*env)->NewGlobalRef(env, class));
    Scanner_one = (*env)->GetMethodID(env, Scanner_class, "one",
                                      "()Ljava/util/Optional;");
    if (Scanner_one == NULL) {
        ereport(ERROR, (errmsg("Scanner.one is not found")));
    }
}

static void clear_exception() {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    (*env)->ExceptionClear(env);
}

static void catch_exception() {
    ereport(DEBUG1, errmsg("entering function %s", __func__));

    if ((*env)->ExceptionCheck(env)) {
        jthrowable exc = (*env)->ExceptionOccurred(env);

        jstring exception_message =
            (jstring)(*env)->CallObjectMethod(env, exc, Object_toString);
        char* msg = convert_string_to_cstring(exception_message);

        ereport(DEBUG1, errmsg("%s", msg));
        ereport(ERROR, errcode(ERRCODE_FDW_ERROR),
                (errmsg("Exception occurred in JVM")));
    }
    return;
}

static char* convert_string_to_cstring(jstring java_string) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));

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

static char* convert_jbyteArray_to_c_byte_array(jbyteArray bytes) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));

    char* ret = NULL;

    if (bytes != NULL) {
        jbyte* elems = (*env)->GetByteArrayElements(env, bytes, 0);
        jsize len = (*env)->GetArrayLength(env, bytes);
        ret = pnstrdup((char*)elems, sizeof(jbyte) * len);
        (*env)->ReleaseByteArrayElements(env, bytes, elems, JNI_ABORT);
    }
    return ret;
}

static void on_proc_exit_cb() {
    (*env)->CallStaticObjectMethod(env, ScalarDBUtils_class,
                                   ScalarDBUtils_closeStorage);
    destroy_jvm();
}
