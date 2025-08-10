/*************************************************************************
*Copyright (C), 2025-2035, tanhuang
**************************************************************************
*@文件名称:
*   ets_db.c
*@文件描述:
*   数据库相关函数接口实现源文件
*@版本信息:
*   v0.1
*@修改历史:
*   1).created by tanhuang, 2025/06/07
*************************************************************************/
#include "glib.h"
#include "mysql.h"
#include "ets_pub.h"
#include "ets_log.h"
#include "ets_db.h"
#include "gmem.h"
#include "ghash.h"
#include "ets_mutex.h"

#define ETS_SQL_SIZE_MAX()    (8192)

static struct
{
    struct ets_mutex_t lock;
    GHashTable* hash_table;

    void (*remove_hash_node)(gpointer, gpointer, gpointer);
    void (*dump_hash_node)(gpointer, gpointer, gpointer);
    
    void* pri;
}ets_dbi = {0};

typedef struct __tagETS_DB_CTX_S
{
    struct ets_mutex_t lock;
    ETS_DB_CTX_PARAM_S param;
    char* cache;
    int32_t cache_size;
    
    MYSQL* mysql;
    MYSQL* conn_mysql;
    MYSQL_STMT* stmt;
    MYSQL_RES* res;
    
    int32_t (*init)(struct __tagETS_DB_CTX_S*, void*);
    void (*deinit)(struct __tagETS_DB_CTX_S*, void*);

    int32_t (*do_lock)(struct ets_mutex_t*);
    void (*do_unlock)(struct ets_mutex_t*);
    int32_t (*connect)(struct __tagETS_DB_CTX_S*, void*);
    int32_t (*execute_command)(struct __tagETS_DB_CTX_S*, const char*, void*);
    
    void (*remove)(struct __tagETS_DB_CTX_S*, void*);
    void (*dump)(struct __tagETS_DB_CTX_S*, void*);
    
    void* pri;
}ETS_DB_CTX_S;

static int32_t ets_db_ctx_Init(struct __tagETS_DB_CTX_S* dbc, void* ud)
{
    int32_t status = 0;
    ETS_RETURN_IF_PTR_NULL(dbc, ETS_EINVAL);
    
    if (!dbc->mysql)
    {
        dbc->mysql = mysql_init(NULL);
    }

    ETS_LOG_DEBUG("ets_db_ctx_Init, mysql:%p", dbc->mysql);
    
    status = (dbc->mysql)? ETS_SUCCESS : ETS_EFAILED;
    return status;
}

static void ets_db_ctx_Deinit(struct __tagETS_DB_CTX_S* dbc, void* ud)
{
    ETS_CHECK_PTR_NULL(dbc);
    
    ETS_LOG_DEBUG("ets_db_ctx_Deinit, dbc:%p", dbc);
    
    return;
}

static void ets_db_ctx_Remove(struct __tagETS_DB_CTX_S* dbc, void* ud)
{
    ETS_CHECK_PTR_NULL(dbc);

    ETS_LOG_DEBUG("ets_db_ctx_Remove, dbc:%p, conn_mysql:%p, mysql:%p",
        dbc, dbc->conn_mysql, dbc->mysql);

    if (dbc->conn_mysql)
    {
        mysql_close(dbc->conn_mysql);
    }
    
    if (dbc->conn_mysql != dbc->mysql)
    {
        mysql_close(dbc->mysql);
    }

    dbc->conn_mysql = NULL;
    dbc->mysql = NULL;
    
    ETS_LOG_DEBUG("ets_db_ctx_Remove finish");
    return;
}


static void ets_db_ctx_Dump(struct __tagETS_DB_CTX_S* dbc, void* ud)
{
    ETS_CHECK_PTR_NULL(dbc);

    ETS_LOG_DEBUG("ets_db_ctx_Dump");
    ETS_LOG_DEBUG("host     : %s", (dbc->param.host)? dbc->param.host : "---");
    ETS_LOG_DEBUG("user     : %s", (dbc->param.user)? dbc->param.user : "---");
    ETS_LOG_DEBUG("passwd   : %s", (dbc->param.passwd)? dbc->param.passwd : "---");
    ETS_LOG_DEBUG("database : %s", (dbc->param.database)? dbc->param.database : "---");
    ETS_LOG_DEBUG("mysql    : %p", dbc->mysql);
    ETS_LOG_DEBUG("res      : %p", dbc->res);
    
    return;
}

static int32_t ets_db_ctx_Connect(struct __tagETS_DB_CTX_S* dbc, void* ud)
{
    int32_t status = 0;
    ETS_RETURN_IF_PTR_NULL(dbc, ETS_EINVAL);
    
    if (!dbc->conn_mysql)
    {
        dbc->conn_mysql = mysql_real_connect(dbc->mysql, dbc->param.host, dbc->param.user,
            dbc->param.passwd, dbc->param.database, 0, NULL, 0 /*CLIENT_MULTI_STATEMENTS*/);
    }

    ETS_LOG_DEBUG("ets_db_ctx_Connect, conn_mysql:%p", dbc->conn_mysql);

    mysql_query(dbc->mysql, "SET NAMES utf8mb4");
    
    status = (dbc->conn_mysql)? ETS_SUCCESS : ETS_EFAILED;
    return status;
}

static int32_t ets_db_cmd_ExecuteCommand(ETS_DB_CTX_S* dbc, const char* sql, void* ud)
{
    int32_t status = 0;

    ETS_LOG_DEBUG("ets_db_cmd_ExecuteCommand");

    (void)dbc->do_lock(&dbc->lock);
    status = dbc->init(dbc, ud);
    if (status)
    {
        ETS_LOG_ERROR("init failed, status:%d", status);
        goto finish;
    }

    status = dbc->connect(dbc, ud);
    if (status)
    {
        ETS_LOG_ERROR("connect failed, status:%d", status);
        goto finish;
    }

    status = mysql_query(dbc->mysql, sql);
    if (status)
    {
        ETS_LOG_ERROR("mysql_query failed, status:%d, mysql_error:'%s'",
            status, mysql_error(dbc->mysql));
        goto finish;
    }

    ETS_LOG_DEBUG("mysql_query success, sql:'%s'", (char*)sql);
    
finish:
    dbc->do_unlock(&dbc->lock);
    return status;
}

static guint ets_db_env_ctx_Hash(gconstpointer key)
{
    return ((guint)*((const gint*)key));
}

static gboolean ets_db_env_ctx_Equal(gconstpointer a, gconstpointer b)
{
    return (*((const gint*)a) == *((const gint*)b));
}

static void ets_db_env_ctx_Remove(gpointer key, gpointer val, gpointer ud)
{
    ETS_DB_CTX_S* ctx = (ETS_DB_CTX_S*)val;

    ETS_CHECK_PTR_NULL(ctx);
    ETS_LOG_DEBUG("ets_db_env_ctx_Remove, key:%p, val:%p", key, val);

    /* 数据库连接上下文做清理动作 */
    if (ctx->remove)
    {
        ctx->remove(ctx, ctx->pri);
    }
    
    (void)ETS_Mutex_Lock(&ets_dbi.lock);
    (void)g_hash_table_remove(ets_dbi.hash_table, key);
    
    ETS_Mutex_Unlock(&ets_dbi.lock);

    ETS_Mutex_Exit(&ctx->lock);

    ETS_LOG_DEBUG("ets_db_env_ctx_Remove free");

    ETS_LOG_DEBUG("ctx->cache   : %p", ctx->cache);
    ETS_LOG_DEBUG("ctx->host    : %p", ctx->param.host);
    ETS_LOG_DEBUG("ctx->user    : %p", ctx->param.user);
    ETS_LOG_DEBUG("ctx->passwd  : %p", ctx->param.passwd);
    ETS_LOG_DEBUG("ctx->database: %p", ctx->param.database);
    
    ETS_FREE(ctx->cache);
    ETS_FREE(ctx->param.host);
    ETS_FREE(ctx->param.user);
    ETS_FREE(ctx->param.passwd);
    ETS_FREE(ctx->param.database);
    //ETS_FREE(ctx);

    ETS_LOG_DEBUG("ets_db_env_ctx_Remove finish");
    return;
}

static void ets_db_env_ctx_Dump(gpointer key, gpointer val, gpointer ud)
{
    ETS_DB_CTX_S* ctx = (ETS_DB_CTX_S*)val;

    ETS_CHECK_PTR_NULL(ctx);
    if (ctx->dump)
    {
        ctx->dump(ctx, ud);
    }

    return;
}

void ETS_db_env_Dump(void)
{
    ETS_LOG_DEBUG("ETS_db_env_Dump");
    
    (void)ETS_Mutex_Lock(&ets_dbi.lock);
    ETS_LOG_DEBUG("-----------------------------------------------------------");
    ETS_LOG_DEBUG("dump hash_table, size:%u", g_hash_table_size(ets_dbi.hash_table));
    
    g_hash_table_foreach(ets_dbi.hash_table, ets_dbi.dump_hash_node, ets_dbi.pri);
    ETS_Mutex_Unlock(&ets_dbi.lock);

    ETS_LOG_DEBUG("-----------------------------------------------------------");
    return;
}

int32_t ETS_db_env_Init(void)
{
    int32_t status = 0;
    GHashTable* hash_table = NULL;

    ETS_LOG_DEBUG("ETS_db_env_Init");
    
    status = ETS_Mutex_Init(&ets_dbi.lock);
    ETS_RETURN_IF_CONDITION_TURE(status, status);
    
    hash_table = g_hash_table_new(ets_db_env_ctx_Hash, ets_db_env_ctx_Equal);
    if (NULL == hash_table)
    {
        ETS_Mutex_Exit(&ets_dbi.lock);
        
        ETS_LOG_ERROR("memory short and g_hash_table_new failed");
        return ETS_ENOMEM;
    }

    ets_dbi.hash_table        = hash_table;
    ets_dbi.remove_hash_node  = ets_db_env_ctx_Remove;
    ets_dbi.dump_hash_node    = ets_db_env_ctx_Dump;
    
    ETS_LOG_DEBUG("ETS_db_env_Init success");
    return ETS_SUCCESS;
}

void ETS_db_env_Exit(void)
{
    ETS_LOG_DEBUG("ETS_db_env_Exit");
    
    (void)ETS_Mutex_Lock(&ets_dbi.lock);

    g_hash_table_foreach(ets_dbi.hash_table, ets_dbi.remove_hash_node, ets_dbi.pri);
    g_hash_table_destroy(ets_dbi.hash_table);
    
    ets_dbi.hash_table = NULL;
    ETS_Mutex_Unlock(&ets_dbi.lock);
    
    ETS_LOG_DEBUG("ETS_db_env_Exit finish");
    return;
}

int32_t ETS_db_ctx_New(ETS_DB_CTX_PARAM_S* param, void** ctx)
{
    ETS_DB_CTX_S* dbc = NULL;
    int32_t status = 0;

    ETS_RETURN_IF_PTR_NULL(param, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(param->host, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(param->user, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(param->passwd, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(param->database, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(ctx, ETS_EINVAL);

    ETS_LOG_DEBUG("ETS_db_ctx_New");
    ETS_LOG_DEBUG("host     : %s", param->host);
    ETS_LOG_DEBUG("user     : %s", param->user);
    ETS_LOG_DEBUG("passwd   : %s", param->passwd);
    ETS_LOG_DEBUG("database : %s", param->database);

    dbc = g_new(ETS_DB_CTX_S, 1);
    ETS_RETURN_IF_PTR_NULL(ctx, ETS_ENOMEM);

    ETS_MEMSET(dbc, 0, sizeof(ETS_DB_CTX_S));

    status = ETS_Mutex_Init(&dbc->lock);
    ETS_JUMP_IF_CONDITION_TURE(status, failed);

    dbc->cache_size     = ETS_SQL_SIZE_MAX();
    dbc->cache          = ETS_MALLOC(dbc->cache_size);
    dbc->param.host     = ets_strdup(param->host);
    dbc->param.user     = ets_strdup(param->user);
    dbc->param.passwd   = ets_strdup(param->passwd);
    dbc->param.database = ets_strdup(param->database);

    ETS_JUMP_IF_PTR_NULL(dbc->cache, failed);
    ETS_JUMP_IF_PTR_NULL(dbc->param.host, failed);
    ETS_JUMP_IF_PTR_NULL(dbc->param.user, failed);
    ETS_JUMP_IF_PTR_NULL(dbc->param.passwd, failed);
    ETS_JUMP_IF_PTR_NULL(dbc->param.database, failed);

    ETS_MEMSET(dbc->cache, 0, dbc->cache_size);
    dbc->do_lock    = ETS_Mutex_Lock;
    dbc->do_unlock  = ETS_Mutex_Unlock;
    dbc->dump       = ets_db_ctx_Dump;
    dbc->init       = ets_db_ctx_Init;
    dbc->deinit     = ets_db_ctx_Deinit;
    dbc->connect    = ets_db_ctx_Connect;
    dbc->execute_command = ets_db_cmd_ExecuteCommand;
    dbc->remove = ets_db_ctx_Remove;
    dbc->pri    = dbc;

    (void)ETS_Mutex_Lock(&ets_dbi.lock);
    g_hash_table_insert(ets_dbi.hash_table, dbc, dbc);
    ETS_Mutex_Unlock(&ets_dbi.lock);
    
    *ctx = dbc;

    ETS_LOG_DEBUG("dbc->cache   : %p", dbc->cache);
    ETS_LOG_DEBUG("dbc->host    : %p", dbc->param.host);
    ETS_LOG_DEBUG("dbc->user    : %p", dbc->param.user);
    ETS_LOG_DEBUG("dbc->passwd  : %p", dbc->param.passwd);
    ETS_LOG_DEBUG("dbc->database: %p", dbc->param.database);
    
    ETS_LOG_DEBUG("ETS_db_ctx_New success, conn:%p", dbc);
    return ETS_SUCCESS;

failed:
    ETS_Mutex_Exit(&dbc->lock);

    ETS_FREE(dbc->cache);
    ETS_FREE(dbc->param.host);
    ETS_FREE(dbc->param.user);
    ETS_FREE(dbc->param.passwd);
    ETS_FREE(dbc->param.database);
    ETS_FREE(dbc);

    ETS_LOG_ERROR("ETS_db_ctx_New failed");
    return ETS_ENOMEM;
}

void ETS_db_ctx_Del(void** ctx)
{
    ETS_DB_CTX_S* dbc = NULL;

    ETS_LOG_DEBUG("ETS_db_ctx_Del, ctx:%p", ctx);
    
    ETS_CHECK_PTR_NULL(ctx);
    ETS_CHECK_PTR_NULL(dbc = (*((ETS_DB_CTX_S**)ctx)));

    if (dbc->remove)
    {
        dbc->remove(dbc, dbc->pri);
    }
    
    (void)ETS_Mutex_Lock(&ets_dbi.lock);
    
    if (!g_hash_table_lookup(ets_dbi.hash_table, dbc))
    {
        ETS_Mutex_Unlock(&ets_dbi.lock);
        
        ETS_LOG_WARN("dbc %p not exist, do not brk", dbc);
        goto next;
    }

    (void)g_hash_table_remove(ets_dbi.hash_table, dbc);
    ETS_Mutex_Unlock(&ets_dbi.lock);

next:
    ETS_Mutex_Exit(&dbc->lock);

    ETS_LOG_DEBUG("ETS_db_ctx_Del free");
    ETS_LOG_DEBUG("dbc->cache   : %p", dbc->cache);
    ETS_LOG_DEBUG("dbc->host    : %p", dbc->param.host);
    ETS_LOG_DEBUG("dbc->user    : %p", dbc->param.user);
    ETS_LOG_DEBUG("dbc->passwd  : %p", dbc->param.passwd);
    ETS_LOG_DEBUG("dbc->database: %p", dbc->param.database);
    ETS_LOG_DEBUG("dbc          : %p", dbc);
    
    ETS_FREE(dbc->cache);
    ETS_FREE(dbc->param.host);
    ETS_FREE(dbc->param.user);
    ETS_FREE(dbc->param.passwd);
    ETS_FREE(dbc->param.database);

    ETS_LOG_DEBUG("ETS_db_ctx_Del finish");
    return;
}

int32_t ETS_db_cmd_Execute(void* ctx, const char* sql,
    int32_t (*cb)(void* , void*), void* ud)
{
    int32_t status = 0;
    ETS_DB_CTX_S* dbc = (ETS_DB_CTX_S*)ctx;
    
    ETS_RETURN_IF_PTR_NULL(dbc, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(sql, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(!strlen(sql), ETS_EINVAL);

    ETS_LOG_DEBUG("ETS_db_cmd_Execute, sql:'%s'", sql);

    status = dbc->execute_command(dbc, sql, dbc->pri);
    if (status)
    {
        ETS_LOG_ERROR("ETS_db_cmd_Execute failed, status:%d, sql:'%s'", status, sql);
        status = ETS_EIO;
        
        goto finish;
    }

    ETS_LOG_DEBUG("ETS_db_cmd_Execute success, sql:'%s'", sql);
    
    /* TODO:执行回调函数 */
    
finish:
    return status;
}

static MYSQL_STMT* ets_db_stmt_Prepare(MYSQL* mysql, const char* query)
{
    MYSQL_STMT *stmt = mysql_stmt_init(mysql);
    
    if (stmt && mysql_stmt_prepare(stmt, query, (unsigned long)strlen(query)))
    {
        mysql_stmt_close(stmt);
        return NULL;
    }
    
    return stmt;
}

int32_t ETS_db_cmd_Insert(void* ctx, const char* sql,
    int32_t (*fill_bind)(MYSQL_BIND**, int32_t*, void*), void* ud)
{
    int32_t status = 0;
    ETS_DB_CTX_S* dbc = (ETS_DB_CTX_S*)ctx;
    MYSQL_STMT* stmt = NULL;
    MYSQL_BIND* my_bind = NULL;
    int32_t bind_count = 0;

    ETS_RETURN_IF_PTR_NULL(fill_bind, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(dbc, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(sql, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(!strlen(sql), ETS_EINVAL);

    ETS_LOG_DEBUG("ETS_db_cmd_Insert, sql:'%s'", sql);

    (void)dbc->do_lock(&dbc->lock);

    status = dbc->init(dbc, ud);
    if (status)
    {
        ETS_LOG_ERROR("init failed, status:%d", status);
        goto finish;
    }

    status = dbc->connect(dbc, ud);
    if (status)
    {
        ETS_LOG_ERROR("connect failed, status:%d", status);
        goto finish;
    }
    
    stmt = ets_db_stmt_Prepare(dbc->mysql, sql);
    if (!stmt)
    {
        ETS_LOG_ERROR("ets_db_stmt_Prepare failed, status:%d, errno:'%s'",
            status, mysql_error(dbc->mysql));
        
        status = ETS_EFAILED;
        goto finish;
    }

    status = fill_bind(&my_bind, &bind_count, ud);
    if (status)
    {
        ETS_LOG_ERROR("fill_bind failed, status:%d", status);
        
        status = ETS_EFAILED;
        goto finish;
    }

    ETS_LOG_DEBUG("fill_bind success, bind_count:%d", bind_count);
    
    status = mysql_stmt_bind_param(stmt, my_bind);
    if (status)
    {
        ETS_LOG_ERROR("mysql_stmt_bind_param failed, status:%d, errno:'%s'",
            status, mysql_error(dbc->mysql));
        
        status = ETS_EFAILED;
        goto finish;
    }
    
    status = mysql_stmt_execute(stmt);
    if (status)
    {
        ETS_LOG_ERROR("mysql_stmt_execute failed, status:%d, errno:'%s'",
            status, mysql_error(dbc->mysql));
        
        status = ETS_EFAILED;
        goto finish;
    }
    
    status = mysql_commit(dbc->mysql);
    if (status)
    {
        ETS_LOG_ERROR("mysql_commit failed, status:%d, errno:'%s'",
            status, mysql_error(dbc->mysql));
        
        status = ETS_EFAILED;
        goto finish;
    }
    
finish:
    if (stmt)
    {
        (void)mysql_stmt_close(stmt);
        stmt = NULL;
    }
    dbc->do_unlock(&dbc->lock);
    
    return status;
}

int32_t ETS_DB_API ETS_db_cmd_Query(void* ctx, const char* sql,
    int32_t (*fill_bind)(MYSQL_BIND**, int32_t*, void*),
    int32_t (*callback)(void*), void* ud)
{
    int32_t status = 0;
    ETS_DB_CTX_S* dbc = (ETS_DB_CTX_S*)ctx;
    MYSQL_STMT* stmt = NULL;
    MYSQL_BIND* my_bind = NULL;
    int32_t bind_count = 0;
    
    ETS_RETURN_IF_PTR_NULL(dbc, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(sql, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(!strlen(sql), ETS_EINVAL);
    
    ETS_LOG_DEBUG("ETS_db_cmd_Query, sql:'%s'", sql);

    (void)dbc->do_lock(&dbc->lock);

    status = dbc->init(dbc, dbc->pri);
    if (status)
    {
        ETS_LOG_ERROR("init failed, status:%d", status);
        goto finish;
    }

    status = dbc->connect(dbc, dbc->pri);
    if (status)
    {
        ETS_LOG_ERROR("connect failed, status:%d", status);
        goto finish;
    }

    stmt = ets_db_stmt_Prepare(dbc->mysql, sql);
    if (!stmt)
    {
        ETS_LOG_ERROR("ets_db_stmt_Prepare failed, status:%d, errno:'%s'",
            status, mysql_error(dbc->mysql));
        status = ETS_EFAILED;
        goto finish;
    }

    status = mysql_stmt_execute(stmt);
    if (status)
    {
        ETS_LOG_ERROR("mysql_stmt_execute failed, status:%d, errno:'%s'",
            status, mysql_error(dbc->mysql));
        
        status = ETS_EFAILED;
        goto finish;
    }
    
    status = fill_bind(&my_bind, &bind_count, ud);
    if (status)
    {
        ETS_LOG_ERROR("fill_bind failed, status:%d", status);
        
        status = ETS_EFAILED;
        goto finish;
    }

    ETS_LOG_DEBUG("fill_bind success, bind_count:%d", bind_count);
    
    status = mysql_stmt_bind_result(stmt, my_bind);
    if (status)
    {
        ETS_LOG_ERROR("mysql_stmt_bind_result failed, status:%d, errno:'%s'",
            status, mysql_error(dbc->mysql));
        
        status = ETS_EFAILED;
        goto finish;
    }

    ETS_LOG_DEBUG("mysql_stmt_bind_result success");
    
    while (MYSQL_NO_DATA != mysql_stmt_fetch(stmt))
    {
        (void)callback(ud);
    }
    
finish:
    if (stmt)
    {
        (void)mysql_stmt_close(stmt);
        stmt = NULL;
    }
    dbc->do_unlock(&dbc->lock);
    
    return status;
}

int32_t ETS_db_cmd_Update(void* ctx, const char* sql,
    int32_t (*fill_bind)(MYSQL_BIND**, int32_t*, void*), void* ud)
{
    return ETS_db_cmd_Insert(ctx, sql, fill_bind, ud);
}

int32_t ETS_db_cmd_Delete(void* ctx, const char* sql,
    int32_t (*fill_bind)(MYSQL_BIND**, int32_t*, void*), void* ud)
{
    return ETS_db_cmd_Insert(ctx, sql, fill_bind, ud);
}

/* !!! 注意:数组下标与ETS_DBTBL_FIELD_TYPE_E一一映射 !!! */
static struct
{
    ETS_DBTBL_FIELD_TYPE_E u_type;
    enum_field_types i_type;
    char* sql_type;
}__ets_type_name_map[ETS_DBTBL_FIELD_TYPE_MAX] =
{
    {ETS_DBTBL_FIELD_TYPE_TINY          , MYSQL_TYPE_TINY        , "TINYINT"     }, //1
    {ETS_DBTBL_FIELD_TYPE_SHORT         , MYSQL_TYPE_SHORT       , "SMALLINT"    }, //2
    {ETS_DBTBL_FIELD_TYPE_LONG          , MYSQL_TYPE_LONG        , "INT"         }, //4 mediumint:3,int/integer:4
    {ETS_DBTBL_FIELD_TYPE_LONGLONG      , MYSQL_TYPE_LONGLONG    , "BIGINT"      }, //8
    {ETS_DBTBL_FIELD_TYPE_FLOAT         , MYSQL_TYPE_FLOAT       , "FLOAT"       },
    {ETS_DBTBL_FIELD_TYPE_DOUBLE        , MYSQL_TYPE_DOUBLE      , "DOUBLE"      },
    {ETS_DBTBL_FIELD_TYPE_STRING        , MYSQL_TYPE_VAR_STRING  , "VARCHAR"     },
    {ETS_DBTBL_FIELD_TYPE_TINY_BLOB     , MYSQL_TYPE_TINY_BLOB   , "TINYBLOB"    }, //1:小型二进制数据(如token), 255字节(2⁸-1)
    {ETS_DBTBL_FIELD_TYPE_BLOB          , MYSQL_TYPE_BLOB        , "BLOB"        }, //2:普通图片(如头像), 65,535字节(2¹⁶-1)
    {ETS_DBTBL_FIELD_TYPE_MEDIUM_BLOB   , MYSQL_TYPE_MEDIUM_BLOB , "MEDIUMBLOB"  }, //4:高清图片/音频, 16,777,215字节(2²⁴-1, 约16MB)
    {ETS_DBTBL_FIELD_TYPE_LONG_BLOB     , MYSQL_TYPE_LONG_BLOB   , "LONGBLOB"    }, //8:视频/大型文件,不确定大小的二进制数据, 4,294,967,295字节(2³²-1,约4GB)

    /*
    DATE:
        YYYY-MM-DD, 1000-01-01至9999-12-31,精度:天,不支持时区
        仅存储日期,节省空间(生日、纪念日)
    TIME:
        HH:MM:SS, -838:59:59至838:59:59,精度:秒,不支持时区
        仅存储时间,营业时间、持续时间
    DATETIME:
        8:YYYY-MM-DD HH:MM:SS, 1000-01-01 00:00:00至9999-12-31 23:59:59,精度:秒,不支持时区
        大范围、无时区干扰,固定时间点（如订单创建时间）
        DATETIME(3)为毫秒级精度
        DATETIME(6)为微秒级精度
        DATETIME 默认允许NULL，需显式设置DEFAULT CURRENT_TIMESTAMP
    TIMESTAMP:
        4:YYYY-MM-DD HH:MM:SS, 1970-01-01 00:00:01至2038-01-19 03:14:07,精度:秒,自动转换时区
        时区感知、自动更新、节省空间,全球应用、数据修改跟踪 TIMESTAMP(6)为微秒级精度
        TIMESTAMP(6) : 高精度时间记录,金融交易时间、性能监控
        TIMESTAMP 默认为NOT NULL且自动初始化为当前时间
    */
    {ETS_DBTBL_FIELD_TYPE_DATE          , MYSQL_TYPE_DATE        , "DATE"        },
    {ETS_DBTBL_FIELD_TYPE_TIME          , MYSQL_TYPE_TIME        , "TIME"        },
    {ETS_DBTBL_FIELD_TYPE_DATETIME      , MYSQL_TYPE_DATETIME    , "DATETIME"    },
    {ETS_DBTBL_FIELD_TYPE_TIMESTAMP     , MYSQL_TYPE_TIMESTAMP   , "TIMESTAMP"   },

};

int32_t ETS_db_opr_CreateTable(void* ctx, const char* tbl,
    ETS_DBTBL_FIELD_DESC_S* fields, int32_t count)
{
    int32_t status = ETS_EINVAL;
    int32_t idx = 0;
    ETS_DBTBL_FIELD_DESC_S* fi = fields;
    char* buf = NULL;
    int32_t buf_size = 0;
    ETS_DB_CTX_S* dbc = NULL;
    int32_t len = 0;
    int32_t offset = 0;
    
    ETS_RETURN_IF_PTR_NULL(ctx, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(tbl, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(fields, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(!strlen(tbl), ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(count<=0, ETS_EINVAL);

    ETS_LOG_DEBUG("ETS_db_opr_CreateTable, tbl:'%s', count:%d", tbl, count);
    
    dbc = (ETS_DB_CTX_S*)ctx;

    (void)dbc->do_lock(&dbc->lock);
    
    buf = dbc->cache;
    buf_size = dbc->cache_size;
    ETS_MEMSET(buf, 0, buf_size);

    status = ETS_EIO;
    len = ets_snprintf_s(buf + offset, buf_size - offset, "CREATE TABLE %s (", tbl);
    ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
    offset += len;
    
    for (idx = 0; idx < count; idx++)
    {
        if (!fi->is_active)
        {
            fi++;
            continue;
        }
        
        switch (fi->key.type)
        {
        case ETS_DBTBL_FIELD_TYPE_STRING:
            status = ETS_EINVAL;
            ETS_JUMP_IF_CONDITION_TURE((!fi->key.max_length), finish);

            status = ETS_EIO;
            len = ets_snprintf_s(buf + offset, buf_size - offset, "%s%s %s(%u) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci %s%s%s",
                (!idx)? "" : ", ", fi->key.name, __ets_type_name_map[fi->key.type].sql_type, fi->key.max_length,
                (fi->key.is_null)? "NULL" : "NOT NULL",
                (fi->key.auto_increment)? " AUTO_INCREMENT" : "",
                (fi->key.is_primary_key)? " PRIMARY KEY" : "");
            break;
            
        default:
            status = ETS_EIO;
            len = ets_snprintf_s(buf + offset, buf_size - offset, "%s%s %s %s%s%s",
                (!idx)? "" : ", ", fi->key.name, __ets_type_name_map[fi->key.type].sql_type,
                (fi->key.is_null)? "NULL" : "NOT NULL",
                 (fi->key.auto_increment)? " AUTO_INCREMENT" : "",
                 (fi->key.is_primary_key)? " PRIMARY KEY" : "");
            break;
        }
        
        ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
        offset += len;
        
        fi++;
    }

    status = ETS_EIO;
    len = ets_snprintf_s(buf + offset, buf_size - offset, ")");
    ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
    offset += len;

    ETS_LOG_DEBUG("build CREATE_TABLE SQL success, sql:'%s'", buf);

    status = ETS_db_cmd_Execute(ctx, buf, NULL, NULL);
    if (status)
    {
        ETS_LOG_ERROR("CREATE_TABLE failed, status:%d", status);
        goto finish;
    }

    ETS_LOG_DEBUG("CREATE_TABLE success");
    
finish:
    dbc->do_unlock(&dbc->lock);
    return status;
}

static void ets_db_fd_Free(struct __tagETS_DBTBL_FIELD_DESC_S** pfd)
{
    struct __tagETS_DBTBL_FIELD_DESC_S* fd = NULL;

    ETS_CHECK_PTR_NULL(pfd);
    ETS_CHECK_PTR_NULL(fd=*pfd);
    
    ETS_FREE(fd->key.name);
    
    switch (fd->key.type)
    {
    case ETS_DBTBL_FIELD_TYPE_STRING:
        ETS_FREE(fd->val.d_str.buf);
        break;
    
    case ETS_DBTBL_FIELD_TYPE_TINY_BLOB:
    case ETS_DBTBL_FIELD_TYPE_BLOB:
    case ETS_DBTBL_FIELD_TYPE_MEDIUM_BLOB:
    case ETS_DBTBL_FIELD_TYPE_LONG_BLOB:
        ETS_FREE(fd->val.d_blob.buf);
        break;
    
    default:
        break;
    }
    
    ETS_FREE(fd);
    return;
}

static void ets_db_fd_FreeFieldsInfo(ETS_DBTBL_FIELD_DESC_S*** field_desc, int32_t count)
{
    int32_t idx = 0;
    ETS_DBTBL_FIELD_DESC_S** fds = NULL;
    
    ETS_CHECK_PTR_NULL(field_desc);
    ETS_CHECK_PTR_NULL(fds=*field_desc);
    ETS_CHECK_CONDITION_TURE(!count);

    for (idx = 0; idx < count; idx++)
    {
        ets_db_fd_Free(&fds[idx]);
    }
    
    ETS_FREE(fds);
    *field_desc = NULL;
    
    return;
}

static int32_t ets_db_fd_GetFieldsInfo(void* ctx, const char* tbl,
    ETS_DBTBL_FIELD_DESC_S*** field_desc, int32_t* count)
{
    int32_t status = 0;
    ETS_DB_CTX_S* dbc = NULL;
    char* buf = NULL;
    int32_t buf_size = 0;
    int32_t len = 0;
    int32_t offset = 0;
    MYSQL_STMT* stmt = NULL;
    MYSQL_RES* result = NULL;
    int32_t num_fields = 0;
    int32_t idx = 0;
    MYSQL_FIELD* fields = NULL;
    ETS_DBTBL_FIELD_DESC_S** fds = NULL;
    int32_t is_nullable = 0;
    int32_t is_primary_key = 0;
    int32_t auto_increment = 0;
    int32_t kdx = 0;

    ETS_RETURN_IF_PTR_NULL(ctx, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(tbl, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(!strlen(tbl), ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(field_desc, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(count, ETS_EINVAL);
    
    ETS_LOG_DEBUG("ETS_db_opr_GetFieldsInfo, tbl:'%s'", tbl);
    
    dbc = (ETS_DB_CTX_S*)ctx;
    (void)dbc->do_lock(&dbc->lock);
    
    buf = dbc->cache;
    buf_size = dbc->cache_size;
    ETS_MEMSET(buf, 0, buf_size);

    status = ETS_EIO;
    len = ets_snprintf_s(buf + offset, buf_size - offset, "SELECT * FROM %s", tbl);
    ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
    offset += len;

    ETS_LOG_DEBUG("build SELECT SQL success, sql:'%s'", buf);
    
    status = dbc->init(dbc, dbc->pri);
    if (status)
    {
        ETS_LOG_ERROR("init failed, status:%d", status);
        goto finish;
    }

    status = dbc->connect(dbc, dbc->pri);
    if (status)
    {
        ETS_LOG_ERROR("connect failed, status:%d", status);
        goto finish;
    }
    
    stmt = ets_db_stmt_Prepare(dbc->mysql, buf);
    if (!stmt)
    {
        ETS_LOG_ERROR("ets_db_stmt_Prepare failed, status:%d, errno:'%s'",
            status, mysql_error(dbc->mysql));
        
        status = ETS_EFAILED;
        goto finish;
    }

    status = mysql_stmt_execute(stmt);
    if (status)
    {
        ETS_LOG_ERROR("mysql_stmt_execute failed, status:%d, errno:'%s'",
            status, mysql_error(dbc->mysql));
        
        status = ETS_EFAILED;
        goto finish;
    }

    status = mysql_stmt_store_result(stmt);
    if (status)
    {
        ETS_LOG_ERROR("mysql_stmt_store_result failed, status:%d, errno:'%s'",
            status, mysql_error(dbc->mysql));
        
        status = ETS_EFAILED;
        goto finish;
    }
    
    result = mysql_stmt_result_metadata(stmt);
    if (!result)
    {
        ETS_LOG_ERROR("mysql_stmt_result_metadata failed, errno:'%s'", mysql_error(dbc->mysql));
        
        status = ETS_EFAILED;
        goto finish;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    num_fields = mysql_num_fields(result);
    fields = mysql_fetch_fields(result);

    if (!num_fields || !fields)
    {
        ETS_LOG_ERROR("mysql_fetch_fields exception, num_fields:%d, fields:%p", num_fields, fields);
        
        status = ETS_EFAILED;
        goto finish;
    }
    
    ETS_LOG_DEBUG("mysql_fetch_fields success, num_fields:%d", num_fields);

    fds = (ETS_DBTBL_FIELD_DESC_S**)ETS_MALLOC(sizeof(ETS_DBTBL_FIELD_DESC_S*) * num_fields);
    if (!fds)
    {
        ETS_LOG_ERROR("xalloc FIELD_DESC** failed, num_fields:%d", num_fields);
        
        status = ETS_ENOMEM;
        goto finish;
    }

    ETS_MEMSET(fds, 0, sizeof(ETS_DBTBL_FIELD_DESC_S*) * num_fields);
    
    ETS_LOG_DEBUG("表'%s'的字段信息:", tbl);
    ETS_LOG_DEBUG("+----------------+----------------+----------------+--------+---------+------+");
    ETS_LOG_DEBUG("| 字段名         | 类型           | 最大长度       | 可为空 | 是否主键| 自增 |");
    ETS_LOG_DEBUG("+----------------+----------------+----------------+--------+---------+------+");

    // 遍历所有字段并打印信息
    for (idx = 0; idx < num_fields; idx++)
    {
        fds[idx] = (ETS_DBTBL_FIELD_DESC_S*)ETS_MALLOC(sizeof(ETS_DBTBL_FIELD_DESC_S));
        if (!fds[idx])
        {
            ETS_LOG_ERROR("xalloc FIELD_DESC* failed, idx:%d", idx);
            status = ETS_ENOMEM;
            
            goto failed;
        }

        ETS_MEMSET(fds[idx], 0, sizeof(ETS_DBTBL_FIELD_DESC_S));

        is_nullable    = (fields[idx].flags & NOT_NULL_FLAG) ? 0 : 1;
        is_primary_key = (fields[idx].flags & PRI_KEY_FLAG) ? 1 : 0;
        auto_increment = (fields[idx].flags & AUTO_INCREMENT_FLAG) ? 1 : 0;
        
        fds[idx]->is_active          = 0;
        fds[idx]->key.name           = ets_strdup(fields[idx].name);
        fds[idx]->key.is_null        = is_nullable;
        fds[idx]->key.is_primary_key = is_primary_key;
        fds[idx]->key.auto_increment = auto_increment;
        fds[idx]->key.type           = ETS_DBTBL_FIELD_TYPE_MAX;
        
        for (kdx = 0; kdx < ETS_DIM(__ets_type_name_map); kdx++)
        {
            if (__ets_type_name_map[kdx].i_type == fields[idx].type)
            {
                fds[idx]->key.type = __ets_type_name_map[kdx].u_type;
                break;
            }
        }

        ETS_LOG_DEBUG("| %-15s | %-15lu %-15d | %-15lu %-15lu | %-6s | %-8s | %-8s |",
           fields[idx].name, fields[idx].type, fds[idx]->key.type, fields[idx].length, fields[idx].max_length,
           is_nullable ? "是" : "否", is_primary_key ? "是" : "否",
           auto_increment ? "是" : "否");
    
        if ((ETS_DBTBL_FIELD_TYPE_MAX == fds[idx]->key.type) || (NULL == fds[idx]->key.name))
        {
            status = (NULL == fds[idx]->key.name)? ETS_ENOMEM : ETS_EUNEXPECTED;
            
            ETS_FREE(fds[idx]->key.name);
            ETS_FREE(fds[idx]);

            ETS_LOG_ERROR("build FIELD_DESC failed, idx:%d, status:%d", idx, status);
            goto failed;
        }

        switch (fds[idx]->key.type)
        {
        case ETS_DBTBL_FIELD_TYPE_STRING:
            fds[idx]->val.d_str.size = fields[idx].length;
            
            if (fds[idx]->val.d_str.size > 0)
            {
                fds[idx]->val.d_str.buf = (char*)ETS_MALLOC(fds[idx]->val.d_str.size);
                if (NULL == fds[idx]->val.d_str.buf)
                {
                    status = ETS_ENOMEM;
                    
                    ETS_FREE(fds[idx]->key.name);
                    ETS_FREE(fds[idx]);

                    ETS_LOG_ERROR("alloc d_str failed, idx:%d", idx);
                    goto failed;
                }
            }
            break;
            
        case ETS_DBTBL_FIELD_TYPE_TINY_BLOB:
            fds[idx]->val.d_blob.size = fields[idx].length;
            
            if (fds[idx]->val.d_blob.size > 0)
            {
                fds[idx]->val.d_blob.buf = (char*)ETS_MALLOC(fds[idx]->val.d_blob.size);
                if (NULL == fds[idx]->val.d_blob.buf)
                {
                    status = ETS_ENOMEM;
                    
                    ETS_FREE(fds[idx]->key.name);
                    ETS_FREE(fds[idx]);

                    ETS_LOG_ERROR("alloc d_blob failed, idx:%d", idx);
                    goto failed;
                }
            }
            break;
        
        case ETS_DBTBL_FIELD_TYPE_BLOB:
            fds[idx]->val.d_blob.size = fields[idx].length;
            
            if (fds[idx]->val.d_blob.size > 0)
            {
                fds[idx]->val.d_blob.buf = (char*)ETS_MALLOC(sizeof(uint16_t) * fds[idx]->val.d_blob.size);
                if (NULL == fds[idx]->val.d_blob.buf)
                {
                    status = ETS_ENOMEM;
                    
                    ETS_FREE(fds[idx]->key.name);
                    ETS_FREE(fds[idx]);

                    ETS_LOG_ERROR("alloc d_blob failed, idx:%d", idx);
                    goto failed;
                }
            }
            break;
            
        case ETS_DBTBL_FIELD_TYPE_MEDIUM_BLOB:
            fds[idx]->val.d_blob.size = fields[idx].length;
            
            if (fds[idx]->val.d_blob.size > 0)
            {
                fds[idx]->val.d_blob.buf = (char*)ETS_MALLOC(sizeof(uint32_t) * fds[idx]->val.d_blob.size);
                if (NULL == fds[idx]->val.d_blob.buf)
                {
                    status = ETS_ENOMEM;
                    
                    ETS_FREE(fds[idx]->key.name);
                    ETS_FREE(fds[idx]);

                    ETS_LOG_ERROR("alloc d_blob failed, idx:%d", idx);
                    goto failed;
                }
            }
            break;
            
        case ETS_DBTBL_FIELD_TYPE_LONG_BLOB:
            fds[idx]->val.d_blob.size = fields[idx].length;
            
            if (fds[idx]->val.d_blob.size > 0)
            {
                fds[idx]->val.d_blob.buf = (char*)ETS_MALLOC(2 * sizeof(uint32_t) * fds[idx]->val.d_blob.size);
                if (NULL == fds[idx]->val.d_blob.buf)
                {
                    status = ETS_ENOMEM;
                    
                    ETS_FREE(fds[idx]->key.name);
                    ETS_FREE(fds[idx]);

                    ETS_LOG_ERROR("alloc d_blob failed, idx:%d", idx);
                    goto failed;
                }
            }
            break;

        case ETS_DBTBL_FIELD_TYPE_TINY:
        case ETS_DBTBL_FIELD_TYPE_SHORT:
        case ETS_DBTBL_FIELD_TYPE_LONG:
        case ETS_DBTBL_FIELD_TYPE_LONGLONG:
        case ETS_DBTBL_FIELD_TYPE_FLOAT:
        case ETS_DBTBL_FIELD_TYPE_DOUBLE:
        case ETS_DBTBL_FIELD_TYPE_DATE:
        case ETS_DBTBL_FIELD_TYPE_TIME:
        case ETS_DBTBL_FIELD_TYPE_DATETIME:
        case ETS_DBTBL_FIELD_TYPE_TIMESTAMP:
            break;
        
        default:
            {
                status = ETS_ENSURPPT;
                
                ETS_FREE(fds[idx]->key.name);
                ETS_FREE(fds[idx]);

                ETS_LOG_ERROR("unspported field failed, idx:%d, key.type:%d", idx, fds[idx]->key.type);
                goto failed;
            }
            break;
        }
    }
    ///////////////////////////////////////////////////////////////////////////////////////////

    *field_desc = fds;
    *count = num_fields;

    ETS_LOG_DEBUG("ETS_db_opr_GetFieldsInfo success, num_fields:%d", num_fields);

    status = ETS_SUCCESS;
    goto finish;

failed:
    idx--;
    for (; idx >= 0; --idx)
    {
        ets_db_fd_Free(&fds[idx]);
    }
    ETS_FREE(fds);

finish:
    if (result)
    {
        mysql_free_result(result);
    }
    
    if (stmt)
    {
        (void)mysql_stmt_close(stmt);
        stmt = NULL;
    }
    dbc->do_unlock(&dbc->lock);
    
    return status;
}

int32_t ETS_db_opr_GetFieldsDesc(void* db_ctx, const char* tbl,
    int32_t (*callback)(int32_t, ETS_DBTBL_FIELD_DESC_S**, void*),
    int32_t (*iterator)(int32_t, ETS_DBTBL_FIELD_DESC_S*, void*),
    void* ud)
{
    int32_t status = 0;
    ETS_DBTBL_FIELD_DESC_S** field_desc = NULL;
    int32_t count = 0;
    int32_t idx = 0;
    
    ETS_RETURN_IF_PTR_NULL(db_ctx, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(tbl, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(!strlen(tbl), ETS_EINVAL);
    
    status = ets_db_fd_GetFieldsInfo(db_ctx, tbl, &field_desc, &count);
    if (status)
    {
        ETS_LOG_ERROR("ets_db_fd_GetFieldsInfo failed, status:%d, tbl:%s", status, tbl);
        goto finish;
    }

    ETS_LOG_DEBUG("ets_db_fd_GetFieldsInfo success, count:%d, tbl:%s", count, tbl);

    if (callback)
    {
        status = callback(count, field_desc, ud);
        if (status)
        {
            ETS_LOG_ERROR("callback failed, status:%d", status);
            goto finish;
        }
    }

    if (iterator)
    {
        for (idx = 0; idx < count; idx++)
        {
            status = iterator(idx, field_desc[idx], ud);
            if (status)
            {
                ETS_LOG_ERROR("iterator failed, idx:%d, status:%d", idx, status);
                goto finish;
            }
        }

        ETS_LOG_DEBUG("iterator success");
    }
    
finish:
    ets_db_fd_FreeFieldsInfo(&field_desc, count);
    return status;
}

int32_t ETS_db_opr_CopyFieldsDesc(ETS_DBTBL_FIELD_DESC_S** src, int32_t sc,
    ETS_DBTBL_FIELD_DESC_S*** dst, int32_t* dc)
{
    int32_t status = 0;
    ETS_DBTBL_FIELD_DESC_S** fds = NULL;
    int32_t idx = 0;
    
    ETS_RETURN_IF_PTR_NULL(src, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(dst, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(dc, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(sc<=0, ETS_EINVAL);

    fds = (ETS_DBTBL_FIELD_DESC_S**)ETS_MALLOC(sizeof(ETS_DBTBL_FIELD_DESC_S*) * sc);
    if (!fds)
    {
        ETS_LOG_ERROR("xalloc FIELD_DESC** failed, sc:%d", sc);
        
        status = ETS_ENOMEM;
        goto finish;
    }

    ETS_MEMSET(fds, 0, sizeof(ETS_DBTBL_FIELD_DESC_S*) * sc);
    for (idx = 0; idx < sc; idx++)
    {
        fds[idx] = (ETS_DBTBL_FIELD_DESC_S*)ETS_MALLOC(sizeof(ETS_DBTBL_FIELD_DESC_S));
        if (!fds[idx])
        {
            ETS_LOG_ERROR("xalloc FIELD_DESC* failed, idx:%d", idx);
            status = ETS_ENOMEM;
            
            goto failed;
        }

        ETS_MEMSET(fds[idx], 0, sizeof(ETS_DBTBL_FIELD_DESC_S));
        ETS_MEMCPY(fds[idx], src[idx], sizeof(ETS_DBTBL_FIELD_DESC_S));

        fds[idx]->key.name = ets_strdup(src[idx]->key.name);
        if (!fds[idx]->key.name)
        {
            ETS_LOG_ERROR("xalloc key.name failed, idx:%d", idx);
            status = ETS_ENOMEM;

            ETS_FREE(fds[idx]);
            goto failed;
        }
    
        switch (src[idx]->key.type)
        {
        case ETS_DBTBL_FIELD_TYPE_STRING:
            if (fds[idx]->val.d_str.size <= 0)
            {
                break;
            }
            
            fds[idx]->val.d_str.buf = (char*)ETS_MALLOC(fds[idx]->val.d_str.size);
            if (!fds[idx]->val.d_str.buf)
            {
                ETS_LOG_ERROR("xalloc d_str.buf failed, idx:%d", idx);
                status = ETS_ENOMEM;

                ETS_FREE(fds[idx]->key.name);
                ETS_FREE(fds[idx]);
                
                goto failed;
            }

            ETS_MEMSET(fds[idx]->val.d_str.buf, 0, fds[idx]->val.d_str.size);
            if (fds[idx]->val.d_str.len > 0)
            {
                ETS_MEMCPY(fds[idx]->val.d_str.buf, src[idx]->val.d_str.buf, fds[idx]->val.d_str.len);
            }
            
            break;
        
        case ETS_DBTBL_FIELD_TYPE_TINY_BLOB:
        case ETS_DBTBL_FIELD_TYPE_BLOB:
        case ETS_DBTBL_FIELD_TYPE_MEDIUM_BLOB:
        case ETS_DBTBL_FIELD_TYPE_LONG_BLOB:
            if (fds[idx]->val.d_blob.size <= 0)
            {
                break;
            }
            
            fds[idx]->val.d_blob.buf = (char*)ETS_MALLOC(fds[idx]->val.d_blob.size);
            if (!fds[idx]->val.d_blob.buf)
            {
                ETS_LOG_ERROR("xalloc d_blob.buf failed, idx:%d", idx);
                status = ETS_ENOMEM;

                ETS_FREE(fds[idx]->key.name);
                ETS_FREE(fds[idx]);
                
                goto failed;
            }

            ETS_MEMSET(fds[idx]->val.d_blob.buf, 0, fds[idx]->val.d_blob.size);
            if (fds[idx]->val.d_blob.len > 0)
            {
                ETS_MEMCPY(fds[idx]->val.d_blob.buf, src[idx]->val.d_blob.buf, fds[idx]->val.d_blob.len);
            }
            
            break;
        
        default:
            break;
        }
    }

    *dst = fds;
    *dc = sc;

    ETS_LOG_DEBUG("ETS_db_opr_CopyFieldsDesc success, dc:%d", *dc);
    
    status = ETS_SUCCESS;
    goto finish;
    
failed:
    idx--;
    for (; idx >= 0; --idx)
    {
        ets_db_fd_Free(&fds[idx]);
    }
    ETS_FREE(fds);

finish:
    return status;
}

void ETS_db_opr_FreeFieldInfo(ETS_DBTBL_FIELD_DESC_S** field_desc)
{
    ets_db_fd_Free(field_desc);
    return;
}

void ETS_db_opr_DumpFieldInfo(ETS_DBTBL_FIELD_DESC_S* field_desc)
{
    ETS_CHECK_PTR_NULL(field_desc);

    ETS_LOG_DEBUG("field_desc           : %p", field_desc);
    ETS_LOG_DEBUG("is_active            : %hhu", field_desc->is_active);
    ETS_LOG_DEBUG("cond                 : %d", field_desc->cond);
    ETS_LOG_DEBUG("key.name             : %s", (field_desc->key.name)? field_desc->key.name : "---");
    ETS_LOG_DEBUG("key.type             : %d", field_desc->key.type);
    ETS_LOG_DEBUG("key.is_null          : %hhu", field_desc->key.is_null);
    ETS_LOG_DEBUG("key.auto_increment   : %hhu", field_desc->key.auto_increment);
    ETS_LOG_DEBUG("key.is_primary_key   : %hhu", field_desc->key.is_primary_key);
    ETS_LOG_DEBUG("key.max_length       : %lu", field_desc->key.max_length);

    switch (field_desc->key.type)
    {
    case ETS_DBTBL_FIELD_TYPE_TINY:
        ETS_LOG_DEBUG("val.d_u8             : %hhu", field_desc->val.d_u8);
        break;
    
    case ETS_DBTBL_FIELD_TYPE_SHORT:
        ETS_LOG_DEBUG("val.d_u16            : %hu", field_desc->val.d_u16);
        break;

    case ETS_DBTBL_FIELD_TYPE_LONG:
        ETS_LOG_DEBUG("val.d_u32            : %u", field_desc->val.d_u32);
        break;

    case ETS_DBTBL_FIELD_TYPE_LONGLONG:
        ETS_LOG_DEBUG("val.d_u64            : %lu", field_desc->val.d_u64);
        break;

    case ETS_DBTBL_FIELD_TYPE_FLOAT:
        ETS_LOG_DEBUG("val.d_float          : %.5f", field_desc->val.d_float);
        break;
    
    case ETS_DBTBL_FIELD_TYPE_DOUBLE:
        ETS_LOG_DEBUG("val.d_double         : %.5f", field_desc->val.d_double);
        break;

    case ETS_DBTBL_FIELD_TYPE_STRING:
        ETS_LOG_DEBUG("val.d_str            : %p %d %d %s",
            field_desc->val.d_str.buf, field_desc->val.d_str.size, field_desc->val.d_str.len,
            (field_desc->val.d_str.len>0)? field_desc->val.d_str.buf : "---");
        break;

    case ETS_DBTBL_FIELD_TYPE_TINY_BLOB:
    case ETS_DBTBL_FIELD_TYPE_BLOB:
    case ETS_DBTBL_FIELD_TYPE_MEDIUM_BLOB:
    case ETS_DBTBL_FIELD_TYPE_LONG_BLOB:
        ETS_LOG_DEBUG("val.d_blob.len       : %d", field_desc->val.d_blob.len);
        break;

    case ETS_DBTBL_FIELD_TYPE_DATE:
        ETS_LOG_DEBUG("val.d_date           : %04u-%02u-%02u",
            field_desc->val.d_datetime.year,
            field_desc->val.d_datetime.month,
            field_desc->val.d_datetime.day);
        break;
        
    case ETS_DBTBL_FIELD_TYPE_TIME:
        ETS_LOG_DEBUG("val.d_time           : %02u:%02u:%02u",
            field_desc->val.d_datetime.hour,
            field_desc->val.d_datetime.minute,
            field_desc->val.d_datetime.second);
        break;

    case ETS_DBTBL_FIELD_TYPE_DATETIME:
        ETS_LOG_DEBUG("val.d_datetime       : %04u-%02u-%02u %02u:%02u:%02u",
            field_desc->val.d_datetime.year,
            field_desc->val.d_datetime.month,
            field_desc->val.d_datetime.day,
            field_desc->val.d_datetime.hour,
            field_desc->val.d_datetime.minute,
            field_desc->val.d_datetime.second);
        break;
        
    default:
        break;
    }

    ETS_LOG_DEBUG(" ");
    return;
}

void ETS_db_opr_FreeFieldsInfo(ETS_DBTBL_FIELD_DESC_S*** field_desc, int32_t count)
{
    ets_db_fd_FreeFieldsInfo(field_desc, count);
    return;
}

void ETS_db_opr_DumpFieldsInfo(ETS_DBTBL_FIELD_DESC_S** field_desc, int32_t count)
{
    int32_t idx = 0;
    
    ETS_CHECK_PTR_NULL(field_desc);
    ETS_CHECK_CONDITION_TURE(count<=0);

    ETS_LOG_DEBUG("DumpFieldsInfo, count:%d", count);
    
    for (idx = 0; idx < count; idx++)
    {
        ETS_LOG_DEBUG("idx                  : %d", idx);
        ETS_db_opr_DumpFieldInfo(field_desc[idx]);
    }
    
    return;
}

///////////////////////////////////////////////////////////////////////////////////////////////////
int32_t ETS_db_opr_Insert(void* ctx, const char* tbl,
    ETS_DBTBL_FIELD_DESC_S** field_desc, int32_t count)
{
    int32_t status = ETS_EIO;
    ETS_DB_CTX_S* dbc = NULL;
    char* buf = NULL;
    int32_t buf_size = 0;
    int32_t len = 0;
    int32_t offset = 0;
    int32_t idx = 0;
    ETS_DBTBL_FIELD_DESC_S* fd = NULL;
    int32_t valid_nr = 0;
    
    ETS_RETURN_IF_PTR_NULL(ctx, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(tbl, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(field_desc, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(!strlen(tbl), ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(count<=0, ETS_EINVAL);

    ETS_LOG_DEBUG("ETS_db_opr_Insert, count:%d, tbl:%s", count, tbl);

    dbc = (ETS_DB_CTX_S*)ctx;
    (void)dbc->do_lock(&dbc->lock);
    
    buf = dbc->cache;
    buf_size = dbc->cache_size;
    ETS_MEMSET(buf, 0, buf_size);

    len = ets_snprintf_s(buf + offset, buf_size - offset, "INSERT INTO %s (", tbl);
    ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
    offset += len;

    valid_nr = 0;
    
    /* 组装字段 */
    for (idx = 0; idx < count; idx++)
    {
        fd = field_desc[idx];
        if (!fd->is_active || fd->key.auto_increment)
        {
            continue;
        }

        len = ets_snprintf_s(buf + offset, buf_size - offset, "%s%s", (valid_nr)? ", " : "", fd->key.name);
        ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
        offset += len;
        
        valid_nr++;
    }

    if (!valid_nr)
    {
        ETS_LOG_ERROR("no valid insert field, tbl:%s", tbl);
        status = ETS_EINVAL;
        
        goto finish;
    }

    ETS_LOG_DEBUG("valid_nr:%d", valid_nr);

    len = ets_snprintf_s(buf + offset, buf_size - offset, ") VALUES (");
    ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
    offset += len;

    valid_nr = 0;
    
    for (idx = 0; idx < count; idx++)
    {
        fd = field_desc[idx];
        if (!fd->is_active || fd->key.auto_increment)
        {
            continue;
        }

        switch (fd->key.type)
        {
        case ETS_DBTBL_FIELD_TYPE_TINY:
            len = ets_snprintf_s(buf + offset, buf_size - offset, "%s%hhu", (valid_nr)? ", " : "", fd->val.d_u8);
            break;
        
        case ETS_DBTBL_FIELD_TYPE_SHORT:
            len = ets_snprintf_s(buf + offset, buf_size - offset, "%s%hu", (valid_nr)? ", " : "", fd->val.d_u16);
            break;

        case ETS_DBTBL_FIELD_TYPE_LONG:
            len = ets_snprintf_s(buf + offset, buf_size - offset, "%s%u", (valid_nr)? ", " : "", fd->val.d_u32);
            break;

        case ETS_DBTBL_FIELD_TYPE_LONGLONG:
            len = ets_snprintf_s(buf + offset, buf_size - offset, "%s%llu", (valid_nr)? ", " : "", fd->val.d_u64);
            break;

        case ETS_DBTBL_FIELD_TYPE_FLOAT:
            len = ets_snprintf_s(buf + offset, buf_size - offset, "%s%f", (valid_nr)? ", " : "", fd->val.d_float);
            break;
        
        case ETS_DBTBL_FIELD_TYPE_DOUBLE:
            len = ets_snprintf_s(buf + offset, buf_size - offset, "%s%f", (valid_nr)? ", " : "", fd->val.d_double);
            break;

        case ETS_DBTBL_FIELD_TYPE_STRING:
            len = ets_snprintf_s(buf + offset, buf_size - offset, "%s'%s'", (valid_nr)? ", " : "", fd->val.d_str.buf);
            break;

        case ETS_DBTBL_FIELD_TYPE_TINY_BLOB:
        case ETS_DBTBL_FIELD_TYPE_BLOB:
        case ETS_DBTBL_FIELD_TYPE_MEDIUM_BLOB:
        case ETS_DBTBL_FIELD_TYPE_LONG_BLOB:
            {
                //TODO:暂时不支持blob数据
                ETS_LOG_ERROR("do not support blob data");
                
                status = ETS_ENSURPPT;
                goto finish;
            }
            break;

        case ETS_DBTBL_FIELD_TYPE_DATE:
            len = ets_snprintf_s(buf + offset, buf_size - offset, "%s'%04u-%02u-%02u'", (valid_nr)? ", " : "",
                fd->val.d_datetime.year,
                fd->val.d_datetime.month,
                fd->val.d_datetime.day);
            break;

        case ETS_DBTBL_FIELD_TYPE_TIME:
            len = ets_snprintf_s(buf + offset, buf_size - offset, "%s'%02u:%02u:%02u'", (valid_nr)? ", " : "",
                fd->val.d_datetime.hour,
                fd->val.d_datetime.minute,
                fd->val.d_datetime.second);
            break;

        case ETS_DBTBL_FIELD_TYPE_DATETIME:
            len = ets_snprintf_s(buf + offset, buf_size - offset, "%s'%04u-%02u-%02u %02u:%02u:%02u'", (valid_nr)? ", " : "",
                fd->val.d_datetime.year,
                fd->val.d_datetime.month,
                fd->val.d_datetime.day,
                fd->val.d_datetime.hour,
                fd->val.d_datetime.minute,
                fd->val.d_datetime.second);
            break;
            
        default:
            {
                //TODO:暂时不支持blob数据
                ETS_LOG_ERROR("do not support data, key type:%d", fd->key.type);
                
                status = ETS_ENSURPPT;
                goto finish;
            }
            break;
        }
        
        ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
        offset += len;

        valid_nr++;
    }

    len = ets_snprintf_s(buf + offset, buf_size - offset, ")");
    ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
    offset += len;

    ETS_LOG_DEBUG("build insert_sql success, offset:%d, sql:%s", offset, buf);

    status = ETS_db_cmd_Execute(ctx, buf, NULL, NULL);
    if (status)
    {
        ETS_LOG_ERROR("ETS_db_cmd_Execute failed, status:%d", status);
        goto finish;
    }

    ETS_LOG_DEBUG("ETS_db_cmd_Execute success");
    status = ETS_SUCCESS;
    
finish:
    dbc->do_unlock(&dbc->lock);
    return status;
}

int32_t ETS_db_opr_Update(void* ctx, const char* tbl,
    ETS_DBTBL_FIELD_DESC_S** fdu, int32_t fdu_count,
    ETS_DBTBL_FIELD_DESC_S** fdc, int32_t fdc_count)
{
    int32_t status = ETS_EIO;
    ETS_DB_CTX_S* dbc = NULL;
    char* buf = NULL;
    int32_t buf_size = 0;
    int32_t len = 0;
    int32_t offset = 0;
    int32_t idx = 0;
    ETS_DBTBL_FIELD_DESC_S* fd = NULL;
    int32_t valid_nr = 0;
    char* cond[] = {" AND ", " AND ", " OR "};
    
    ETS_RETURN_IF_PTR_NULL(ctx, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(tbl, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(fdu, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(fdc, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(!strlen(tbl), ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(fdu_count<=0, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(fdc_count<=0, ETS_EINVAL);
    
    ETS_LOG_DEBUG("ETS_db_opr_Update, fdu_count:%d, fdc_count:%d, tbl:%s",
        fdu_count, fdc_count, tbl);

    dbc = (ETS_DB_CTX_S*)ctx;
    (void)dbc->do_lock(&dbc->lock);
    
    buf = dbc->cache;
    buf_size = dbc->cache_size;
    ETS_MEMSET(buf, 0, buf_size);

    len = ets_snprintf_s(buf + offset, buf_size - offset, "UPDATE %s SET ", tbl);
    ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
    offset += len;

    for (idx = 0; idx < fdu_count; idx++)
    {
        fd = fdu[idx];
        if (!fd->is_active || fd->key.auto_increment)
        {
            continue;
        }

        switch (fd->key.type)
        {
        case ETS_DBTBL_FIELD_TYPE_TINY:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%hhu", (valid_nr)? ", " : "", fd->key.name, fd->val.d_u8);
            break;
        
        case ETS_DBTBL_FIELD_TYPE_SHORT:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%hu", (valid_nr)? ", " : "", fd->key.name, fd->val.d_u16);
            break;

        case ETS_DBTBL_FIELD_TYPE_LONG:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%u", (valid_nr)? ", " : "", fd->key.name, fd->val.d_u32);
            break;

        case ETS_DBTBL_FIELD_TYPE_LONGLONG:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%llu", (valid_nr)? ", " : "", fd->key.name, fd->val.d_u64);
            break;

        case ETS_DBTBL_FIELD_TYPE_FLOAT:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%f", (valid_nr)? ", " : "", fd->key.name, fd->val.d_float);
            break;
        
        case ETS_DBTBL_FIELD_TYPE_DOUBLE:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%f", (valid_nr)? ", " : "", fd->key.name, fd->val.d_double);
            break;

        case ETS_DBTBL_FIELD_TYPE_STRING:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s='%s'", (valid_nr)? ", " : "", fd->key.name, fd->val.d_str.buf);
            break;

        case ETS_DBTBL_FIELD_TYPE_TINY_BLOB:
        case ETS_DBTBL_FIELD_TYPE_BLOB:
        case ETS_DBTBL_FIELD_TYPE_MEDIUM_BLOB:
        case ETS_DBTBL_FIELD_TYPE_LONG_BLOB:
            {
                //TODO:暂时不支持blob数据
                ETS_LOG_ERROR("do not support blob data");
                
                status = ETS_ENSURPPT;
                goto finish;
            }
            break;

        case ETS_DBTBL_FIELD_TYPE_DATE:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                    "%s%s='%04u-%02u-%02u'", (valid_nr)? ", " : "", fd->key.name,
                    fd->val.d_datetime.year,
                    fd->val.d_datetime.month,
                    fd->val.d_datetime.day);
            break;

        case ETS_DBTBL_FIELD_TYPE_TIME:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                    "%s%s='%02u:%02u:%02u'", (valid_nr)? ", " : "", fd->key.name,
                    fd->val.d_datetime.hour,
                    fd->val.d_datetime.minute,
                    fd->val.d_datetime.second);
            break;
            
        case ETS_DBTBL_FIELD_TYPE_DATETIME:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                    "%s%s='%04u-%02u-%02u %02u:%02u:%02u'", (valid_nr)? ", " : "", fd->key.name,
                    fd->val.d_datetime.year,
                    fd->val.d_datetime.month,
                    fd->val.d_datetime.day,
                    fd->val.d_datetime.hour,
                    fd->val.d_datetime.minute,
                    fd->val.d_datetime.second);
            break;
            
        default:
            {
                //TODO:暂时不支持blob数据
                ETS_LOG_ERROR("do not support data, key type:%d", fd->key.type);
                
                status = ETS_ENSURPPT;
                goto finish;
            }
            break;
        }
        
        ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
        offset += len;

        valid_nr++;
    }

    if (!valid_nr)
    {
        ETS_LOG_ERROR("no valid update field, tbl:%s", tbl);
        status = ETS_EINVAL;
        
        goto finish;
    }

    ETS_LOG_DEBUG("valid_nr:%d", valid_nr);
    
    len = ets_snprintf_s(buf + offset, buf_size - offset, " WHERE ");
    ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
    offset += len;

    valid_nr = 0;
    for (idx = 0; idx < fdc_count; idx++)
    {
        fd = fdc[idx];
        if (!fd->is_active)
        {
            continue;
        }

        switch (fd->key.type)
        {
        case ETS_DBTBL_FIELD_TYPE_TINY:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%hhu", (valid_nr)? cond[fd->cond] : "", fd->key.name, fd->val.d_u8);
            break;
        
        case ETS_DBTBL_FIELD_TYPE_SHORT:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%hu", (valid_nr)? cond[fd->cond] : "", fd->key.name, fd->val.d_u16);
            break;

        case ETS_DBTBL_FIELD_TYPE_LONG:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%u", (valid_nr)? cond[fd->cond] : "", fd->key.name, fd->val.d_u32);
            break;

        case ETS_DBTBL_FIELD_TYPE_LONGLONG:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%llu", (valid_nr)? cond[fd->cond] : "", fd->key.name, fd->val.d_u64);
            break;

        case ETS_DBTBL_FIELD_TYPE_FLOAT:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%f", (valid_nr)? cond[fd->cond] : "", fd->key.name, fd->val.d_float);
            break;
        
        case ETS_DBTBL_FIELD_TYPE_DOUBLE:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%f", (valid_nr)? cond[fd->cond] : "", fd->key.name, fd->val.d_double);
            break;

        case ETS_DBTBL_FIELD_TYPE_STRING:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s='%s'", (valid_nr)? cond[fd->cond] : "", fd->key.name, fd->val.d_str.buf);
            break;

        case ETS_DBTBL_FIELD_TYPE_TINY_BLOB:
        case ETS_DBTBL_FIELD_TYPE_BLOB:
        case ETS_DBTBL_FIELD_TYPE_MEDIUM_BLOB:
        case ETS_DBTBL_FIELD_TYPE_LONG_BLOB:
            {
                //TODO:暂时不支持blob数据
                ETS_LOG_ERROR("do not support blob data");
                
                status = ETS_ENSURPPT;
                goto finish;
            }
            break;
            
        default:
            {
                //TODO:暂时不支持blob数据
                ETS_LOG_ERROR("do not support data, key type:%d", fd->key.type);
                
                status = ETS_ENSURPPT;
                goto finish;
            }
            break;
        }
        
        ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
        offset += len;

        valid_nr++;
    }

    if (!valid_nr)
    {
        ETS_LOG_ERROR("no valid update field, tbl:%s", tbl);
        status = ETS_EINVAL;
        
        goto finish;
    }

    ETS_LOG_DEBUG("valid_nr:%d", valid_nr);
    
    ETS_LOG_DEBUG("build update success, offset:%d, sql:%s", offset, buf);

    status = ETS_db_cmd_Execute(ctx, buf, NULL, NULL);
    if (status)
    {
        ETS_LOG_ERROR("ETS_db_cmd_Execute failed, status:%d", status);
        goto finish;
    }

    ETS_LOG_DEBUG("ETS_db_cmd_Execute success");
    status = ETS_SUCCESS;
    
finish:
    dbc->do_unlock(&dbc->lock);
    return status;
}

int32_t ETS_db_opr_Delete(void* ctx, const char* tbl,
    ETS_DBTBL_FIELD_DESC_S** fdc, int32_t fdc_count)
{
    int32_t status = ETS_EIO;
    ETS_DB_CTX_S* dbc = NULL;
    char* buf = NULL;
    int32_t buf_size = 0;
    int32_t len = 0;
    int32_t offset = 0;
    int32_t idx = 0;
    ETS_DBTBL_FIELD_DESC_S* fd = NULL;
    int32_t valid_nr = 0;
    char* cond[] = {" AND ", " AND ", " OR "};
    
    ETS_RETURN_IF_PTR_NULL(ctx, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(tbl, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(fdc, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(!strlen(tbl), ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(fdc_count<=0, ETS_EINVAL);
    
    ETS_LOG_DEBUG("ETS_db_opr_Delete, fdc_count:%d, tbl:%s", fdc_count, tbl);

    dbc = (ETS_DB_CTX_S*)ctx;
    (void)dbc->do_lock(&dbc->lock);
    
    buf = dbc->cache;
    buf_size = dbc->cache_size;
    ETS_MEMSET(buf, 0, buf_size);

    len = ets_snprintf_s(buf + offset, buf_size - offset, "DELETE FROM %s ", tbl);
    ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
    offset += len;

    len = ets_snprintf_s(buf + offset, buf_size - offset, " WHERE ");
    ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
    offset += len;

    valid_nr = 0;
    for (idx = 0; idx < fdc_count; idx++)
    {
        fd = fdc[idx];
        if (!fd->is_active)
        {
            continue;
        }

        switch (fd->key.type)
        {
        case ETS_DBTBL_FIELD_TYPE_TINY:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%hhu", (valid_nr)? cond[fd->cond] : "", fd->key.name, fd->val.d_u8);
            break;
        
        case ETS_DBTBL_FIELD_TYPE_SHORT:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%hu", (valid_nr)? cond[fd->cond] : "", fd->key.name, fd->val.d_u16);
            break;

        case ETS_DBTBL_FIELD_TYPE_LONG:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%u", (valid_nr)? cond[fd->cond] : "", fd->key.name, fd->val.d_u32);
            break;

        case ETS_DBTBL_FIELD_TYPE_LONGLONG:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%llu", (valid_nr)? cond[fd->cond] : "", fd->key.name, fd->val.d_u64);
            break;

        case ETS_DBTBL_FIELD_TYPE_FLOAT:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%f", (valid_nr)? cond[fd->cond] : "", fd->key.name, fd->val.d_float);
            break;
        
        case ETS_DBTBL_FIELD_TYPE_DOUBLE:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s=%f", (valid_nr)? cond[fd->cond] : "", fd->key.name, fd->val.d_double);
            break;

        case ETS_DBTBL_FIELD_TYPE_STRING:
            len = ets_snprintf_s(buf + offset, buf_size - offset,
                "%s%s='%s'", (valid_nr)? cond[fd->cond] : "", fd->key.name, fd->val.d_str.buf);
            break;

        case ETS_DBTBL_FIELD_TYPE_TINY_BLOB:
        case ETS_DBTBL_FIELD_TYPE_BLOB:
        case ETS_DBTBL_FIELD_TYPE_MEDIUM_BLOB:
        case ETS_DBTBL_FIELD_TYPE_LONG_BLOB:
            {
                //TODO:暂时不支持blob数据
                ETS_LOG_ERROR("do not support blob data");
                
                status = ETS_ENSURPPT;
                goto finish;
            }
            break;
            
        default:
            {
                //TODO:暂时不支持blob数据
                ETS_LOG_ERROR("do not support data, key type:%d", fd->key.type);
                
                status = ETS_ENSURPPT;
                goto finish;
            }
            break;
        }
        
        ETS_JUMP_IF_CONDITION_TURE((len <= 0 || len == (buf_size - offset)), finish);
        offset += len;

        valid_nr++;
    }

    if (!valid_nr)
    {
        ETS_LOG_ERROR("no valid delete field, tbl:%s", tbl);
        status = ETS_EINVAL;
        
        goto finish;
    }

    ETS_LOG_DEBUG("valid_nr:%d", valid_nr);
    
    ETS_LOG_DEBUG("build delete success, offset:%d, sql:%s", offset, buf);

    status = ETS_db_cmd_Execute(ctx, buf, NULL, NULL);
    if (status)
    {
        ETS_LOG_ERROR("ETS_db_cmd_Execute failed, status:%d", status);
        goto finish;
    }

    ETS_LOG_DEBUG("ETS_db_cmd_Execute success");
    status = ETS_SUCCESS;
    
finish:
    dbc->do_unlock(&dbc->lock);
    return status;
}

int32_t ETS_db_opr_BindFields(ETS_DBTBL_FIELD_DESC_S** field_desc,
    int32_t count, MYSQL_BIND* my_bind, void* ud)
{
    int32_t status = 0;
    int32_t idx = 0;
    ETS_DBTBL_FIELD_DESC_S* fd = NULL;
    
    ETS_RETURN_IF_PTR_NULL(field_desc, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(my_bind, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(count<=0, ETS_EINVAL);
    
    for (idx = 0; idx < count; idx++)
    {
        fd = field_desc[idx];

        switch (fd->key.type)
        {
        case ETS_DBTBL_FIELD_TYPE_TINY:
            my_bind[idx].buffer_type  = MYSQL_TYPE_TINY;
            my_bind[idx].buffer       = (void*)&fd->val.d_u8;
            break;
            
        case ETS_DBTBL_FIELD_TYPE_SHORT:
            my_bind[idx].buffer_type  = MYSQL_TYPE_SHORT;
            my_bind[idx].buffer       = (void*)&fd->val.d_u16;
            break;

        case ETS_DBTBL_FIELD_TYPE_LONG:
            my_bind[idx].buffer_type  = MYSQL_TYPE_LONG;
            my_bind[idx].buffer       = (void*)&fd->val.d_u32;
            break;

        case ETS_DBTBL_FIELD_TYPE_LONGLONG:
            my_bind[idx].buffer_type  = MYSQL_TYPE_LONGLONG;
            my_bind[idx].buffer       = (void*)&fd->val.d_u64;
            break;

        case ETS_DBTBL_FIELD_TYPE_FLOAT:
            my_bind[idx].buffer_type  = MYSQL_TYPE_FLOAT;
            my_bind[idx].buffer       = (void*)&fd->val.d_float;
            break;
        
        case ETS_DBTBL_FIELD_TYPE_DOUBLE:
            my_bind[idx].buffer_type  = MYSQL_TYPE_DOUBLE;
            my_bind[idx].buffer       = (void*)&fd->val.d_double;
            break;

        case ETS_DBTBL_FIELD_TYPE_STRING:
            my_bind[idx].buffer_type  = MYSQL_TYPE_VAR_STRING;
            my_bind[idx].buffer       = (void*)fd->val.d_str.buf;
            my_bind[idx].buffer_length = fd->val.d_str.size;
            break;

        case ETS_DBTBL_FIELD_TYPE_TINY_BLOB:
            my_bind[idx].buffer_type  = MYSQL_TYPE_TINY_BLOB;
            my_bind[idx].buffer       = (void*)fd->val.d_blob.buf;
            my_bind[idx].buffer_length = fd->val.d_blob.size;
            break;
            
        case ETS_DBTBL_FIELD_TYPE_BLOB:
            my_bind[idx].buffer_type  = MYSQL_TYPE_BLOB;
            my_bind[idx].buffer       = (void*)fd->val.d_blob.buf;
            my_bind[idx].buffer_length = fd->val.d_blob.size;
            break;
            
        case ETS_DBTBL_FIELD_TYPE_MEDIUM_BLOB:
            my_bind[idx].buffer_type  = MYSQL_TYPE_MEDIUM_BLOB;
            my_bind[idx].buffer       = (void*)fd->val.d_blob.buf;
            my_bind[idx].buffer_length = fd->val.d_blob.size;
            break;
            
        case ETS_DBTBL_FIELD_TYPE_LONG_BLOB:
            my_bind[idx].buffer_type  = MYSQL_TYPE_LONG_BLOB;
            my_bind[idx].buffer       = (void*)fd->val.d_blob.buf;
            my_bind[idx].buffer_length = fd->val.d_blob.size;
            break;

        case ETS_DBTBL_FIELD_TYPE_DATE:
            my_bind[idx].buffer_type  = MYSQL_TYPE_DATE;
            my_bind[idx].buffer       = (void*)&fd->val.d_datetime;
            my_bind[idx].buffer_length = sizeof(fd->val.d_datetime);
            break;

        case ETS_DBTBL_FIELD_TYPE_TIME:
            my_bind[idx].buffer_type  = MYSQL_TYPE_TIME;
            my_bind[idx].buffer       = (void*)&fd->val.d_datetime;
            my_bind[idx].buffer_length = sizeof(fd->val.d_datetime);
            break;

        case ETS_DBTBL_FIELD_TYPE_DATETIME:
            my_bind[idx].buffer_type  = MYSQL_TYPE_DATETIME;
            my_bind[idx].buffer       = (void*)&fd->val.d_datetime;
            my_bind[idx].buffer_length = sizeof(fd->val.d_datetime);
            break;
            
        default:
            break;
        }
    }
    
    return status;
}

int32_t ETS_db_opr_PrintField(int32_t row, ETS_DBTBL_FIELD_DESC_S** fds,
    int32_t count, void* ud)
{
    int32_t idx = 0;
    ETS_DBTBL_FIELD_DESC_S* field_desc = NULL;
    
    ETS_RETURN_IF_PTR_NULL(fds, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(count<=0, ETS_EINVAL);

    for (idx = 0; idx < count; idx++)
    {
        field_desc = fds[idx];

        switch (field_desc->key.type)
        {
        case ETS_DBTBL_FIELD_TYPE_TINY:
            ETS_LOG_DEBUG("%-10d : %-15s : %hhu", idx, field_desc->key.name, field_desc->val.d_u8);
            break;
        
        case ETS_DBTBL_FIELD_TYPE_SHORT:
            ETS_LOG_DEBUG("%-10d : %-15s : %hu", idx, field_desc->key.name, field_desc->val.d_u16);
            break;

        case ETS_DBTBL_FIELD_TYPE_LONG:
            ETS_LOG_DEBUG("%-10d : %-15s : %u", idx, field_desc->key.name, field_desc->val.d_u32);
            break;

        case ETS_DBTBL_FIELD_TYPE_LONGLONG:
            ETS_LOG_DEBUG("%-10d : %-15s : %lu", idx, field_desc->key.name, field_desc->val.d_u64);
            break;

        case ETS_DBTBL_FIELD_TYPE_FLOAT:
            ETS_LOG_DEBUG("%-10d : %-15s : %.5f", idx, field_desc->key.name, field_desc->val.d_float);
            break;
        
        case ETS_DBTBL_FIELD_TYPE_DOUBLE:
            ETS_LOG_DEBUG("%-10d : %-15s : %.5f", idx, field_desc->key.name, field_desc->val.d_double);
            break;

        case ETS_DBTBL_FIELD_TYPE_STRING:
            ETS_LOG_DEBUG("%-10d : %-15s : %s", idx, field_desc->key.name, (field_desc->val.d_str.buf)? field_desc->val.d_str.buf : "---");
            break;

        case ETS_DBTBL_FIELD_TYPE_TINY_BLOB:
        case ETS_DBTBL_FIELD_TYPE_BLOB:
        case ETS_DBTBL_FIELD_TYPE_MEDIUM_BLOB:
        case ETS_DBTBL_FIELD_TYPE_LONG_BLOB:
            break;

        case ETS_DBTBL_FIELD_TYPE_DATE:
            ETS_LOG_DEBUG("%-10d : %-15s : %04u-%02u-%02u", idx, field_desc->key.name,
                field_desc->val.d_datetime.year,
                field_desc->val.d_datetime.month,
                field_desc->val.d_datetime.day);
            break;

        case ETS_DBTBL_FIELD_TYPE_TIME:
            ETS_LOG_DEBUG("%-10d : %-15s : %02u:%02u:%02u", idx, field_desc->key.name,
                field_desc->val.d_datetime.hour,
                field_desc->val.d_datetime.minute,
                field_desc->val.d_datetime.second);
            break;

        case ETS_DBTBL_FIELD_TYPE_DATETIME:
            ETS_LOG_DEBUG("%-10d : %-15s : %04u-%02u-%02u %02u:%02u:%02u", idx, field_desc->key.name,
                field_desc->val.d_datetime.year,
                field_desc->val.d_datetime.month,
                field_desc->val.d_datetime.day,
                field_desc->val.d_datetime.hour,
                field_desc->val.d_datetime.minute,
                field_desc->val.d_datetime.second);
            break;
            
        default:
            break;
        }
    }
    
    (void)ud;
    return 0;
}

int32_t ETS_db_opr_Query(void* ctx, const char* sql,
    ETS_DBTBL_FIELD_DESC_S** field_desc, int32_t fc,
    int32_t (*fill_bind)(ETS_DBTBL_FIELD_DESC_S**, int32_t, MYSQL_BIND*, void*),
    int32_t (*callback)(int32_t, ETS_DBTBL_FIELD_DESC_S**, int32_t, void*),
    void* ud)
{
    int32_t status = 0;
    ETS_DB_CTX_S* dbc = NULL;
    MYSQL_STMT* stmt = NULL;
    MYSQL_BIND* my_bind = NULL;
    int32_t bind_count = 0;
    int32_t row = 0;
    int32_t (*do_bind)(ETS_DBTBL_FIELD_DESC_S**, int32_t, MYSQL_BIND*, void*) = (fill_bind)?
        fill_bind : ETS_db_opr_BindFields;
    int32_t (*do_callback)(int32_t, ETS_DBTBL_FIELD_DESC_S**, int32_t, void*) = (callback)?
        callback : ETS_db_opr_PrintField;
    
    ETS_RETURN_IF_PTR_NULL(ctx, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(sql, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(field_desc, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(!strlen(sql), ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(fc<=0, ETS_EINVAL);
    
    ETS_LOG_DEBUG("ETS_db_cmd_Query_v2, fc:%d, sql:'%s'", fc, sql);

    my_bind = (MYSQL_BIND*)ETS_MALLOC(sizeof(MYSQL_BIND) * fc);
    ETS_RETURN_IF_PTR_NULL(my_bind, ETS_ENOMEM);
    ETS_MEMSET(my_bind, 0, sizeof(MYSQL_BIND) * fc);

    dbc = (ETS_DB_CTX_S*)ctx;
    (void)dbc->do_lock(&dbc->lock);

    status = dbc->init(dbc, dbc->pri);
    if (status)
    {
        ETS_LOG_ERROR("init failed, status:%d", status);
        goto finish;
    }

    status = dbc->connect(dbc, dbc->pri);
    if (status)
    {
        ETS_LOG_ERROR("connect failed, status:%d", status);
        goto finish;
    }

    stmt = ets_db_stmt_Prepare(dbc->mysql, sql);
    if (!stmt)
    {
        ETS_LOG_ERROR("ets_db_stmt_Prepare failed, status:%d, errno:'%s'",
            status, mysql_error(dbc->mysql));
        status = ETS_EFAILED;
        goto finish;
    }

    status = mysql_stmt_execute(stmt);
    if (status)
    {
        ETS_LOG_ERROR("mysql_stmt_execute failed, status:%d, errno:'%s'",
            status, mysql_error(dbc->mysql));
        
        status = ETS_EFAILED;
        goto finish;
    }

    status = do_bind(field_desc, fc, my_bind, ud);
    if (status)
    {
        ETS_LOG_ERROR("do_bind failed, status:%d", status);
        
        status = ETS_EFAILED;
        goto finish;
    }

    ETS_LOG_DEBUG("fill_bind success, bind_count:%d", bind_count);
    
    status = mysql_stmt_bind_result(stmt, my_bind);
    if (status)
    {
        ETS_LOG_ERROR("mysql_stmt_bind_result failed, status:%d, errno:'%s'",
            status, mysql_error(dbc->mysql));
        
        status = ETS_EFAILED;
        goto finish;
    }

    ETS_LOG_DEBUG("mysql_stmt_bind_result success");
    
    while (MYSQL_NO_DATA != mysql_stmt_fetch(stmt))
    {
        (void)do_callback(row, field_desc, fc, ud);
        row++;
    }
    
finish:
    if (stmt)
    {
        (void)mysql_stmt_close(stmt);
        stmt = NULL;
    }
    dbc->do_unlock(&dbc->lock);
    ETS_FREE(my_bind);
    
    return status;
}

static void __null_func__(void){}
