/*************************************************************************
*Copyright (C), 2025-2035, tanhuang
**************************************************************************
*@文件名称:
*   ets_db.h
*@文件描述:
*   数据库相关数据结构及函数接口声明头文件
*@版本信息:
*   v0.1
*@修改历史:
*   1).created by tanhuang, 2025/06/07
*************************************************************************/
#ifndef __ETS_DB_H__
#define __ETS_DB_H__

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#ifndef ETS_EXPORT_DB
    #define ETS_DB_API __declspec(dllimport)
#else
    #define ETS_DB_API __declspec(dllexport)
#endif

int32_t ETS_DB_API ETS_db_env_Init(void);
void ETS_DB_API ETS_db_env_Exit(void);
void ETS_DB_API ETS_db_env_Dump(void);

typedef struct ETS_DB_API __tagETS_DB_CTX_PARAM_S
{
    char* host;
    char* user;
    char* passwd;
    char* database;
}ETS_DB_CTX_PARAM_S;

int32_t ETS_DB_API ETS_db_ctx_New(ETS_DB_CTX_PARAM_S* param, void** ctx);
void ETS_DB_API ETS_db_ctx_Del(void** ctx);

int32_t ETS_DB_API ETS_db_cmd_Delete(void* ctx, const char* sql,
    int32_t (*fill_bind)(MYSQL_BIND**, int32_t*, void*), void* ud);
int32_t ETS_DB_API ETS_db_cmd_Update(void* ctx, const char* sql,
    int32_t (*fill_bind)(MYSQL_BIND**, int32_t*, void*), void* ud);
int32_t ETS_DB_API ETS_db_cmd_Query(void* ctx, const char* sql,
    int32_t (*fill_bind)(MYSQL_BIND**, int32_t*, void*),
    int32_t (*callback)(void*), void* ud);
int32_t ETS_DB_API ETS_db_cmd_Insert(void* ctx, const char* sql,
    int32_t (*fill_bind)(MYSQL_BIND**, int32_t*, void*), void* ud);
int32_t ETS_DB_API ETS_db_cmd_Execute(void* ctx, const char* sql,
    int32_t (*cb)(void* , void*), void* ud);

///////////////////////////////////////////////////////////////////////////////////
typedef enum __tagETS_DBTBL_FIELD_TYPE_E
{
    ETS_DBTBL_FIELD_TYPE_TINY       = 0,
    ETS_DBTBL_FIELD_TYPE_SHORT      = 1,
    ETS_DBTBL_FIELD_TYPE_LONG       = 2,
    ETS_DBTBL_FIELD_TYPE_LONGLONG   = 3,
    ETS_DBTBL_FIELD_TYPE_FLOAT      = 4,
    ETS_DBTBL_FIELD_TYPE_DOUBLE     = 5,
    ETS_DBTBL_FIELD_TYPE_STRING     = 6,
    ETS_DBTBL_FIELD_TYPE_TINY_BLOB  = 7,
    ETS_DBTBL_FIELD_TYPE_BLOB       = 8,
    ETS_DBTBL_FIELD_TYPE_MEDIUM_BLOB= 9,
    ETS_DBTBL_FIELD_TYPE_LONG_BLOB  = 10,
    ETS_DBTBL_FIELD_TYPE_DATE       = 11, //YYYY-MM-DD
    ETS_DBTBL_FIELD_TYPE_TIME       = 12, //HH:MM:SS
    ETS_DBTBL_FIELD_TYPE_DATETIME   = 13, //YYYY-MM-DD HH:MM:SS
    ETS_DBTBL_FIELD_TYPE_TIMESTAMP  = 14, //格式与 DATETIME 相同

    ETS_DBTBL_FIELD_TYPE_MAX
}ETS_DBTBL_FIELD_TYPE_E;

typedef enum __tagETS_DBTBL_FIELD_COND_E
{
    ETS_DBTBL_FIELD_COND_UNK = 0x0,
    ETS_DBTBL_FIELD_COND_AND = 0x1,
    ETS_DBTBL_FIELD_COND_OR  = 0x2,
}ETS_DBTBL_FIELD_COND_E;

typedef struct __tagETS_DBTBL_FIELD_DESC_S
{
    uint8_t is_active;
    ETS_DBTBL_FIELD_COND_E cond;
    
    struct
    {
        char* name;
        ETS_DBTBL_FIELD_TYPE_E type;
        uint8_t is_null;
        uint8_t auto_increment;
        uint8_t is_primary_key;
        unsigned long max_length;
    }key;

    struct
    {
        union
        {
            uint8_t  d_u8;      //MYSQL_TYPE_TINY
            uint16_t d_u16;     //MYSQL_TYPE_SHORT
            uint32_t d_u32;     //MYSQL_TYPE_LONG
            uint64_t d_u64;     //MYSQL_TYPE_LONGLONG
            float    d_float;   //MYSQL_TYPE_FLOAT
            double   d_double;  //MYSQL_TYPE_DOUBLE
            struct
            {
                char* buf;
                int32_t size;
                int32_t len;
            }d_str;             //MYSQL_TYPE_STRING;
            struct
            {
                uint8_t* buf;
                int32_t size;
                int32_t len;
            }d_blob;            //MYSQL_TYPE_TINY_BLOB;
            MYSQL_TIME d_datetime;
        };
    }val;
}ETS_DBTBL_FIELD_DESC_S;

int32_t ETS_DB_API ETS_db_opr_CreateTable(void* ctx, const char* tbl,
    ETS_DBTBL_FIELD_DESC_S* fields, int32_t count);
int32_t ETS_DB_API ETS_db_opr_GetFieldsDesc(void* ctx, const char* tbl,
    int32_t (*callback)(int32_t, ETS_DBTBL_FIELD_DESC_S**, void*),
    int32_t (*iterator)(int32_t, ETS_DBTBL_FIELD_DESC_S*, void*),
    void* ud);
int32_t ETS_DB_API ETS_db_opr_CopyFieldsDesc(ETS_DBTBL_FIELD_DESC_S** src, int32_t sc,
    ETS_DBTBL_FIELD_DESC_S*** dst, int32_t* dc);
void ETS_DB_API ETS_db_opr_FreeFieldInfo(ETS_DBTBL_FIELD_DESC_S** field_desc);
void ETS_DB_API ETS_db_opr_DumpFieldInfo(ETS_DBTBL_FIELD_DESC_S* field_desc);
void ETS_DB_API ETS_db_opr_FreeFieldsInfo(ETS_DBTBL_FIELD_DESC_S*** field_desc, int32_t count);
void ETS_DB_API ETS_db_opr_DumpFieldsInfo(ETS_DBTBL_FIELD_DESC_S** field_desc, int32_t count);

int32_t ETS_db_opr_BindFields(ETS_DBTBL_FIELD_DESC_S** field_desc,
    int32_t count, MYSQL_BIND* my_bind, void* ud);
int32_t ETS_db_opr_PrintField(int32_t row, ETS_DBTBL_FIELD_DESC_S** fds,
    int32_t count, void* ud);

int32_t ETS_DB_API ETS_db_opr_Insert(void* ctx, const char* tbl,
    ETS_DBTBL_FIELD_DESC_S** field_desc, int32_t count);
int32_t ETS_DB_API ETS_db_opr_Update(void* ctx, const char* tbl,
    ETS_DBTBL_FIELD_DESC_S** fdu, int32_t fdu_count,
    ETS_DBTBL_FIELD_DESC_S** fdc, int32_t fdc_count);
int32_t ETS_DB_API ETS_db_opr_Delete(void* ctx, const char* tbl,
    ETS_DBTBL_FIELD_DESC_S** fdc, int32_t fdc_count);
int32_t ETS_DB_API ETS_db_opr_Query(void* ctx, const char* sql,
    ETS_DBTBL_FIELD_DESC_S** field_desc, int32_t fc,
    int32_t (*fill_bind)(ETS_DBTBL_FIELD_DESC_S**, int32_t, MYSQL_BIND*, void*),
    int32_t (*callback)(int32_t, ETS_DBTBL_FIELD_DESC_S**, int32_t, void*),
    void* ud);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif
