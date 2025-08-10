/*************************************************************************
*Copyright (C), 2025-2035, tanhuang
**************************************************************************
*@文件名称:
*   ets_log.h
*@文件描述:
*   日志相关数据结构及函数接口声明头文件
*@版本信息:
*   v0.1
*@修改历史:
*   1).created by tanhuang, 2025/01/05
*************************************************************************/
#include <windows.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "ets_pub.h"
#include "ets_log.h"

static struct
{
    char* file;
    char* file_back;
    int32_t max_file_size;//unit:bytes
    char* buf_ptr;
    int32_t buf_size;
    CRITICAL_SECTION lock;
    ets_atomic32_t level;
    ets_atomic32_t init;
}ets_lc = {0};

#define ETS_DECALRE_LEVEL_MARK() {"debug", "info", "warn", "error", "fatal"}

int32_t ETS_log_Write(int32_t level, const char* fmt, ...)
{
    int32_t len = 0;
    int32_t offset = 0;
    FILE* fp = NULL;
    const char* marks[] = ETS_DECALRE_LEVEL_MARK();
    char* buf_ptr = ets_lc.buf_ptr;
    int32_t buf_size = ets_lc.buf_size;
    SYSTEMTIME cts = {0};
    DWORD taskId = GetCurrentThreadId();
    struct _stat st = {0};
    
    if ((!ets_atomic32_rd(&ets_lc.init)) || level < ets_atomic32_rd(&ets_lc.level))
    {
        return ETS_SUCCESS;
    }

    GetLocalTime(&cts);
    EnterCriticalSection(&ets_lc.lock);
    
    offset = ets_snprintf(buf_ptr, buf_size, "[%04d-%02d-%02d %02d:%02d:%02d][%s][%ld]:",
        cts.wYear, cts.wMonth, cts.wDay,
        cts.wHour, cts.wMinute, cts.wSecond, marks[level], taskId);
    if (offset <= 0 || buf_size == offset)
    {
        LeaveCriticalSection(&ets_lc.lock);
        return ETS_EIO;
    }

    len += offset;

    va_list marker;
    va_start(marker, fmt);

    offset = vsprintf(buf_ptr + offset, fmt, marker);
    //offset = _snprintf(buffer + offset, 2046 - len, fmt, marker);
    //if (offset <= 0 || 2046 - len == offset)
    if (offset <= 0)
    {
        LeaveCriticalSection(&ets_lc.lock);
        return ETS_EIO;
    }

    va_end(marker);

    len += offset;
    buf_ptr[len] = '\n';
    buf_ptr[len + 1] = '\0';

    ETS_MEMSET(&st, 0, sizeof(struct _stat));
    if (!_stat(ets_lc.file, &st))
    {
        if (st.st_size > ets_lc.max_file_size)
        {
            _unlink(ets_lc.file_back);
            (void)rename(ets_lc.file, ets_lc.file_back);
        }
    }
    
    fp = fopen(ets_lc.file, "a+");
    if (!fp)
    {
        goto back;
    }

    (void)fwrite(buf_ptr, len + 1, 1, fp);
    (void)fclose(fp);
    
    (void)fprintf(stdout, "%s", buf_ptr);
    
back:
    LeaveCriticalSection(&ets_lc.lock);
    return ETS_SUCCESS;
}

void ETS_log_LevelSet(int32_t level)
{
    ets_atomic32_wr(&ets_lc.level, level);
    return;
}

int32_t ETS_log_LevelGet(void)
{
    return ets_atomic32_rd(&ets_lc.level);
}

int32_t ETS_log_Init(const char* file, const char* file_back, uint32_t max_file_size)
{
    char* buf = NULL;
    int32_t buf_size = 8 * 1024;
    int32_t level = ETS_LOG_LEVEL_DEBUG;
    
    if (ets_atomic32_rd(&ets_lc.init))
    {
        return ETS_SUCCESS;
    }
    
    ETS_RETURN_IF_PTR_NULL(file, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(file_back, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(!max_file_size, ETS_EINVAL);

    buf = (char*)ETS_MALLOC(buf_size);
    ETS_RETURN_IF_PTR_NULL(buf, ETS_ENOMEM);

    ets_lc.file = ets_strdup(file);
    ets_lc.file_back = ets_strdup(file_back);
    ETS_JUMP_IF_PTR_NULL(ets_lc.file, failed);
    ETS_JUMP_IF_PTR_NULL(ets_lc.file_back, failed);

    ets_lc.max_file_size = max_file_size;
    
    ets_lc.buf_ptr = buf;
    ets_lc.buf_size= buf_size;
    InitializeCriticalSection(&ets_lc.lock);
    
    ets_atomic32_wr(&ets_lc.init, 1);
    ets_atomic32_wr(&ets_lc.level, level);
    
    return ETS_SUCCESS;

failed:
    ETS_FREE(ets_lc.file);
    ETS_FREE(ets_lc.file_back);
    ETS_FREE(buf);
    
    return ETS_ENOMEM;
}

void ETS_log_Exit(void)
{
    ets_lc.max_file_size = 0;
    ets_lc.buf_size      = 0;
    
    ETS_FREE(ets_lc.file);
    ETS_FREE(ets_lc.file_back);
    ETS_FREE(ets_lc.buf_ptr);
    DeleteCriticalSection(&ets_lc.lock);
    
    ets_atomic32_wr(&ets_lc.init, 0);
    return;
}

