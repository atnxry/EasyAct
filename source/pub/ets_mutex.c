/*************************************************************************
*Copyright (C), 2025-2035, tanhuang
**************************************************************************
*@文件名称:
*   ets_mutex.c
*@文件描述:
*   mutex锁操作相关函数接口实现源文件
*@版本信息:
*   v0.1
*@修改历史:
*   1).created by tanhuang, 2025/06/25 21:30:13
*************************************************************************/
#include <windows.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "ets_pub.h"
#include "ets_log.h"
#include "ets_mutex.h"

int32_t ETS_Mutex_Init(struct ets_mutex_t* mutex)
{
    ETS_RETURN_IF_PTR_NULL(mutex, ETS_EINVAL);
    ETS_RETURN_IF_CONDITION_TURE(NULL!=mutex->lock, ETS_EINVAL);

    mutex->lock = CreateMutex(NULL, FALSE, NULL);
    ETS_RETURN_IF_PTR_NULL(mutex->lock, ETS_EINVAL);
    
    return ETS_SUCCESS;
}

void ETS_Mutex_Exit(struct ets_mutex_t* mutex)
{
    ETS_CHECK_PTR_NULL(mutex);
    ETS_CHECK_PTR_NULL(mutex->lock);

    ETS_CLOSE_HANDLE(mutex->lock);
    return;
}

int32_t ETS_Mutex_Lock(struct ets_mutex_t* mutex)
{
    DWORD status = 0;
    
    ETS_RETURN_IF_PTR_NULL(mutex, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(mutex->lock, ETS_EINVAL);

    status = WaitForSingleObject(mutex->lock, INFINITE);
    switch (status)
    {
    case WAIT_OBJECT_0:
        {
            return ETS_SUCCESS;
        }
        break;

    case WAIT_TIMEOUT:
        {
            ETS_LOG_WARN("ETS_Mutex_Lock timeout");
            return ETS_ETIMEOUT;
        }
        break;
    
    case WAIT_FAILED:
    case WAIT_ABANDONED:
    default:
        ETS_LOG_ERROR("ETS_Mutex_Lock failed, status:%d", status);
        break;
    }

    return ETS_EFAILED;
}

void ETS_Mutex_Unlock(struct ets_mutex_t* mutex)
{
    ETS_CHECK_PTR_NULL(mutex);
    ETS_CHECK_PTR_NULL(mutex->lock);
    
    ReleaseMutex(mutex->lock);
    return;
}

