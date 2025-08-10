/*************************************************************************
*Copyright (C), 2025-2035, tanhuang
**************************************************************************
*@文件名称:
*   ets_mutex.h
*@文件描述:
*   mutex锁操作相关数据结构及函数接口声明头文件
*@版本信息:
*   v0.1
*@修改历史:
*   1).created by tanhuang, 2025/06/25 21:30:13
*************************************************************************/
#ifndef __ETS_MUTEX_H__
#define __ETS_MUTEX_H__

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#ifndef ETS_EXPORT_PUB
    #define ETS_PUB_API __declspec(dllimport)
#else
    #define ETS_PUB_API __declspec(dllexport)
#endif

struct ETS_PUB_API ets_mutex_t
{
    HANDLE lock;
};

int32_t ETS_PUB_API ETS_Mutex_Init(struct ets_mutex_t* mutex);
void    ETS_PUB_API ETS_Mutex_Exit(struct ets_mutex_t* mutex);
int32_t ETS_PUB_API ETS_Mutex_Lock(struct ets_mutex_t* mutex);
void    ETS_PUB_API ETS_Mutex_Unlock(struct ets_mutex_t* mutex);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif

