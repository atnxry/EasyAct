/*************************************************************************
*Copyright (C), 2025-2035, tanhuang
**************************************************************************
*@文件名称:
*   ETS_atomic.h
*@文件描述:
*   原子操作相关数据结构及函数接口声明头文件
*@版本信息:
*   v0.1
*@修改历史:
*   1).created by tanhuang, 2025/01/05
*************************************************************************/
#ifndef __ETS_ATOMIC_H__
#define __ETS_ATOMIC_H__

#include <intrin.h>

typedef struct ets_atomic32
{
    volatile long count;
}ets_atomic32_t;

typedef struct ets_atomic64
{
    volatile int64_t count;
}ets_atomic64_t;

#define ets_atomic32_rd(_n32_)        _InterlockedOr(&(_n32_)->count, 0)
#define ets_atomic32_wr(_v32_, _n32_) _InterlockedExchange(&(_v32_)->count, (_n32_))
#define ets_atomic32_inc(_v32_)       _InterlockedIncrement(&(_v32_)->count)
#define ets_atomic32_dec(_v32_)       _InterlockedDecrement(&(_v32_)->count)
#define ets_atomic64_rd(_v64_)        _InterlockedOr64(&(_v64_)->count, 0)
#define ets_atomic64_wr(_v64_, _n64_) _InterlockedExchange64(&(_v64_)->count, (_n64_))
#define ets_atomic64_inc(_v64_)       _InterlockedIncrement64(&(_v64_)->count)
#define ets_atomic64_dec(_v64_)       _InterlockedDecrement64(&(_v64_)->count)

#endif
