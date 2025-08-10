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
#ifndef __ETS_LOG_H__
#define __ETS_LOG_H__

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#ifndef ETS_EXPORT_PUB
    #define ETS_PUB_API __declspec(dllimport)
#else
    #define ETS_PUB_API __declspec(dllexport)
#endif

typedef ETS_PUB_API enum
{
    ETS_LOG_LEVEL_DEBUG ,
    ETS_LOG_LEVEL_INFO  ,
    ETS_LOG_LEVEL_WARN  ,
    ETS_LOG_LEVEL_ERROR ,
    ETS_LOG_LEVEL_FATAL ,
}ETS_LOG_LEVEL_E;

int32_t ETS_PUB_API ETS_log_Init(const char* file, const char* file_back, uint32_t max_file_size);
void    ETS_PUB_API ETS_log_Exit(void);
void    ETS_PUB_API ETS_log_LevelSet(int32_t level);
int32_t ETS_PUB_API ETS_log_LevelGet(void);
int32_t ETS_PUB_API ETS_log_Write(int32_t level, const char* fmt, ...);

#define ETS_LOG_DEBUG(fmt, ...) (void)ETS_log_Write(ETS_LOG_LEVEL_DEBUG, fmt, ####__VA_ARGS__)
#define ETS_LOG_INFO(fmt , ...) (void)ETS_log_Write(ETS_LOG_LEVEL_INFO , fmt, ####__VA_ARGS__)
#define ETS_LOG_WARN(fmt , ...) (void)ETS_log_Write(ETS_LOG_LEVEL_WARN , fmt, ####__VA_ARGS__)
#define ETS_LOG_ERROR(fmt, ...) (void)ETS_log_Write(ETS_LOG_LEVEL_ERROR, fmt, ####__VA_ARGS__)
#define ETS_LOG_FATAL(fmt, ...) (void)ETS_log_Write(ETS_LOG_LEVEL_FATAL, fmt, ####__VA_ARGS__)

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif