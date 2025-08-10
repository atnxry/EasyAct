//#include <stdlib.h>
//#include <stdio.h>
#include <windows.h>
#include "mysql.h"
#include "ets_pub.h"
#include "ets_log.h"
#include "ets_db.h"
#include "ets_mutex.h"

static int unittest_mutex(void)
{
    int32_t status = 0;
    struct ets_mutex_t lock;

    ETS_MEMSET(&lock, 0, sizeof(struct ets_mutex_t));

    status = ETS_Mutex_Init(&lock);
    ETS_RETURN_IF_CONDITION_TURE(status, status);

    status = ETS_Mutex_Lock(&lock);
    if (status)
    {
        ETS_LOG_ERROR("ETS_Mutex_Lock first failed");
        goto finish;
    }

    ETS_LOG_DEBUG("ETS_Mutex_Lock first success");
    
    status = ETS_Mutex_Lock(&lock);
    if (status)
    {
        ETS_LOG_ERROR("ETS_Mutex_Lock second failed");
        goto finish;
    }

    ETS_LOG_DEBUG("ETS_Mutex_Lock second success");
    
finish:
    ETS_Mutex_Exit(&lock);
    return 0;
}

static int unittest_database(void)
{
    int32_t status = 0;
    struct
    {
        ETS_DB_CTX_PARAM_S param;
        void* dbc;
    }params[] =
    {
    #ifdef __ut_test__
        {{"10.198.13.101", "user1", "password1", "database1"}, NULL},
        {{"10.198.13.102", "user2", "password2", "database2"}, NULL},
        {{"10.198.13.103", "user3", "password3", "database3"}, NULL},
        {{"10.198.13.104", "user4", "password4", "database4"}, NULL},
        {{"10.198.13.105", "user5", "password5", "database5"}, NULL},
    #endif
        //create database if not exists easyact;
        //show database;
        //show tables from world
        //"SELECT * FROM world.city"
        //{"192.168.1.3", "superman", "123456", "world", "SELECT * FROM city"},
        {{"192.168.1.3", "superman", "123456", "easyact"}, NULL},
    };
    
    int32_t idx = 0;
    
    status = ETS_db_env_Init();
    ETS_RETURN_IF_CONDITION_TURE(status, status);

    for (idx = 0; idx < ETS_DIM(params); idx++)
    {
        status = ETS_db_ctx_New(&params[idx].param, &params[idx].dbc);
        if (status)
        {
            ETS_LOG_ERROR("ETS_db_ctx_New failed, idx:%d, status:%d", idx, status);
            break;
        }

        ETS_LOG_DEBUG("ETS_db_ctx_New success, idx:%d, dbc:%p", idx, params[idx].dbc);

        ETS_db_env_Dump();
    }

    for (idx = 0; idx < ETS_DIM(params); idx++)
    {
        ETS_db_ctx_Del(&params[idx].dbc);

        ETS_LOG_DEBUG("ETS_db_ctx_Del finish, idx:%d, dbc:%p", idx, params[idx].dbc);

        ETS_db_env_Dump();
    }

    ETS_LOG_DEBUG("ready ETS_db_env_Exit");
    ETS_db_env_Dump();
    
    ETS_db_env_Exit();
    return 0;
}

struct bind_ud_t
{
    int32_t index;
    char c_ProductName[256];
    int32_t c_Count;
    double c_Price;
    double c_Amount;
    double c_OtherAmountOri;
    double c_AdvanceFreight;
    double c_TotalCost;
    double c_SellPrice;
    double c_SellAmount;
    double c_GrossProfit;
    double c_GrossProfitRatio;
    MYSQL_BIND bind[11];
};

static int32_t fill_bind_insert(MYSQL_BIND** mybind, int32_t* count, void* ud)
{
    /*{
        "CREATE TABLE tbl_sale_detail ("
            "c_Id INT NOT NULL AUTO_INCREMENT, "
            "c_SupplyMonth DATE NULL, c_SupplyDate DATE NULL, c_SupplyBill VARCHAR(128) NULL, "
            "c_SupplierName VARCHAR(128) NULL, c_CustomerName VARCHAR(256) NULL, c_ProductName VARCHAR(256) NOT NULL, "
            "c_Specification VARCHAR(128) NULL, c_Unit VARCHAR(32) NULL, c_Count INT NOT NULL, "
            "c_Price DOUBLE NOT NULL, c_Amount DOUBLE NOT NULL, c_OtherAmountOri DOUBLE NOT NULL, "
            "c_AdvanceFreight DOUBLE NOT NULL, c_TotalCost DOUBLE NOT NULL, c_ProductType VARCHAR(256) NOT NULL,"
            "c_BillingMonth DATE NULL, c_BillingDate DATE NULL, c_SellBill VARCHAR(128) NOT NULL, "
            "c_SellPrice DOUBLE NOT NULL, c_SellAmount DOUBLE NOT NULL, c_GrossProfit DOUBLE NOT NULL, "
            "c_GrossProfitRatio DOUBLE NOT NULL, c_Description VARCHAR(512) NULL, "
            "PRIMARY KEY (c_Id)"
        ")", NULL, NULL
    },*/
    int32_t bnum = 11;
    MYSQL_BIND* my_bind = NULL;
    struct bind_ud_t* iud = (struct bind_ud_t*)ud;
    
    //ETS_MEMCPY(iud->c_ProductName, "product.example", strlen("product.example") + 1);
    (void)ets_snprintf_s(iud->c_ProductName, sizeof(iud->c_ProductName), "product.example.%d", iud->index);
    
    iud->c_Count = 11 * iud->index;
    iud->c_Price = 12.3 * iud->index;
    iud->c_Amount = 13.4 * iud->index;
    iud->c_OtherAmountOri = 14.5 * iud->index;
    iud->c_AdvanceFreight = 15.6 * iud->index;
    iud->c_TotalCost = 16.7 * iud->index;
    iud->c_SellPrice = 17.8 * iud->index;
    iud->c_SellAmount = 18.9 * iud->index;
    iud->c_GrossProfit = 19.21 * iud->index;
    iud->c_GrossProfitRatio = 20.23 * iud->index;
    
    my_bind = &iud->bind[0];
    ETS_MEMSET(my_bind, 0, sizeof(MYSQL_BIND) * bnum);

    /*
    "INSERT INTO tbl_sale_detail ("
        "c_ProductName, "
        "c_Count, "
        "c_Price, c_Amount, c_OtherAmountOri, "
        "c_AdvanceFreight, c_TotalCost, "
        "c_SellPrice, c_SellAmount, c_GrossProfit, "
        "c_GrossProfitRatio) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    */
    my_bind[0].buffer_type= MYSQL_TYPE_STRING;
    my_bind[0].buffer= (void*)&iud->c_ProductName[0];
    my_bind[0].buffer_length = (unsigned long)(strlen(iud->c_ProductName) + 1);

    my_bind[1].buffer_type= MYSQL_TYPE_LONG;
    my_bind[1].buffer= (void*)&iud->c_Count;
    
    my_bind[2].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[2].buffer= (void*)&iud->c_Price;

    my_bind[3].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[3].buffer= (void*)&iud->c_Amount;

    my_bind[4].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[4].buffer= (void*)&iud->c_OtherAmountOri;

    my_bind[5].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[5].buffer= (void*)&iud->c_AdvanceFreight;

    my_bind[6].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[6].buffer= (void*)&iud->c_TotalCost;

    my_bind[7].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[7].buffer= (void*)&iud->c_SellPrice;

    my_bind[8].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[8].buffer= (void*)&iud->c_SellAmount;

    my_bind[9].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[9].buffer= (void*)&iud->c_GrossProfit;

    my_bind[10].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[10].buffer= (void*)&iud->c_GrossProfitRatio;

    *mybind = my_bind;
    *count  = 11;

    ETS_LOG_DEBUG("fill_bind_insert success, *mybind:%p, *count:%d", *mybind, *count);
    return ETS_SUCCESS;
}

static int32_t fill_bind_query(MYSQL_BIND** mybind, int32_t* count, void* ud)
{
    MYSQL_BIND* my_bind = NULL;
    struct bind_ud_t* iud = (struct bind_ud_t*)ud;
    
    ETS_MEMSET(iud, 0, sizeof(struct bind_ud_t));
    my_bind = &iud->bind[0];
    
    my_bind[0].buffer_type= MYSQL_TYPE_STRING;
    my_bind[0].buffer= (void*)&iud->c_ProductName[0];
    my_bind[0].buffer_length = sizeof(iud->c_ProductName);

    my_bind[1].buffer_type= MYSQL_TYPE_LONG;
    my_bind[1].buffer= (void*)&iud->c_Count;
    
    my_bind[2].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[2].buffer= (void*)&iud->c_Price;

    my_bind[3].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[3].buffer= (void*)&iud->c_Amount;

    my_bind[4].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[4].buffer= (void*)&iud->c_OtherAmountOri;

    my_bind[5].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[5].buffer= (void*)&iud->c_AdvanceFreight;

    my_bind[6].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[6].buffer= (void*)&iud->c_TotalCost;

    my_bind[7].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[7].buffer= (void*)&iud->c_SellPrice;

    my_bind[8].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[8].buffer= (void*)&iud->c_SellAmount;

    my_bind[9].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[9].buffer= (void*)&iud->c_GrossProfit;

    my_bind[10].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[10].buffer= (void*)&iud->c_GrossProfitRatio;

    *mybind = my_bind;
    *count  = 11;

    ETS_LOG_DEBUG("fill_bind_query success, *mybind:%p, *count:%d", *mybind, *count);
    return ETS_SUCCESS;
}

static int32_t query_callback(void* ud)
{
    struct bind_ud_t* ibud = (struct bind_ud_t*)ud;

    ETS_RETURN_IF_PTR_NULL(ibud, ETS_EINVAL);
    
    ETS_LOG_DEBUG("\nquery_callback");
    ETS_LOG_DEBUG("c_ProductName        : '%s'", ibud->c_ProductName);
    ETS_LOG_DEBUG("c_Count              : %d", ibud->c_Count);
    ETS_LOG_DEBUG("c_Price              : %.2f", ibud->c_Price);
    ETS_LOG_DEBUG("c_Amount             : %.2f", ibud->c_Amount);
    ETS_LOG_DEBUG("c_OtherAmountOri     : %.2f", ibud->c_OtherAmountOri);
    ETS_LOG_DEBUG("c_AdvanceFreight     : %.2f", ibud->c_AdvanceFreight);
    ETS_LOG_DEBUG("c_TotalCost          : %.2f", ibud->c_TotalCost);
    ETS_LOG_DEBUG("c_SellPrice          : %.2f", ibud->c_SellPrice);
    ETS_LOG_DEBUG("c_SellAmount         : %.2f", ibud->c_SellAmount);
    ETS_LOG_DEBUG("c_GrossProfit        : %.2f", ibud->c_GrossProfit);
    ETS_LOG_DEBUG("c_GrossProfitRatio   : %.2f", ibud->c_GrossProfitRatio);
    
    return 0;
}

struct query_field_t
{
    enum_field_types field_type;
    char is_null;
    unsigned long length;
    
    union
    {
        char d_str[256];
        int32_t d_i32;
        double d_double;
        float d_float;
    }data;
};

struct query_bind_ud_t
{
    struct query_field_t c_ProductName;
    struct query_field_t c_Count;
    struct query_field_t c_Price;
    struct query_field_t c_Amount;
    struct query_field_t c_OtherAmountOri;
    struct query_field_t c_AdvanceFreight;
    struct query_field_t c_TotalCost;
    struct query_field_t c_SellPrice;
    struct query_field_t c_SellAmount;
    struct query_field_t c_GrossProfit;
    struct query_field_t c_GrossProfitRatio;
    struct query_field_t c_CustomerName;
    struct query_field_t c_Id;
    
    MYSQL_BIND bind[13];
};

static int32_t advance_fill_bind_query(MYSQL_BIND** mybind, int32_t* count, void* ud)
{
    MYSQL_BIND* my_bind = NULL;
    struct query_bind_ud_t* iud = (struct query_bind_ud_t*)ud;
    
    ETS_MEMSET(iud, 0, sizeof(struct bind_ud_t));
    my_bind = &iud->bind[0];
    
    my_bind[0].buffer_type= MYSQL_TYPE_STRING;
    my_bind[0].buffer = (void*)&iud->c_ProductName.data.d_str;
    my_bind[0].buffer_length = sizeof(iud->c_ProductName.data.d_str);
    my_bind[0].length = &iud->c_ProductName.length;
    my_bind[0].is_null = &iud->c_ProductName.is_null;
    
    my_bind[1].buffer_type= MYSQL_TYPE_LONG;
    my_bind[1].buffer= (void*)&iud->c_Count.data.d_i32;
    my_bind[1].is_null = &iud->c_Count.is_null;
    
    my_bind[2].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[2].buffer= (void*)&iud->c_Price.data.d_double;
    my_bind[2].is_null = &iud->c_Price.is_null;
    
    my_bind[3].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[3].buffer= (void*)&iud->c_Amount.data.d_double;
    my_bind[3].is_null = &iud->c_Amount.is_null;
    
    my_bind[4].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[4].buffer= (void*)&iud->c_OtherAmountOri.data.d_double;
    my_bind[4].is_null = &iud->c_OtherAmountOri.is_null;
    
    my_bind[5].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[5].buffer= (void*)&iud->c_AdvanceFreight.data.d_double;
    my_bind[5].is_null = &iud->c_AdvanceFreight.is_null;
    
    my_bind[6].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[6].buffer= (void*)&iud->c_TotalCost.data.d_double;
    my_bind[6].is_null = &iud->c_TotalCost.is_null;
    
    my_bind[7].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[7].buffer= (void*)&iud->c_SellPrice.data.d_double;
    my_bind[7].is_null = &iud->c_SellPrice.is_null;
    
    my_bind[8].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[8].buffer= (void*)&iud->c_SellAmount.data.d_double;
    my_bind[8].is_null = &iud->c_SellAmount.is_null;
    
    my_bind[9].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[9].buffer= (void*)&iud->c_GrossProfit.data.d_double;
    my_bind[9].is_null = &iud->c_GrossProfit.is_null;
    
    my_bind[10].buffer_type= MYSQL_TYPE_DOUBLE;
    my_bind[10].buffer= (void*)&iud->c_GrossProfitRatio.data.d_double;
    my_bind[10].is_null = &iud->c_GrossProfitRatio.is_null;

    my_bind[11].buffer_type= MYSQL_TYPE_STRING;
    my_bind[11].buffer = (void*)&iud->c_CustomerName.data.d_str;
    my_bind[11].buffer_length = sizeof(iud->c_CustomerName.data.d_str);
    my_bind[11].length = &iud->c_CustomerName.length;
    my_bind[11].is_null = &iud->c_CustomerName.is_null;

    my_bind[12].buffer_type= MYSQL_TYPE_LONG;
    my_bind[12].buffer= (void*)&iud->c_Id.data.d_i32;
    my_bind[12].is_null = &iud->c_Id.is_null;
    
    *mybind = my_bind;
    *count  = 13;

    ETS_LOG_DEBUG("fill_bind_query success, *mybind:%p, *count:%d", *mybind, *count);
    return ETS_SUCCESS;
}

static int32_t advance_query_callback(void* ud)
{
    struct query_bind_ud_t* ibud = (struct query_bind_ud_t*)ud;

    ETS_RETURN_IF_PTR_NULL(ibud, ETS_EINVAL);
    
    ETS_LOG_DEBUG("\nadvance_query_callback");
    ETS_LOG_DEBUG("c_ProductName        : %hhd : '%s'", ibud->c_ProductName.is_null, ibud->c_ProductName.data.d_str);
    ETS_LOG_DEBUG("c_Count              : %hhd : %d", ibud->c_Count.is_null, ibud->c_Count.data.d_i32);
    ETS_LOG_DEBUG("c_Price              : %hhd : %.2f", ibud->c_Price.is_null, ibud->c_Price.data.d_double);
    ETS_LOG_DEBUG("c_Amount             : %hhd : %.2f", ibud->c_Amount.is_null, ibud->c_Amount.data.d_double);
    ETS_LOG_DEBUG("c_OtherAmountOri     : %hhd : %.2f", ibud->c_OtherAmountOri.is_null, ibud->c_OtherAmountOri.data.d_double);
    ETS_LOG_DEBUG("c_AdvanceFreight     : %hhd : %.2f", ibud->c_AdvanceFreight.is_null, ibud->c_AdvanceFreight.data.d_double);
    ETS_LOG_DEBUG("c_TotalCost          : %hhd : %.2f", ibud->c_TotalCost.is_null, ibud->c_TotalCost.data.d_double);
    ETS_LOG_DEBUG("c_SellPrice          : %hhd : %.2f", ibud->c_SellPrice.is_null, ibud->c_SellPrice.data.d_double);
    ETS_LOG_DEBUG("c_SellAmount         : %hhd : %.2f", ibud->c_SellAmount.is_null, ibud->c_SellAmount.data.d_double);
    ETS_LOG_DEBUG("c_GrossProfit        : %hhd : %.2f", ibud->c_GrossProfit.is_null, ibud->c_GrossProfit.data.d_double);
    ETS_LOG_DEBUG("c_GrossProfitRatio   : %hhd : %.2f", ibud->c_GrossProfitRatio.is_null, ibud->c_GrossProfitRatio.data.d_double);
    ETS_LOG_DEBUG("c_CustomerName       : %hhd : '%s'", ibud->c_CustomerName.is_null, ibud->c_CustomerName.data.d_str);
    ETS_LOG_DEBUG("c_Id                 : %hhd : %d", ibud->c_Id.is_null, ibud->c_Id.data.d_i32);
    
    return 0;
}

struct delete_bind_ud_t
{
    int32_t index;
    struct query_field_t c_Id;
    MYSQL_BIND bind[1];
};

static int32_t fill_bind_delete(MYSQL_BIND** mybind, int32_t* count, void* ud)
{
    MYSQL_BIND* my_bind = NULL;
    struct delete_bind_ud_t* iud = (struct delete_bind_ud_t*)ud;
    
    my_bind = &iud->bind[0];
    ETS_MEMSET(my_bind, 0, sizeof(iud->bind));
    ETS_MEMSET(&iud->c_Id, 0, sizeof(struct query_field_t));

    iud->c_Id.data.d_i32 = iud->index;
    my_bind[0].buffer_type= MYSQL_TYPE_LONG;
    my_bind[0].buffer= (void*)&iud->c_Id.data.d_i32;
    
    *mybind = my_bind;
    *count  = 1;

    ETS_LOG_DEBUG("fill_bind_delete success, *mybind:%p, *count:%d", *mybind, *count);
    return ETS_SUCCESS;
}

static int unittest_database_easyact(void)
{
    int32_t status = 0;
    struct
    {
        ETS_DB_CTX_PARAM_S param;
        void* dbc;
    }params =
    {
        //create database if not exists easyact;
        //grant all privileges on easyact.* to 'superman'@'%'
        //show database;
        //show tables from world
        //"SELECT * FROM world.city"
        //{"192.168.1.3", "superman", "123456", "world"}, NULL
        {"192.168.1.3", "superman", "123456", "easyact"}, NULL
    };
    /*
    供货月份,供货日期,供货单号,供货单位,客户名称,商品名称,规格,单位,数量,供货单价,供货金额,
    其他公司提供原材料金额,垫付费用,总成本,商品分类,开单月份,开单日期,销售单号,销售单价,销售金额,
    毛利润,毛利率,备注
    */
    struct
    {
        const char* sql;
        int32_t (*cb)(void*, void*);
        void* ud;
    }dbi[] =
    {
        {
            "DROP TABLE IF EXISTS tbl_sale_detail", NULL, NULL
        },
        /*{
            "CREATE TABLE tbl_sale_detail ("
                "c_Id INT NOT NULL AUTO_INCREMENT, "
                "c_SupplyMonth DATE NULL, c_SupplyDate DATE NULL, c_SupplyBill VARCHAR(128) NULL, "
                "c_SupplierName VARCHAR(128) NULL, c_CustomerName VARCHAR(256) NULL, c_ProductName VARCHAR(256) NOT NULL, "
                "c_Specification VARCHAR(128) NULL, c_Unit VARCHAR(32) NULL, c_Count INT NOT NULL, "
                "c_Price DOUBLE NOT NULL, c_Amount DOUBLE NOT NULL, c_OtherAmountOri DOUBLE NOT NULL, "
                "c_AdvanceFreight DOUBLE NOT NULL, c_TotalCost DOUBLE NOT NULL, c_ProductType VARCHAR(256) NOT NULL,"
                "c_BillingMonth DATE NULL, c_BillingDate DATE NULL, c_SellBill VARCHAR(128) NOT NULL, "
                "c_SellPrice DOUBLE NOT NULL, c_SellAmount DOUBLE NOT NULL, c_GrossProfit DOUBLE NOT NULL, "
                "c_GrossProfitRatio DOUBLE NOT NULL, c_Description VARCHAR(512) NULL, "
                "PRIMARY KEY (c_Id)"
            ")", NULL, NULL
        },*/
        {
            "CREATE TABLE tbl_sale_detail ("
                "c_Id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
                "c_SupplyMonth DATE NULL, c_SupplyDate DATE NULL, c_SupplyBill VARCHAR(128) NULL, "
                "c_SupplierName VARCHAR(128) NULL, c_CustomerName VARCHAR(256) NULL, c_ProductName VARCHAR(256), "
                "c_Specification VARCHAR(128) NULL, c_Unit VARCHAR(32) NULL, c_Count INT NOT NULL, "
                "c_Price DOUBLE NOT NULL, c_Amount DOUBLE NOT NULL, c_OtherAmountOri DOUBLE NOT NULL, "
                "c_AdvanceFreight DOUBLE NOT NULL, c_TotalCost DOUBLE NOT NULL, c_ProductType VARCHAR(256),"
                "c_BillingMonth DATE NULL, c_BillingDate DATE NULL, c_SellBill VARCHAR(128), "
                "c_SellPrice DOUBLE NOT NULL, c_SellAmount DOUBLE NOT NULL, c_GrossProfit DOUBLE NOT NULL, "
                "c_GrossProfitRatio DOUBLE NOT NULL, c_Description VARCHAR(512) NULL"
            ")", NULL, NULL
        }
    };
    int32_t idx = 0;
    
    status = ETS_db_env_Init();
    ETS_RETURN_IF_CONDITION_TURE(status, status);

    status = ETS_db_ctx_New(&params.param, &params.dbc);
    if (status)
    {
        ETS_LOG_ERROR("ETS_db_ctx_New failed, status:%d", status);
        goto finish;
    }

    ETS_LOG_DEBUG("ETS_db_ctx_New success, dbc:%p", params.dbc);

    for (idx = 0; idx < ETS_DIM(dbi); idx++)
    {
        status = ETS_db_cmd_Execute(params.dbc, dbi[idx].sql, dbi[idx].cb, dbi[idx].ud);
        if (status)
        {
            ETS_LOG_ERROR("ETS_db_cmd_Execute failed, idx:%d, status:%d", idx, status);
            goto finish;
        }

        ETS_LOG_DEBUG("ETS_db_cmd_Execute success, idx:%d", idx);
    }

    //////////////////////////////////////////////////////////////////////////////////////////
    // insert
    struct
    {
        const char* sql_fmt;
        int32_t (*fill_bind)(MYSQL_BIND**,int32_t*, void*);
        struct bind_ud_t ud;
    }fi_insert[] =
    {
        {
            "INSERT INTO tbl_sale_detail ("
                "c_ProductName, "
                "c_Count, "
                "c_Price, c_Amount, c_OtherAmountOri, "
                "c_AdvanceFreight, c_TotalCost, "
                "c_SellPrice, c_SellAmount, c_GrossProfit, "
                "c_GrossProfitRatio) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            fill_bind_insert,
            {
                .index = 1,
            }
        },
        {
            "INSERT INTO tbl_sale_detail ("
                "c_ProductName, "
                "c_Count, "
                "c_Price, c_Amount, c_OtherAmountOri, "
                "c_AdvanceFreight, c_TotalCost, "
                "c_SellPrice, c_SellAmount, c_GrossProfit, "
                "c_GrossProfitRatio) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            fill_bind_insert,
            {
                .index = 2,
            }
        }
    };

    for (idx = 0; idx < ETS_DIM(fi_insert); idx++)
    {
        status = ETS_db_cmd_Insert(params.dbc,fi_insert[idx].sql_fmt,
            fi_insert[idx].fill_bind, &fi_insert[idx].ud);
        if (status)
        {
            ETS_LOG_ERROR("ETS_db_cmd_Insert failed, idx:%d, status:%d", idx, status);
            goto finish;
        }

        ETS_LOG_DEBUG("ETS_db_cmd_Insert success, idx:%d", idx);
    }

    //////////////////////////////////////////////////////////////////////////////////////////
    // select
    struct
    {
        const char* sql_fmt;
        int32_t (*fill_bind)(MYSQL_BIND**,int32_t*, void*);
        int32_t (*callback)(void*);
        struct bind_ud_t ud;
    }fi_query[] =
    {
        {
            "SELECT c_ProductName, c_Count, c_Price, c_Amount, c_OtherAmountOri, "
                "c_AdvanceFreight, c_TotalCost, c_SellPrice, c_SellAmount, c_GrossProfit, "
                "c_GrossProfitRatio "
            "FROM tbl_sale_detail",
            fill_bind_query, query_callback
        }
    };

    for (idx = 0; idx < ETS_DIM(fi_query); idx++)
    {
        status = ETS_db_cmd_Query(params.dbc,fi_query[idx].sql_fmt,
            fi_query[idx].fill_bind, fi_query[idx].callback, &fi_query[idx].ud);
        if (status)
        {
            ETS_LOG_ERROR("ETS_db_cmd_Query failed, idx:%d, status:%d", idx, status);
            goto finish;
        }

        ETS_LOG_DEBUG("ETS_db_cmd_Query success, idx:%d", idx);
    }

    //////////////////////////////////////////////////////////////////////////////////////////
    // advance select
    struct
    {
        const char* sql_fmt;
        int32_t (*fill_bind)(MYSQL_BIND**,int32_t*, void*);
        int32_t (*callback)(void*);
        struct query_bind_ud_t ud;
    }fiex_query[] =
    {
        {
            "SELECT c_ProductName, c_Count, c_Price, c_Amount, c_OtherAmountOri, "
                "c_AdvanceFreight, c_TotalCost, c_SellPrice, c_SellAmount, c_GrossProfit, "
                "c_GrossProfitRatio, c_CustomerName, c_Id "
            "FROM tbl_sale_detail",
            advance_fill_bind_query, advance_query_callback
        }
    };

    for (idx = 0; idx < ETS_DIM(fiex_query); idx++)
    {
        status = ETS_db_cmd_Query(params.dbc, fiex_query[idx].sql_fmt,
            fiex_query[idx].fill_bind, fiex_query[idx].callback, &fiex_query[idx].ud);
        if (status)
        {
            ETS_LOG_ERROR("ETS_db_cmd_Query failed, idx:%d, status:%d", idx, status);
            goto finish;
        }

        ETS_LOG_DEBUG("ETS_db_cmd_Query success, idx:%d", idx);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////
    // update
    struct
    {
        const char* sql_fmt;
        int32_t (*fill_bind)(MYSQL_BIND**,int32_t*, void*);
        struct bind_ud_t ud;
    }fi_update[] =
    {
        {
            "UPDATE tbl_sale_detail SET "
                "c_ProductName = ?, "
                "c_Count = ?, "
                "c_Price = ?, c_Amount = ?, c_OtherAmountOri = ?, "
                "c_AdvanceFreight = ?, c_TotalCost = ?, "
                "c_SellPrice = ?, c_SellAmount = ?, c_GrossProfit = ?, "
                "c_GrossProfitRatio = ? "
            "WHERE c_Id = 1",
            fill_bind_insert,
            {
                .index = 3,
            }
        },
        {
            "UPDATE tbl_sale_detail SET "
                "c_ProductName = ?, "
                "c_Count = ?, "
                "c_Price = ?, c_Amount = ?, c_OtherAmountOri = ?, "
                "c_AdvanceFreight = ?, c_TotalCost = ?, "
                "c_SellPrice = ?, c_SellAmount = ?, c_GrossProfit = ?, "
                "c_GrossProfitRatio = ? "
            "WHERE c_Id = 2",
            fill_bind_insert,
            {
                .index = 4,
            }
        }
    };

    for (idx = 0; idx < ETS_DIM(fi_update); idx++)
    {
        status = ETS_db_cmd_Update(params.dbc, fi_update[idx].sql_fmt,
            fi_update[idx].fill_bind, &fi_update[idx].ud);
        if (status)
        {
            ETS_LOG_ERROR("ETS_db_cmd_Update failed, idx:%d, status:%d", idx, status);
            goto finish;
        }

        ETS_LOG_DEBUG("ETS_db_cmd_Update success, idx:%d", idx);
    }

    //////////////////////////////////////////////////////////////////////////////////////////
    // advance select
    for (idx = 0; idx < ETS_DIM(fiex_query); idx++)
    {
        status = ETS_db_cmd_Query(params.dbc, fiex_query[idx].sql_fmt,
            fiex_query[idx].fill_bind, fiex_query[idx].callback, &fiex_query[idx].ud);
        if (status)
        {
            ETS_LOG_ERROR("ETS_db_cmd_Query failed, idx:%d, status:%d", idx, status);
            goto finish;
        }

        ETS_LOG_DEBUG("ETS_db_cmd_Query success, idx:%d", idx);
    }

    //////////////////////////////////////////////////////////////////////////////////////////
    // delete
    struct
    {
        const char* sql_fmt;
        int32_t (*fill_bind)(MYSQL_BIND**,int32_t*, void*);
        struct delete_bind_ud_t ud;
    }fi_delete[] =
    {
        {
            "DELETE FROM tbl_sale_detail WHERE c_Id = ?",
            fill_bind_delete,
            {
                .index = 1,
            }
        }
    };

    for (idx = 0; idx < ETS_DIM(fi_delete); idx++)
    {
        status = ETS_db_cmd_Delete(params.dbc,fi_delete[idx].sql_fmt,
            fi_delete[idx].fill_bind, &fi_delete[idx].ud);
        if (status)
        {
            ETS_LOG_ERROR("ETS_db_cmd_Delete failed, idx:%d, status:%d", idx, status);
            goto finish;
        }

        ETS_LOG_DEBUG("ETS_db_cmd_Delete success, idx:%d", idx);
    }

    //////////////////////////////////////////////////////////////////////////////////////////
    // advance select
    for (idx = 0; idx < ETS_DIM(fiex_query); idx++)
    {
        status = ETS_db_cmd_Query(params.dbc, fiex_query[idx].sql_fmt,
            fiex_query[idx].fill_bind, fiex_query[idx].callback, &fiex_query[idx].ud);
        if (status)
        {
            ETS_LOG_ERROR("ETS_db_cmd_Query failed, idx:%d, status:%d", idx, status);
            goto finish;
        }

        ETS_LOG_DEBUG("ETS_db_cmd_Query success, idx:%d", idx);
    }
    
finish:
    ETS_db_ctx_Del(&params.dbc);
    ETS_db_env_Exit();
    
    return 0;
}

struct meta_field_info_ctx_t
{
    ETS_DBTBL_FIELD_DESC_S** fds;
    int32_t count;
};

static int32_t dup_field_desc_array(int32_t count, ETS_DBTBL_FIELD_DESC_S** fd, void* ud)
{
    int32_t status = 0;
    struct meta_field_info_ctx_t* meta = (struct meta_field_info_ctx_t*)ud;

    ETS_RETURN_IF_CONDITION_TURE(count<=0, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(fd, ETS_EINVAL);
    ETS_RETURN_IF_PTR_NULL(ud, ETS_EINVAL);

    ETS_LOG_DEBUG("dup_field_desc_array");
    
    //ETS_db_opr_DumpFieldsInfo(fd, count);

    meta->fds   = NULL;
    meta->count = 0;
    
    status = ETS_db_opr_CopyFieldsDesc(fd, count, &meta->fds, &meta->count);
    if (status)
    {
        ETS_LOG_ERROR("ETS_db_opr_CopyFieldsDesc failed, status:%d", status);
        return status;
    }

    //ETS_db_opr_DumpFieldsInfo(meta->fds, meta->count);
    return status;
}

//#define __ets_support_zh_cn__

static void unittest_database_create_table(void)
{
    int32_t status = 0;
    struct
    {
        ETS_DB_CTX_PARAM_S param;
        void* dbc;
    }params =
    {
        //create database if not exists easyact;
        //grant all privileges on easyact.* to 'superman'@'%'
        //show database;
        //show tables from world
        //"SELECT * FROM world.city"
        //{"192.168.1.3", "superman", "123456", "world"}, NULL
        {"192.168.1.3", "superman", "123456", "easyact"}, NULL
    };
    char* tbl = "tbl_sale_detail";
    char* sql_drop_table = "DROP TABLE IF EXISTS tbl_sale_detail";
    ETS_DBTBL_FIELD_DESC_S fields[] =
    {
        /*
        "CREATE TABLE tbl_sale_detail ("
            "c_Id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
            "c_SupplyMonth DATE NULL, c_SupplyDate DATE NULL, c_SupplyBill VARCHAR(128) NULL, "
            "c_SupplierName VARCHAR(128) NULL, c_CustomerName VARCHAR(256) NULL, c_ProductName VARCHAR(256), "
            "c_Specification VARCHAR(128) NULL, c_Unit VARCHAR(32) NULL, c_Count INT NOT NULL, "
            "c_Price DOUBLE NOT NULL, c_Amount DOUBLE NOT NULL, c_OtherAmountOri DOUBLE NOT NULL, "
            "c_AdvanceFreight DOUBLE NOT NULL, c_TotalCost DOUBLE NOT NULL, c_ProductType VARCHAR(256),"
            "c_BillingMonth DATE NULL, c_BillingDate DATE NULL, c_SellBill VARCHAR(128), "
            "c_SellPrice DOUBLE NOT NULL, c_SellAmount DOUBLE NOT NULL, c_GrossProfit DOUBLE NOT NULL, "
            "c_GrossProfitRatio DOUBLE NOT NULL, c_Description VARCHAR(512) NULL"
        ")"
        */
        {1, 0, {"c_Id"             , ETS_DBTBL_FIELD_TYPE_LONGLONG, 0, 1, 1, 0  }},
        {1, 0, {"c_SupplierName"   , ETS_DBTBL_FIELD_TYPE_STRING  , 0, 0, 0, 256}},
        {1, 0, {"c_CustomerName"   , ETS_DBTBL_FIELD_TYPE_STRING  , 0, 0, 0, 256}},
        {1, 0, {"c_ProductName"    , ETS_DBTBL_FIELD_TYPE_STRING  , 0, 0, 0, 256}},
        {1, 0, {"c_Count"          , ETS_DBTBL_FIELD_TYPE_LONG    , 0, 0, 0, 0  }},
        {1, 0, {"c_Price"          , ETS_DBTBL_FIELD_TYPE_DOUBLE  , 0, 0, 0, 0  }},
        {1, 0, {"c_Amount"         , ETS_DBTBL_FIELD_TYPE_DOUBLE  , 0, 0, 0, 0  }},
        {1, 0, {"c_Date"           , ETS_DBTBL_FIELD_TYPE_DATE    , 0, 0, 0, 0  }},
        {1, 0, {"c_Time"           , ETS_DBTBL_FIELD_TYPE_TIME    , 0, 0, 0, 0  }},
        {1, 0, {"c_Datetime"       , ETS_DBTBL_FIELD_TYPE_DATETIME, 0, 0, 0, 0  }},
    };
    char* c_SupplierName = "atnxry coperation ltd.";
    char* c_CustomerName = "hank";
    char* c_ProductName  = "iphone 12 pro";
    uint32_t count = 0;
    struct meta_field_info_ctx_t meta = {0};
    int32_t colidx = 0;
    int32_t keyidx = 0;
    int32_t cntidx = 4;
    int32_t prcidx = 5;
    int32_t amtidx = 6;
    /*
    供货月份,供货日期,供货单号,供货单位,客户名称,商品名称,规格,单位,数量,供货单价,供货金额,
    其他公司提供原材料金额,垫付费用,总成本,商品分类,开单月份,开单日期,销售单号,销售单价,销售金额,
    毛利润,毛利率,备注
    */
    status = ETS_db_env_Init();
    ETS_CHECK_CONDITION_TURE(status);

    status = ETS_db_ctx_New(&params.param, &params.dbc);
    if (status)
    {
        ETS_LOG_ERROR("ETS_db_ctx_New failed, status:%d", status);
        goto env_exit;
    }

    ETS_LOG_DEBUG("ETS_db_ctx_New success, dbc:%p", params.dbc);

    status = ETS_db_cmd_Execute(params.dbc, sql_drop_table, NULL, NULL);
    if (status)
    {
        ETS_LOG_ERROR("DROP_TABLE failed, status:%d, tbl:%s", status, tbl);
        goto env_exit;
    }

    ETS_LOG_DEBUG("DROP_TABLE success, tbl:%s", tbl);

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //创建数据表
    status = ETS_db_opr_CreateTable(params.dbc, tbl, &fields[0], ETS_DIM(fields));
    if (status)
    {
        ETS_LOG_ERROR("ETS_db_opr_CreateTable failed, status:%d", status);
        goto ctx_del;
    }

    ETS_LOG_DEBUG("ETS_db_opr_CreateTable success");

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //插入数据行
    status = ETS_db_opr_GetFieldsDesc(params.dbc, tbl, dup_field_desc_array, NULL, &meta);
    if (status)
    {
        goto ctx_del;
    }

    colidx++;
    
    /* 填充需要insert的数据 */
    meta.fds[colidx]->is_active      = 1;
    meta.fds[colidx]->val.d_str.len  = (int32_t)(strlen(c_SupplierName) + 1);
    ETS_MEMCPY(meta.fds[colidx]->val.d_str.buf, c_SupplierName, meta.fds[colidx]->val.d_str.len);
    colidx++;

    meta.fds[colidx]->is_active      = 1;
    meta.fds[colidx]->val.d_str.len = (int32_t)(strlen(c_CustomerName) + 1);
    ETS_MEMCPY(meta.fds[colidx]->val.d_str.buf, c_CustomerName, meta.fds[colidx]->val.d_str.len);
    colidx++;
    
    meta.fds[colidx]->is_active      = 1;
    meta.fds[colidx]->val.d_str.len = (int32_t)(strlen(c_ProductName) + 1);
    ETS_MEMCPY(meta.fds[colidx]->val.d_str.buf, c_ProductName, meta.fds[colidx]->val.d_str.len);
    colidx++;

    meta.fds[colidx]->is_active      = 1;
    meta.fds[colidx]->val.d_u32      = 123;
    colidx++;
    
    meta.fds[colidx]->is_active      = 1;
    meta.fds[colidx]->val.d_double   = 23.45;
    colidx++;
    
    meta.fds[colidx]->is_active      = 1;
    meta.fds[colidx]->val.d_double   = meta.fds[colidx-2]->val.d_u32 * meta.fds[colidx-1]->val.d_double;
    colidx++;
    
    meta.fds[colidx]->is_active      = 1;
    meta.fds[colidx]->val.d_datetime.year  = 2025;
    meta.fds[colidx]->val.d_datetime.month = 8;
    meta.fds[colidx]->val.d_datetime.day   = 2;
    colidx++;
    
    meta.fds[colidx]->is_active      = 1;
    meta.fds[colidx]->val.d_datetime.hour   = 15;
    meta.fds[colidx]->val.d_datetime.minute = 54;
    meta.fds[colidx]->val.d_datetime.second = 13;
    colidx++;
    
    meta.fds[colidx]->is_active      = 1;
    meta.fds[colidx]->val.d_datetime.year  = 2025;
    meta.fds[colidx]->val.d_datetime.month = 8;
    meta.fds[colidx]->val.d_datetime.day   = 2;
    meta.fds[colidx]->val.d_datetime.hour   = 15;
    meta.fds[colidx]->val.d_datetime.minute = 54;
    meta.fds[colidx]->val.d_datetime.second = 13;
    colidx++;

    status = ETS_db_opr_Insert(params.dbc, tbl, meta.fds, meta.count);
    if (status)
    {
        ETS_LOG_ERROR("ETS_db_opr_Insert failed, status:%d", status);
        goto fields_info;
    }

    ETS_LOG_DEBUG("ETS_db_opr_Insert success");

    
    /////////////////////////////////////////////////////////////////////////////////////////////////
    //更新数据表
    meta.fds[keyidx]->is_active = 1;
    meta.fds[keyidx]->val.d_u64 = 1;
    meta.fds[keyidx]->cond = ETS_DBTBL_FIELD_COND_AND;

    meta.fds[cntidx]->is_active      = 1;
    meta.fds[cntidx]->val.d_u32      = 234;
    
    meta.fds[prcidx]->is_active      = 1;
    meta.fds[prcidx]->val.d_double   = 34.56;
    
    meta.fds[amtidx]->is_active      = 1;
    meta.fds[amtidx]->val.d_double   = meta.fds[cntidx]->val.d_u32 * meta.fds[prcidx]->val.d_double;

    meta.fds[9]->is_active      = 1;
    meta.fds[9]->val.d_datetime.year  = 2026;
    meta.fds[9]->val.d_datetime.month = 9;
    meta.fds[9]->val.d_datetime.day   = 3;
    meta.fds[9]->val.d_datetime.hour   = 16;
    meta.fds[9]->val.d_datetime.minute = 55;
    meta.fds[9]->val.d_datetime.second = 14;

    status = ETS_db_opr_Update(params.dbc, tbl, &meta.fds[2], meta.count - 2, meta.fds, 2);
    if (status)
    {
        ETS_LOG_ERROR("ETS_db_opr_Update failed, status:%d", status);
        goto fields_info;
    }

    ETS_LOG_DEBUG("ETS_db_opr_Update success");

    status = ETS_db_opr_Query(params.dbc, "SELECT * FROM tbl_sale_detail",
        meta.fds, meta.count, NULL, NULL, NULL);
    if (status)
    {
        ETS_LOG_WARN("ETS_db_opr_Query failed, status:%d", status);
        goto fields_info;
    }

    status = ETS_db_opr_Delete(params.dbc, tbl, meta.fds, 2);
    if (status)
    {
        ETS_LOG_ERROR("ETS_db_opr_Delete failed, status:%d", status);
        goto fields_info;
    }

    ETS_LOG_DEBUG("ETS_db_opr_Delete success");
    
fields_info:
    meta.fds[1]->is_active      = 0;
    meta.fds[1]->val.d_str.buf  = NULL;
    meta.fds[1]->val.d_str.size = 0;
    meta.fds[1]->val.d_str.len  = 0;

    meta.fds[2]->is_active      = 0;
    meta.fds[2]->val.d_str.buf  = NULL;
    meta.fds[2]->val.d_str.size = 0;
    meta.fds[2]->val.d_str.len  = 0;

    meta.fds[3]->is_active      = 0;
    meta.fds[3]->val.d_str.buf  = NULL;
    meta.fds[3]->val.d_str.size = 0;
    meta.fds[3]->val.d_str.len  = 0;
    
    ETS_db_opr_FreeFieldsInfo(&meta.fds, meta.count);
    
ctx_del:
    ETS_db_ctx_Del(&params.dbc);
    
env_exit:
    ETS_db_env_Exit();
    return;
}

static void unittest_database_opr_table()
{
    int32_t status = 0;
    struct
    {
        ETS_DB_CTX_PARAM_S param;
        void* dbc;
    }params =
    {
        //create database if not exists easyact;
        //grant all privileges on easyact.* to 'superman'@'%'
        //show database;
        //show tables from world
        //"SELECT * FROM world.city"
        //{"192.168.1.3", "superman", "123456", "world"}, NULL
        {"192.168.1.3", "superman", "123456", "easyact"}, NULL
    };
    char* tbl = "tbl_sale_detail";
    char* sql_drop_table = "DROP TABLE IF EXISTS tbl_sale_detail";
    ETS_DBTBL_FIELD_DESC_S fields[] =
    {
        /*
        "CREATE TABLE tbl_sale_detail ("
            "c_Id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
            "c_SupplyMonth DATE NULL, c_SupplyDate DATE NULL, c_SupplyBill VARCHAR(128) NULL, "
            "c_SupplierName VARCHAR(128) NULL, c_CustomerName VARCHAR(256) NULL, c_ProductName VARCHAR(256), "
            "c_Specification VARCHAR(128) NULL, c_Unit VARCHAR(32) NULL, c_Count INT NOT NULL, "
            "c_Price DOUBLE NOT NULL, c_Amount DOUBLE NOT NULL, c_OtherAmountOri DOUBLE NOT NULL, "
            "c_AdvanceFreight DOUBLE NOT NULL, c_TotalCost DOUBLE NOT NULL, c_ProductType VARCHAR(256),"
            "c_BillingMonth DATE NULL, c_BillingDate DATE NULL, c_SellBill VARCHAR(128), "
            "c_SellPrice DOUBLE NOT NULL, c_SellAmount DOUBLE NOT NULL, c_GrossProfit DOUBLE NOT NULL, "
            "c_GrossProfitRatio DOUBLE NOT NULL, c_Description VARCHAR(512) NULL"
        ")"
        */
        {1, 0, {"c_Id"             , ETS_DBTBL_FIELD_TYPE_LONGLONG, 0, 1, 1, 0  }},
        {1, 0, {"c_SupplierName"   , ETS_DBTBL_FIELD_TYPE_STRING  , 0, 0, 0, 256}},
        {1, 0, {"c_CustomerName"   , ETS_DBTBL_FIELD_TYPE_STRING  , 0, 0, 0, 256}},
        {1, 0, {"c_ProductName"    , ETS_DBTBL_FIELD_TYPE_STRING  , 0, 0, 0, 256}},
        {1, 0, {"c_Count"          , ETS_DBTBL_FIELD_TYPE_LONG    , 0, 0, 0, 0  }},
        {1, 0, {"c_Price"          , ETS_DBTBL_FIELD_TYPE_DOUBLE  , 0, 0, 0, 0  }},
        {1, 0, {"c_Amount"         , ETS_DBTBL_FIELD_TYPE_DOUBLE  , 0, 0, 0, 0  }},
        {1, 0, {"c_Date"           , ETS_DBTBL_FIELD_TYPE_DATE    , 0, 0, 0, 0  }},
        {1, 0, {"c_Time"           , ETS_DBTBL_FIELD_TYPE_TIME    , 0, 0, 0, 0  }},
        {1, 0, {"c_Datetime"       , ETS_DBTBL_FIELD_TYPE_DATETIME, 0, 0, 0, 0  }},
    };
    char* c_SupplierName = "atnxry coperation ltd.";
    char* c_CustomerName = "hank";
    char* c_ProductName  = "iphone 12 pro";
    uint32_t count = 0;
    struct meta_field_info_ctx_t meta = {0};
    int32_t colidx = 0;
    int32_t keyidx = 0;
    int32_t cntidx = 4;
    int32_t prcidx = 5;
    int32_t amtidx = 6;
    struct
    {
        char* c_SupplierName;
        char* c_CustomerName;
        char* c_ProductName;
        int32_t c_Count;
        double c_Price;
        struct
        {
            int32_t year;
            int32_t month;
            int32_t day;
        }c_Date;
        struct
        {
            int32_t hour;
            int32_t min;
            int32_t sec;
        }c_Time;
        struct
        {
            int32_t year;
            int32_t month;
            int32_t day;
            int32_t hour;
            int32_t min;
            int32_t sec;
        }c_Datetime;
    }_i_data[] =
    {
        {"mercedes-benz", "zhangsan", "benz E300", 1, 10.1, {2020, 1, 1}, {10, 20, 30}, {2020, 1, 1, 10, 20, 30}},
        {"Audi ltd", "lisi", "audi A4", 2, 10.2,  {2021, 2, 2}, {11, 21, 31}, {2021, 2, 2, 11, 21, 31}},
        {"BMW ltd", "wangwu", "bmw 740i", 3, 10.3,  {2022, 3, 3}, {12, 22, 32}, {2022, 3, 3, 12, 22, 32}},
    #ifdef __ets_support_zh_cn__
        {"梅赛德斯.奔驰", "zhangsan", "奔驰E300", 1, 10.1, {2020, 1, 1}, {10, 20, 30}, {2020, 1, 1, 10, 20, 30}},
        {"奥迪", "lisi", "奥迪A4", 2, 10.2,  {2021, 2, 2}, {11, 21, 31}, {2021, 2, 2, 11, 21, 31}},
        {"宝马", "wangwu", "宝马740i", 3, 10.3,  {2022, 3, 3}, {12, 22, 32}, {2022, 3, 3, 12, 22, 32}},
    #endif
    };
    int32_t idx = 0;
    
    /*
    供货月份,供货日期,供货单号,供货单位,客户名称,商品名称,规格,单位,数量,供货单价,供货金额,
    其他公司提供原材料金额,垫付费用,总成本,商品分类,开单月份,开单日期,销售单号,销售单价,销售金额,
    毛利润,毛利率,备注
    */
    status = ETS_db_env_Init();
    ETS_CHECK_CONDITION_TURE(status);

    status = ETS_db_ctx_New(&params.param, &params.dbc);
    if (status)
    {
        ETS_LOG_ERROR("ETS_db_ctx_New failed, status:%d", status);
        goto env_exit;
    }

    ETS_LOG_DEBUG("ETS_db_ctx_New success, dbc:%p", params.dbc);

    status = ETS_db_cmd_Execute(params.dbc, sql_drop_table, NULL, NULL);
    if (status)
    {
        ETS_LOG_ERROR("DROP_TABLE failed, status:%d, tbl:%s", status, tbl);
        goto env_exit;
    }

    ETS_LOG_DEBUG("DROP_TABLE success, tbl:%s", tbl);

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //创建数据表
    status = ETS_db_opr_CreateTable(params.dbc, tbl, &fields[0], ETS_DIM(fields));
    if (status)
    {
        ETS_LOG_ERROR("ETS_db_opr_CreateTable failed, status:%d", status);
        goto ctx_del;
    }

    ETS_LOG_DEBUG("ETS_db_opr_CreateTable success");

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //插入数据行
    status = ETS_db_opr_GetFieldsDesc(params.dbc, tbl, dup_field_desc_array, NULL, &meta);
    if (status)
    {
        goto ctx_del;
    }

    for (idx = 0; idx < ETS_DIM(_i_data); idx++)
    {
        colidx = 0;
        colidx++;
        
        meta.fds[colidx]->is_active      = 1;
        meta.fds[colidx]->val.d_str.len  = (int32_t)(strlen(_i_data[idx].c_SupplierName) + 1);
        ETS_MEMCPY(meta.fds[colidx]->val.d_str.buf, _i_data[idx].c_SupplierName, meta.fds[colidx]->val.d_str.len);
        colidx++;

        meta.fds[colidx]->is_active      = 1;
        meta.fds[colidx]->val.d_str.len = (int32_t)(strlen(_i_data[idx].c_CustomerName) + 1);
        ETS_MEMCPY(meta.fds[colidx]->val.d_str.buf, _i_data[idx].c_CustomerName, meta.fds[colidx]->val.d_str.len);
        colidx++;
        
        meta.fds[colidx]->is_active      = 1;
        meta.fds[colidx]->val.d_str.len = (int32_t)(strlen(_i_data[idx].c_ProductName) + 1);
        ETS_MEMCPY(meta.fds[colidx]->val.d_str.buf, _i_data[idx].c_ProductName, meta.fds[colidx]->val.d_str.len);
        colidx++;

        meta.fds[colidx]->is_active      = 1;
        meta.fds[colidx]->val.d_u32      = _i_data[idx].c_Count;
        colidx++;
        
        meta.fds[colidx]->is_active      = 1;
        meta.fds[colidx]->val.d_double   = _i_data[idx].c_Price;
        colidx++;
        
        meta.fds[colidx]->is_active      = 1;
        meta.fds[colidx]->val.d_double   = meta.fds[colidx-2]->val.d_u32 * meta.fds[colidx-1]->val.d_double;
        colidx++;
        
        meta.fds[colidx]->is_active      = 1;
        meta.fds[colidx]->val.d_datetime.year  = _i_data[idx].c_Date.year;
        meta.fds[colidx]->val.d_datetime.month = _i_data[idx].c_Date.month;
        meta.fds[colidx]->val.d_datetime.day   = _i_data[idx].c_Date.day;
        colidx++;
        
        meta.fds[colidx]->is_active      = 1;
        meta.fds[colidx]->val.d_datetime.hour   = _i_data[idx].c_Time.hour;
        meta.fds[colidx]->val.d_datetime.minute = _i_data[idx].c_Time.min;
        meta.fds[colidx]->val.d_datetime.second = _i_data[idx].c_Time.sec;
        colidx++;
        
        meta.fds[colidx]->is_active      = 1;
        meta.fds[colidx]->val.d_datetime.year  = _i_data[idx].c_Datetime.year;
        meta.fds[colidx]->val.d_datetime.month = _i_data[idx].c_Datetime.month;
        meta.fds[colidx]->val.d_datetime.day   = _i_data[idx].c_Datetime.day;
        meta.fds[colidx]->val.d_datetime.hour   = _i_data[idx].c_Datetime.hour;
        meta.fds[colidx]->val.d_datetime.minute = _i_data[idx].c_Datetime.min;
        meta.fds[colidx]->val.d_datetime.second = _i_data[idx].c_Datetime.sec;
        colidx++;

        status = ETS_db_opr_Insert(params.dbc, tbl, meta.fds, meta.count);
        if (status)
        {
            ETS_LOG_ERROR("ETS_db_opr_Insert failed, status:%d", status);
            goto fields_info;
        }

        ETS_LOG_DEBUG("ETS_db_opr_Insert success");

        status = ETS_db_opr_Query(params.dbc, "SELECT * FROM tbl_sale_detail",
            meta.fds, meta.count, NULL, NULL, NULL);
        if (status)
        {
            ETS_LOG_WARN("ETS_db_opr_Query failed, status:%d", status);
            goto fields_info;
        }
    }
    
fields_info:
    for (idx = 0; idx < meta.count; idx++)
    {
        if (ETS_DBTBL_FIELD_TYPE_STRING == meta.fds[idx]->key.type)
        {
            meta.fds[idx]->is_active      = 0;
            meta.fds[idx]->val.d_str.buf  = NULL;
            meta.fds[idx]->val.d_str.size = 0;
            meta.fds[idx]->val.d_str.len  = 0;
            
            continue;
        }

        if (ETS_DBTBL_FIELD_TYPE_BLOB == meta.fds[idx]->key.type)
        {
            meta.fds[idx]->is_active      = 0;
            meta.fds[idx]->val.d_blob.buf  = NULL;
            meta.fds[idx]->val.d_blob.size = 0;
            meta.fds[idx]->val.d_blob.len  = 0;
            
            continue;
        }
    }
    
    ETS_db_opr_FreeFieldsInfo(&meta.fds, meta.count);
    
ctx_del:
    ETS_db_ctx_Del(&params.dbc);
    
env_exit:
    ETS_db_env_Exit();
    return;
}

static void unittest(void)
{
#ifdef __ut_test__
    (void)unittest_mutex();
    (void)unittest_database();
    (void)unittest_database_easyact();
    unittest_database_create_table();
#endif
    unittest_database_opr_table();
    
    return;
}

int main(int argc, char** argv)
{
    int32_t status = 0;

    status = ETS_log_Init("message.log", "rollback.log", 20 * 1024 * 1024);
    if (status)
    {
        return (-status);
    }

    ETS_LOG_DEBUG("sizeof(long)         : %d", sizeof(long));
    ETS_LOG_DEBUG("sizeof(long long int): %d", sizeof(long long int));
    ETS_LOG_DEBUG("sizeof(long long)    : %d", sizeof(long long));
    ETS_LOG_DEBUG("sizeof(float)        : %d", sizeof(float));
    ETS_LOG_DEBUG("sizeof(double)       : %d", sizeof(double));
    
    unittest();

    ETS_log_Exit();
    return (status)? (-status) : 0;
}

