#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

  typedef enum {
    KUDU_INT8 = 0,
    KUDU_INT16 = 1,
    KUDU_INT32 = 2,
    KUDU_INT64 = 3,
    KUDU_STRING = 4,
    KUDU_BOOL = 5,
    KUDU_FLOAT = 6,
    KUDU_DOUBLE = 7,
    KUDU_BINARY = 8,
    KUDU_UNIXTIME_MICROS = 9
  } C_KuduType;

  typedef enum {
    KUDU_AUTO_FLUSH_SYNC = 0,
    KUDU_AUTO_FLUSH_BACKGROUND = 1,
    KUDU_MANUAL_FLUSH = 2
  } C_FlushMode;

  typedef struct C_KuduClient C_KuduClient;
  typedef struct C_KuduClientBuilder C_KuduClientBuilder;
  typedef struct C_KuduColumnSpec C_KuduColumnSpec;
  typedef struct C_KuduScanner C_KuduScanner;
  typedef struct C_KuduScanBatch C_KuduScanBatch;
  typedef struct C_KuduSchema C_KuduSchema;
  typedef struct C_KuduSchemaBuilder C_KuduSchemaBuilder;
  typedef struct C_KuduSession C_KuduSession;
  typedef struct C_KuduStatus C_KuduStatus;
  typedef struct C_KuduTable C_KuduTable;
  typedef struct C_KuduTableCreator C_KuduTableCreator;
  typedef struct C_KuduWriteOperation C_KuduWriteOperation;

  char* KuduStatus_Message(C_KuduStatus* self);
  void KuduStatus_Free(C_KuduStatus* self);

  // ------------------------------------------------------------
  C_KuduClientBuilder* KuduClientBuilder_Create();
  void KuduClientBuilder_Free(C_KuduClientBuilder* self);
  void KuduClientBuilder_add_master_server_addr(
      C_KuduClientBuilder* self,
      const char* addr);
  C_KuduStatus* KuduClientBuilder_Build(
      C_KuduClientBuilder* self,
      C_KuduClient** client);

  //-------------
  C_KuduTableCreator* KuduClient_NewTableCreator(C_KuduClient* self);
  void KuduClient_Free(C_KuduClient* self);

  C_KuduStatus* KuduClient_TableExists(C_KuduClient* self,
                                       const char* table_name,
                                       int* exists);

  C_KuduStatus* KuduClient_OpenTable(C_KuduClient* self,
                                     const char* table_name,
                                     C_KuduTable** table);

  C_KuduSession* KuduClient_NewSession(C_KuduClient* self);

  //-------------
  void KuduSession_Close(C_KuduSession* self);
  C_KuduStatus* KuduSession_SetFlushMode(C_KuduSession* self,
                                         C_FlushMode mode);

  C_KuduStatus* KuduSession_Apply(C_KuduSession* self,
                                  C_KuduWriteOperation* op);

  C_KuduStatus* KuduSession_Flush(C_KuduSession* self);


  //-------------
  void KuduTable_Close(C_KuduTable* table);
  C_KuduWriteOperation* KuduTable_NewInsert(C_KuduTable* self);

  //-------------
  C_KuduStatus* KuduWriteOperation_SetInt32(C_KuduWriteOperation* op,
                                            const char* col_name,
                                            int32_t val);

  C_KuduScanner* KuduTable_NewScanner(C_KuduTable* self);

  //-------------
  void KuduScanner_Free(C_KuduScanner* scanner);
  C_KuduStatus* KuduScanner_SetProjectedColumns(C_KuduScanner* self,
                                                const char** col_names,
                                                int n_cols);
  C_KuduStatus* KuduScanner_Open(C_KuduScanner* self);
  int KuduScanner_HasMoreRows(C_KuduScanner* self);
  C_KuduStatus* KuduScanner_NextBatch(C_KuduScanner* self,
                                      C_KuduScanBatch** batch);

  //-------------
  void KuduScanBatch_Free(C_KuduScanBatch* self);

  int KuduScanBatch_HasNext(C_KuduScanBatch* self);
  void KuduScanBatch_SeekNext(C_KuduScanBatch* self);
  // allocates result (caller must free)
  const char* KuduScanBatch_Row_ToString(C_KuduScanBatch* self);

  //-------------
  C_KuduSchemaBuilder* KuduSchemaBuilder_Create();
  void KuduSchemaBuilder_Free(C_KuduSchemaBuilder* self);
  C_KuduColumnSpec* KuduSchemaBuilder_AddColumn(C_KuduSchemaBuilder* self,
                                                const char* name);
  void KuduSchemaBuilder_SetPrimaryKey(C_KuduSchemaBuilder* self,
                                       const char** col_names,
                                       int n_cols);
  C_KuduStatus* KuduSchemaBuilder_Build(C_KuduSchemaBuilder* self,
                                        C_KuduSchema** out_schema);

    //-------------


  C_KuduColumnSpec* KuduColumnSpec_SetType(C_KuduColumnSpec* self, int type);
  C_KuduColumnSpec* KuduColumnSpec_SetNotNull(C_KuduColumnSpec* self);

  
  //-------------
  void KuduTableCreator_Free(C_KuduTableCreator* self);

  void KuduTableCreator_SetTableName(C_KuduTableCreator* self,
                                     const char* name);

  void KuduTableCreator_SetSchema(C_KuduTableCreator* self,
                                  C_KuduSchema* schema);

  void KuduTableCreator_AddHashPartitions(C_KuduTableCreator* self,
                                          const char** col_names,
                                          int n_cols,
                                          int num_buckets);

  C_KuduStatus* KuduTableCreator_Create(C_KuduTableCreator* self);

#ifdef __cplusplus
}
#endif
