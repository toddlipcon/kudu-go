#include <kudu/client/client.h>
#include <kudu/util/status.h>

#include "kudu.h"

#include <vector>
#include <string>
#include <iostream>

using namespace kudu::client;
using namespace kudu;
using std::cout;
using std::vector;
using std::string;


namespace {
template<class C1, class C2>
vector<C1> FromCVector(const C2* c_elems, int len) {
  vector<C1> ret;
  ret.reserve(len);
  for (int i = 0; i < len; i++) {
    ret.emplace_back(c_elems[i]);
  }
  return ret;
}
}

extern "C" {
  struct C_KuduStatus {
    Status status;
  };

  struct C_KuduClient {
    sp::shared_ptr<KuduClient> impl;
  };

  struct C_KuduTableCreator {
    KuduTableCreator* impl;
  };

  struct C_KuduSchema {
    KuduSchema impl;
  };

  struct C_KuduSchemaBuilder {
    KuduSchemaBuilder* impl;
  };

  struct C_KuduTable {
    sp::shared_ptr<KuduTable> impl;
  };

  struct C_KuduSession {
    sp::shared_ptr<KuduSession> impl;
  };

  struct C_KuduScanner {
    KuduScanner* impl;
  };

  struct C_KuduScanBatch {
    KuduScanBatch impl;
    KuduScanBatch::RowPtr cur_row;
    int cur_idx = -1;
  };
  
  struct C_KuduColumnSpec;
  struct C_KuduWriteOperation;
  
  // Get the message from the status. Must be freed with free().
  char* KuduStatus_Message(C_KuduStatus* self) {
    string msg = self->status.ToString();
    return strdup(msg.c_str());
  }
  
  void KuduStatus_Free(C_KuduStatus* self) {
    delete self;
  }

  static C_KuduStatus* MakeStatus(Status s) {
    if (s.ok()) return nullptr;
    return new C_KuduStatus { std::move(s) };
  }
  

  //////////////////

  struct C_KuduClientBuilder {
    KuduClientBuilder* impl;
  };

  C_KuduClientBuilder* KuduClientBuilder_Create() {
    auto ret = new C_KuduClientBuilder();
    ret->impl = new KuduClientBuilder();
    return ret;
  }


  void KuduClientBuilder_Free(C_KuduClientBuilder* self) {
    delete self->impl;
    delete self;
  }

  void KuduClientBuilder_add_master_server_addr(
      C_KuduClientBuilder* self,
      const char* addr) {
    self->impl->add_master_server_addr(string(addr));
  }

  C_KuduStatus* KuduClientBuilder_Build(
      C_KuduClientBuilder* self,
      C_KuduClient** client) {

    sp::shared_ptr<KuduClient> k_client;
    Status s = self->impl->Build(&k_client);
    if (!s.ok()) return MakeStatus(std::move(s));
    *client = new C_KuduClient { std::move(k_client) };
    return nullptr;
  }

  /////////////////////////////////

  C_KuduTableCreator* KuduClient_NewTableCreator(C_KuduClient* self) {
    return new C_KuduTableCreator { self->impl->NewTableCreator() };
  }

  void KuduClient_Free(C_KuduClient* client) {
    delete client;
  }

  C_KuduStatus* KuduClient_TableExists(C_KuduClient* self,
                                       const char* table_name,
                                       int* exists) {
    bool exists_b;
    Status s = self->impl->TableExists(string(table_name), &exists_b);
    if (!s.ok()) return MakeStatus(std::move(s));
    *exists = exists_b;
    return nullptr;
  }

  C_KuduStatus* KuduClient_OpenTable(C_KuduClient* self,
                                     const char* table_name,
                                     C_KuduTable** table) {
    sp::shared_ptr<KuduTable> c_table;
    Status s = self->impl->OpenTable(string(table_name), &c_table);
    if (!s.ok()) return MakeStatus(std::move(s));
    *table = new C_KuduTable { std::move(c_table) };
    return nullptr;
  }

  C_KuduSession* KuduClient_NewSession(C_KuduClient* self) {
    return new C_KuduSession { self->impl->NewSession() };
  }

  ////////////////////////////////////////////////////////////
  void KuduSession_Close(C_KuduSession* self) {
    delete self;
  }

  C_KuduStatus* KuduSession_SetFlushMode(C_KuduSession* self,
                                         C_FlushMode mode) {
    return MakeStatus(self->impl->SetFlushMode(static_cast<KuduSession::FlushMode>(mode)));
  }

  C_KuduStatus* KuduSession_Apply(C_KuduSession* self,
                                  C_KuduWriteOperation* op) {
    auto cpp_op = reinterpret_cast<KuduWriteOperation*>(op);
    return MakeStatus(self->impl->Apply(cpp_op));
  }

  C_KuduStatus* KuduSession_Flush(C_KuduSession* self) {
    return MakeStatus(self->impl->Flush());
  }

  ////////////////////////////////////////////////////////////
  C_KuduStatus* KuduWriteOperation_SetInt32(C_KuduWriteOperation* op,
                                            const char* col_name,
                                            int32_t val) {
    auto cpp_op = reinterpret_cast<KuduWriteOperation*>(op);
    return MakeStatus(cpp_op->mutable_row()->SetInt32(col_name, val));
  }

  ////////////////////////////////////////////////////////////
  void KuduTable_Close(C_KuduTable* self) {
    delete self;
  }
  C_KuduWriteOperation* KuduTable_NewInsert(C_KuduTable* self) {
    return reinterpret_cast<C_KuduWriteOperation*>(self->impl->NewInsert());
  }

  C_KuduScanner* KuduTable_NewScanner(C_KuduTable* self) {
    return new C_KuduScanner { new KuduScanner(self->impl.get()) };
  }

  ////////////////////////////////////////////////////////////

  void KuduScanner_Free(C_KuduScanner* self) {
    delete self->impl;
    delete self;
  }

  C_KuduStatus* KuduScanner_SetProjectedColumns(C_KuduScanner* self,
                                                const char** col_names,
                                                int n_cols) {
    return MakeStatus(self->impl->SetProjectedColumns(FromCVector<string>(col_names, n_cols)));
  }

  C_KuduStatus* KuduScanner_Open(C_KuduScanner* self) {
    return MakeStatus(self->impl->Open());
  }

  int KuduScanner_HasMoreRows(C_KuduScanner* self) {
    return self->impl->HasMoreRows();
  }

  C_KuduStatus* KuduScanner_NextBatch(C_KuduScanner* self,
                                      C_KuduScanBatch** batch) {
    if (*batch == nullptr) {
      *batch = new C_KuduScanBatch();
    }
    (*batch)->cur_idx = -1;
    return MakeStatus(self->impl->NextBatch(&(*batch)->impl));
  }

  ////////////////////////////////////////////////////////////

  void KuduScanBatch_Free(C_KuduScanBatch* self) {
    delete self;
  }

  int KuduScanBatch_HasNext(C_KuduScanBatch* self) {
    return self->cur_idx + 1 < self->impl.NumRows();
  }

  void KuduScanBatch_SeekNext(C_KuduScanBatch* self) {
    self->cur_idx++;
    self->cur_row = self->impl.Row(self->cur_idx);
  }

  const char* KuduScanBatch_Row_ToString(C_KuduScanBatch* self) {
    return strdup(self->cur_row.ToString().c_str());
  }
  
  ////////////////////////////////////////////////////////////
  void KuduSchema_Free(C_KuduSchema* self) {
    delete self;
  }

  ////////////////////////////////////////////////////////////
  C_KuduColumnSpec* KuduColumnSpec_SetType(C_KuduColumnSpec* self, int type) {
    reinterpret_cast<KuduColumnSpec*>(self)->Type(
        KuduColumnSchema::DataType(type));
    // TODO(todd) expose enum for datatype
    return self;
  }

  C_KuduColumnSpec* KuduColumnSpec_SetNotNull(C_KuduColumnSpec* self) {
    reinterpret_cast<KuduColumnSpec*>(self)->NotNull();
    return self;
  }

  
  ////////////////////////////////////////////////////////////
  C_KuduSchemaBuilder* KuduSchemaBuilder_Create() {
    return new C_KuduSchemaBuilder { new KuduSchemaBuilder() };
  }
  void KuduSchemaBuilder_Free(C_KuduSchemaBuilder* self) {
    delete self->impl;
    delete self;
  }

  C_KuduColumnSpec* KuduSchemaBuilder_AddColumn(C_KuduSchemaBuilder* self,
                                                const char* name) {
    return reinterpret_cast<C_KuduColumnSpec*>(self->impl->AddColumn(string(name)));
  }
  void KuduSchemaBuilder_SetPrimaryKey(C_KuduSchemaBuilder* self,
                                       const char** col_names,
                                       int n_cols) {
    auto names = FromCVector<string>(col_names, n_cols);
    self->impl->SetPrimaryKey(names);
  }
  C_KuduStatus* KuduSchemaBuilder_Build(C_KuduSchemaBuilder* self,
                                        C_KuduSchema** out_schema) {
    KuduSchema schema;
    Status s = self->impl->Build(&schema);
    if (!s.ok()) return MakeStatus(std::move(s));
    *out_schema = new C_KuduSchema { std::move(schema) };
    return nullptr;
  }

  ////////////////////////////////////////////////////////////

  void KuduTableCreator_Free(C_KuduTableCreator* self) {
    delete self->impl;
    delete self;
  }

  void KuduTableCreator_SetTableName(C_KuduTableCreator* self,
                                     const char* name) {
    self->impl->table_name(string(name));
  }

  void KuduTableCreator_SetSchema(C_KuduTableCreator* self,
                                  C_KuduSchema* schema) {
    self->impl->schema(&schema->impl);
  }
  
  void KuduTableCreator_AddHashPartitions(C_KuduTableCreator* self,
                                          const char** col_names,
                                          int n_cols,
                                          int num_buckets) {
    auto names = FromCVector<string>(col_names, n_cols);
    self->impl->add_hash_partitions(names, num_buckets);
    
  }

  
  C_KuduStatus* KuduTableCreator_Create(C_KuduTableCreator* self) {
    Status s = self->impl->Create();
    if (!s.ok()) return MakeStatus(std::move(s));
    return nullptr;
  }

}
