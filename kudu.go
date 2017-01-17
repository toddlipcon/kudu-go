package kudu

import "errors"
import "unsafe"

// #cgo CXXFLAGS: --std=c++11 -Wall
// #cgo LDFLAGS: -lkudu_client
// #include "kudu.h"
// #include <stdlib.h>
import "C"

type Client C.C_KuduClient
type ClientBuilder C.C_KuduClientBuilder
type ColumnSpec C.C_KuduColumnSpec
type Scanner C.C_KuduScanner
type ScanBatch C.C_KuduScanBatch
type Schema C.C_KuduSchema
type SchemaBuilder C.C_KuduSchemaBuilder
type Session C.C_KuduSession
type Status C.C_KuduStatus
type Table C.C_KuduTable
type TableCreator C.C_KuduTableCreator
type WriteOperation C.C_KuduWriteOperation

const INT32 = C.KUDU_INT32

const AUTO_FLUSH_BACKGROUND = C.KUDU_AUTO_FLUSH_BACKGROUND

////////////////////////////////////////////////////////////

func statusToErr(status *C.C_KuduStatus) error {
	if status == nil {
		return nil
	}
	defer C.KuduStatus_Free(status)
	msg := C.KuduStatus_Message(status)
	defer C.free(unsafe.Pointer(msg))
	return errors.New(C.GoString(msg))
}

////////////////////////////////////////////////////////////

func NewClientBuilder() *ClientBuilder {
	return (*ClientBuilder)(C.KuduClientBuilder_Create())
}
func (b *ClientBuilder) Free() {
	C.KuduClientBuilder_Free(b)
}

func (b *ClientBuilder) AddMasterServerAddr(addr string) {
	c_addr := C.CString(addr)
	defer C.free(unsafe.Pointer(c_addr))
	C.KuduClientBuilder_add_master_server_addr(b, c_addr)
}

func (b *ClientBuilder) Build() (*Client, error) {
	var client *C.C_KuduClient
	if err := statusToErr(C.KuduClientBuilder_Build(b, &client)); err != nil {
		return nil, err
	}
	return (*Client)(client), nil
}

////////////////////////////////////////////////////////////

func NewSchemaBuilder() *SchemaBuilder {
	return (*SchemaBuilder)(C.KuduSchemaBuilder_Create())
}

func (sb *SchemaBuilder) Free() {
	C.KuduSchemaBuilder_Free(sb)
}

func (sb *SchemaBuilder) SetPrimaryKey(col_names []string) {
	c_names := make([]*C.char, len(col_names))
	for i, s := range col_names {
		c_names[i] = C.CString(s)
		defer C.free(unsafe.Pointer(c_names[i]))
	}
	C.KuduSchemaBuilder_SetPrimaryKey(sb, &c_names[0],
		C.int(len(c_names)))
}

func (sb *SchemaBuilder) AddColumn(col_name string) *ColumnSpec {
	c_name := C.CString(col_name)
	defer C.free(unsafe.Pointer(c_name))
	return (*ColumnSpec)(C.KuduSchemaBuilder_AddColumn(sb, c_name))
}

func (sb *SchemaBuilder) Build() (*Schema, error) {
	var schema *C.C_KuduSchema
	if err := statusToErr(C.KuduSchemaBuilder_Build(sb, &schema)); err != nil {
		return nil, err
	}
	return (*Schema)(schema), nil
}

////////////////////////////////////////////////////////////

func (cspec *ColumnSpec) SetType(col_type int) {
	C.KuduColumnSpec_SetType(cspec, C.int(col_type))
}

func (cspec *ColumnSpec) SetNotNull() {
	C.KuduColumnSpec_SetNotNull(cspec)
}

////////////////////////////////////////////////////////////

func (tc *TableCreator) Free() {
	C.KuduTableCreator_Free(tc)
}

func (tc *TableCreator) SetSchema(schema *Schema) {
	C.KuduTableCreator_SetSchema(tc, schema)
}

func (tc *TableCreator) SetTableName(name string) {
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))
	C.KuduTableCreator_SetTableName(tc, c_name)
}

func (tc *TableCreator) Create() error {
	return statusToErr(C.KuduTableCreator_Create(tc))
}

func (tc *TableCreator) AddHashPartitions(col_names []string, num_buckets int) {
	c_names := make([]*C.char, len(col_names))
	for i, s := range col_names {
		c_names[i] = C.CString(s)
		defer C.free(unsafe.Pointer(c_names[i]))
	}
	C.KuduTableCreator_AddHashPartitions(tc, &c_names[0], C.int(len(c_names)), C.int(num_buckets))
}

////////////////////////////////////////////////////////////

func (c *Client) Close() {
	C.KuduClient_Free(c)
}

func (c *Client) NewTableCreator() *TableCreator {
	return (*TableCreator)(C.KuduClient_NewTableCreator(c))
}

func (c *Client) TableExists(name string) (bool, error) {
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))
	var exists C.int
	if err := statusToErr(C.KuduClient_TableExists(c, c_name, &exists)); err != nil {
		return false, err
	}
	return exists != 0, nil
}

func (c *Client) OpenTable(name string) (*Table, error) {
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))
	var table *C.C_KuduTable
	if err := statusToErr(C.KuduClient_OpenTable(c, c_name, &table)); err != nil {
		return nil, err
	}
	return (*Table)(table), nil
}

func (c *Client) NewSession() *Session {
	return (*Session)(C.KuduClient_NewSession(c))
}

////////////////////////////////////////////////////////////

func (s *Session) Close() {
	C.KuduSession_Close(s)
}

func (s *Session) Apply(op *WriteOperation) error {
	return statusToErr(C.KuduSession_Apply(s, op))
}

func (s *Session) SetFlushMode(mode C.C_FlushMode) error {
	return statusToErr(C.KuduSession_SetFlushMode(s, mode))
}

func (s *Session) Flush() error {
	return statusToErr(C.KuduSession_Flush(s))
}

////////////////////////////////////////////////////////////

func (t *Table) Close() {
	C.KuduTable_Close(t)
}

func (t *Table) NewInsert() *WriteOperation {
	return (*WriteOperation)(C.KuduTable_NewInsert(t))
}

func (t *Table) NewScanner() *Scanner {
	return (*Scanner)(C.KuduTable_NewScanner(t))
}

////////////////////////////////////////////////////////////

func (s *Scanner) Open() error {
	return statusToErr(C.KuduScanner_Open(s))
}
func (s *Scanner) Free() {
	C.KuduScanner_Free(s)
}
func (s *Scanner) NextBatch(batch **ScanBatch) error {
	var c_batch *C.C_KuduScanBatch
	err := statusToErr(C.KuduScanner_NextBatch(s, &c_batch))
	*batch = (*ScanBatch)(c_batch)
	return err
}
func (s *Scanner) HasMoreRows() bool {
	return C.KuduScanner_HasMoreRows(s) != 0
}

////////////////////////////////////////////////////////////
func (b *ScanBatch) Free() {
	C.KuduScanBatch_Free(b)
}
func (b *ScanBatch) Next() bool {
	if C.KuduScanBatch_HasNext(b) == 0 {
		return false
	}
	C.KuduScanBatch_SeekNext(b)
	return true
}

func (b *ScanBatch) RowToString() string {
	c_str := C.KuduScanBatch_Row_ToString(b)
	defer C.free(unsafe.Pointer(c_str))
	return C.GoString(c_str)
}

////////////////////////////////////////////////////////////
func (op *WriteOperation) SetInt32(col_name string, val int32) error {
	c_name := C.CString(col_name)
	defer C.free(unsafe.Pointer(c_name))
	return statusToErr(C.KuduWriteOperation_SetInt32(op, c_name, C.int32_t(val)))
}
