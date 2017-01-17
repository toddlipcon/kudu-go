package main

import "fmt"
import ".."

const kTableName = "test"

func makeTable(client *kudu.Client) error {
	fmt.Println("making table")
	sb := kudu.NewSchemaBuilder()
	defer sb.Free()
	col := sb.AddColumn("c1")
	col.SetType(kudu.INT32)
	col.SetNotNull()
	sb.SetPrimaryKey([]string{"c1"})

	schema, err := sb.Build()
	if err != nil {
		panic(err)
	}
	tc := client.NewTableCreator()
	defer tc.Free()
	tc.SetSchema(schema)
	tc.SetTableName(kTableName)
	tc.AddHashPartitions([]string{"c1"}, 2)

	if err := tc.Create(); err != nil {
		return err
	}
	return nil
}

func connectToKudu() *kudu.Client {
	b := kudu.NewClientBuilder()
	defer b.Free()
	b.AddMasterServerAddr("localhost")
	client, err := b.Build()
	if err != nil {
		panic(err)
	}
	return client
}

func main() {
	client := connectToKudu()
	defer client.Close()

	// Make a table if it doesn't exist
	exists, err := client.TableExists(kTableName)
	if err != nil {
		panic(err)
	}
	if !exists {
		if err := makeTable(client); err != nil {
			panic(err)
		}
	}

	table, err := client.OpenTable(kTableName)
	if err != nil {
		panic(err)
	}
	defer table.Close()

	session := client.NewSession()
	defer session.Close()

	session.SetFlushMode(kudu.AUTO_FLUSH_BACKGROUND)
	for i := 0; i < 10; i++ {
		ins := table.NewInsert()
		if err := ins.SetInt32("c1", int32(i)); err != nil {
			panic(err)
		}
		if err := session.Apply(ins); err != nil {
			panic(err)
		}
	}
	if err := session.Flush(); err != nil {
		panic(err)
	}

	scanner := table.NewScanner()
	defer scanner.Free()
	if err := scanner.Open(); err != nil {
		panic(err)
	}
	var batch *kudu.ScanBatch
	defer batch.Free()
	for scanner.HasMoreRows() {
		if err := scanner.NextBatch(&batch); err != nil {
			panic(err)
		}
		for batch.Next() {
			fmt.Println("row: ", batch.RowToString())
		}
	}
}
