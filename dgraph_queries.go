package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	// "time"

	"github.com/dgraph-io/dgo/v230"
	"github.com/dgraph-io/dgo/v230/protos/api"
)

const callSchema = `
	call_time: datetime @index(day) .
	latitude: float .
	longitude: float .
	duration: float .
	IMEI_FROM_UID: uid .
	IMEI_TO_UID: uid .
	MSDIN_UID: uid .
`

const deviceSchema = `
	IMEI: string @index(exact) .
	imeis_to: [uid] .
	incoming_msdin: [uid] .
	outgoing_msdin: [uid] .
`

const accountSchema = `
	MSDIN: string @index(exact) .
	imeis: [uid] .
	imeis_to: [uid] .
`

func alterSchema(client *dgo.Dgraph, schema string) error {
	ctx := context.Background()
	op := &api.Operation{Schema: schema}
	if err := client.Alter(ctx, op); err != nil {
		return fmt.Errorf("failed to alter schema: %w", err)
	}
	return nil
}

func upsertDevice(ctx context.Context, client *dgo.Dgraph, call *Call) error {
	if err := alterSchema(client, deviceSchema); err != nil {
		return err
	}

	txn := client.NewTxn()
	defer txn.Discard(ctx)

	// Upsert for device entities (IMEI_FROM and IMEI_TO)
	upsertQuery := fmt.Sprintf(`
		query {
			q(func: eq(IMEI, "%s")) {
				v as uid
			}
			q2(func: eq(IMEI, "%s")) {
				v2 as uid
			}
		}`, call.ImeiFrom, call.ImeiTo)

	mu := &api.Mutation{
		SetNquads: []byte(fmt.Sprintf(`
			uid(v) <IMEI> "%s" .
			uid(v) <dgraph.type> "device" .
			uid(v2) <IMEI> "%s" .
			uid(v2) <dgraph.type> "device" .
			uid(v) <imeis_to> uid(v2) .
			uid(v2) <imeis_to> uid(v) .
			`, call.ImeiFrom, call.ImeiTo)),
	}

	if _, err := txn.Do(ctx, &api.Request{Query: upsertQuery, Mutations: []*api.Mutation{mu}, CommitNow: true}); err != nil {
		log.Printf("Failed to upsert devices: %v", err)
		return err
	}
	return nil
}

func updateDevicesWithAccount(ctx context.Context, client *dgo.Dgraph, imeiFromUid, imeiToUid, msdin string) error {
	if err := alterSchema(client, deviceSchema); err != nil {
		return err
	}

	txn0 := client.NewTxn()
	defer txn0.Discard(ctx)

	accountUid, err := accountUidByMsdin(ctx, txn0, msdin)
	if err != nil {
		return err
	}

	ims := fmt.Sprintf(`<%s> <incoming_msdin> <%s> .`, imeiFromUid, accountUid)
	imu := &api.Mutation{
		SetNquads: []byte(ims),
		CommitNow: true,
	}

	if _, err := txn0.Mutate(context.Background(), imu); err != nil {
		return err
	}

	txn1 := client.NewTxn()
	defer txn1.Discard(ctx)

	oms := fmt.Sprintf(`<%s> <outgoing_msdin> <%s> .`, imeiToUid, accountUid)
	omu := &api.Mutation{
		SetNquads: []byte(oms),
		CommitNow: true,
	}

	if _, err := txn1.Mutate(context.Background(), omu); err != nil {
		return err
	}
	return err
}

func upsertAccount(ctx context.Context, client *dgo.Dgraph, call *Call) error {
	if err := alterSchema(client, accountSchema); err != nil {
		return err
	}

	txn := client.NewTxn()
	defer txn.Discard(ctx)

	imeiFromUid, err := deviceUidByImei(ctx, txn, call.ImeiFrom)
	if err != nil {
		return err
	}
	imeiToUid, err := deviceUidByImei(ctx, txn, call.ImeiTo)
	if err != nil {
		return err
	}

	// Upsert for account entities (MSDIN)
	upsertQuery := fmt.Sprintf(`query {
		var(func: eq(MSDIN, "%s")) {
			account as uid
		}
	}`, call.Msdin)

	mu := &api.Mutation{
		SetNquads: []byte(fmt.Sprintf(`
			uid(account) <MSDIN> "%s" .
			uid(account) <dgraph.type> "account" .
			uid(account) <imeis> <%s> .
			uid(account) <imeis_to> <%s> .`, call.Msdin, imeiFromUid, imeiToUid)),
	}

	if _, err := txn.Do(ctx, &api.Request{Query: upsertQuery, Mutations: []*api.Mutation{mu}, CommitNow: true}); err != nil {
		log.Printf("Failed to upsert account: %v", err)
		return err
	}
	return updateDevicesWithAccount(ctx, client, imeiFromUid, imeiToUid, call.Msdin)
}

func insertCall(ctx context.Context, client *dgo.Dgraph, call *Call) error {
	if err := alterSchema(client, callSchema); err != nil {
		return err
	}

	txn := client.NewTxn()
	defer txn.Discard(ctx)

	imeiFromUid, err := deviceUidByImei(ctx, txn, call.ImeiFrom)
	if err != nil {
		return err
	}

	imeiToUid, err := deviceUidByImei(ctx, txn, call.ImeiTo)
	if err != nil {
		return err
	}

	msdinUid, err := accountUidByMsdin(ctx, txn, call.Msdin)
	if err != nil {
		return err
	}

        mutation := &api.Mutation{
		SetNquads: []byte(fmt.Sprintf(`
			_:call <call_time> "%s" .
			_:call <latitude> "%f" .
			_:call <longitude> "%f" .
			_:call <duration> "%f" .
			_:call <IMEI_FROM_UID> <%s> .
			_:call <IMEI_TO_UID> <%s> .
			_:call <MSDIN_UID> <%s> .
			_:call <dgraph.type> "call" .
		`,
		call.CallTime, call.Latitude, call.Longitude, call.Duration, imeiFromUid, imeiToUid, msdinUid)),
		CommitNow: true,
	}
	if _, err := txn.Mutate(ctx, mutation); err != nil {
		log.Printf("Failed to insert call: %v", err)
		return err
	}
	return nil
}

func upsertAll(client *dgo.Dgraph, call *Call) error {
	ctx := context.Background()
	if err := upsertDevice(ctx, client, call); err != nil {
		return err
	}
	if err := upsertAccount(ctx, client, call); err != nil {
		return err
	}
	if err := insertCall(ctx, client, call); err != nil {
		return err
	}
	return nil
}

func deviceUidByImei(ctx context.Context, txn *dgo.Txn, imei string) (string, error) {
	query := fmt.Sprintf(`{
		Devices(func: eq(dgraph.type, "device")) @filter(eq(IMEI, %s)) {
			uid
		}
	}`, imei)

	resp, err := txn.Query(context.Background(), query)
	if err != nil {
		return "", err
	}

	var result struct {
		Devices []struct {
			UID string `json:"uid"`
		}
	}
	
	if err := json.Unmarshal(resp.Json, &result); err != nil {
		return "", err
	}
	
	if len(result.Devices) > 0 {
		return result.Devices[0].UID, nil
	}
	return "", errors.New("Device is not found")
}

func accountUidByMsdin(ctx context.Context, txn *dgo.Txn, msdin string) (string, error) {
	query := fmt.Sprintf(`{
		Accounts(func: eq(dgraph.type, "account")) @filter(eq(MSDIN, %s)) {
			uid
		}
	}`, msdin)

	resp, err := txn.Query(context.Background(), query)
	if err != nil {
		return "", err
	}

	var result struct {
		Accounts []struct {
			UID string `json:"uid"`
		}
	}
	
	if err := json.Unmarshal(resp.Json, &result); err != nil {
		return "", err
	}
	
	if len(result.Accounts) > 0 {
		return result.Accounts[0].UID, nil
	}
	return "", errors.New("Account is not found")
}
