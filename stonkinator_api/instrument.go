package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MarketList struct {
	Id         primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	MarketList string             `bson:"market_list" json:"market_list"`
}

type Stock struct {
	Id            primitive.ObjectID   `bson:"_id,omitempty" json:"id"`
	Symbol        string               `bson:"symbol" json:"symbol"`
	Industry      string               `bson:"industry" json:"industry"`
	Instrument    string               `bson:"instrument" json:"instrument"`
	MarketListIds []primitive.ObjectID `bson:"market_list_ids" json:"market_list_ids"`
}

type Instrument struct {
	Id         string `json:"id,omitempty"`
	ExchangeId string `json:"exchange_id"`
	Symbol     string `json:"symbol"`
}

// Move func elsewhere?
func convertStringIds(ids []string) ([]primitive.ObjectID, error) {
	var objIds []primitive.ObjectID

	for _, idStr := range ids {
		objId, err := primitive.ObjectIDFromHex(idStr)
		if err != nil {
			return nil, err
		} else {
			objIds = append(objIds, objId)
		}
	}

	return objIds, nil
}

func instrumentAction(pgPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getInstrument(w, r, pgPool)

		case http.MethodPost:
			insertInstrument(w, r, pgPool)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func getInstrument(w http.ResponseWriter, r *http.Request, pgPool *pgxpool.Pool) {
	symbol := r.URL.Query().Get("symbol")

	query := pgPool.QueryRow(
		context.Background(),
		`
			SELECT id, exchange_id, symbol
			FROM instruments
			WHERE UPPER(symbol) = $1
		`,
		strings.ToUpper(symbol),
	)

	var instrument Instrument
	err := query.Scan(&instrument.Id, &instrument.ExchangeId, &instrument.Symbol)
	if err != nil {
		http.Error(w, "Failed to get instrument", http.StatusNoContent)
		return
	}

	jsonInstrument, err := json.Marshal(instrument)
	if err != nil {
		http.Error(w, "Failed to marshal data", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonInstrument)
}

func insertInstrument(w http.ResponseWriter, r *http.Request, pgPool *pgxpool.Pool) {
	var instrument Instrument
	err := json.NewDecoder(r.Body).Decode(&instrument)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	result, err := pgPool.Exec(
		context.Background(),
		`
			INSERT INTO instruments(exchange_id, symbol)
			VALUES($1, $2)
			ON CONFLICT DO NOTHING
		`,
		instrument.ExchangeId, instrument.Symbol,
	)
	if err != nil {
		http.Error(w, "Error while inserting into the database", http.StatusInternalServerError)
		return
	}
	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		http.Error(w, "Failed to insert instrument", http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, rowsAffected)
}

func instrumentsAction(mdb *mongo.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getMarketListInstruments(w, r, mdb)

		case http.MethodPost:
			insertStock(w, r, mdb)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func insertStock(w http.ResponseWriter, r *http.Request, mdb *mongo.Database) {
	marketListId := r.URL.Query().Get("id")
	var stock Stock
	if err := json.NewDecoder(r.Body).Decode(&stock); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	marketListObjectId, err := primitive.ObjectIDFromHex(marketListId)
	if err != nil {
		http.Error(w, "Failed to parse id", http.StatusBadRequest)
		return
	}

	collection := mdb.Collection(INSTRUMENTS_COLLECTION)

	filter := bson.M{"symbol": stock.Symbol}
	update := bson.M{
		"$set": bson.M{
			"symbol":     stock.Symbol,
			"industry":   stock.Industry,
			"instrument": stock.Instrument,
		},
		"$addToSet": bson.M{
			"market_list_ids": marketListObjectId,
		},
	}
	options := options.Update().SetUpsert(true)

	var result *mongo.UpdateResult
	result, err = collection.UpdateOne(context.Background(), filter, update, options)
	if err != nil {
		http.Error(w, "Failed to insert market list", http.StatusInternalServerError)
		return
	}

	jsonResult, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "Failed to marshal data", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResult)
}

func getMarketListInstruments(w http.ResponseWriter, r *http.Request, mdb *mongo.Database) {
	marketListId := r.URL.Query().Get("id")
	projection := r.URL.Query().Get("projection")

	marketListObjectId, err := primitive.ObjectIDFromHex(marketListId)
	if err != nil {
		http.Error(w, "Failed to parse id", http.StatusBadRequest)
		return
	}

	collection := mdb.Collection(MARKET_LISTS_COLLECTION)

	pipeline := mongo.Pipeline{
		bson.D{{
			"$match", bson.D{{"_id", marketListObjectId}},
		}},
		bson.D{{
			"$lookup", bson.D{
				{"from", INSTRUMENTS_COLLECTION},
				{"localField", "_id"},
				{"foreignField", "market_list_ids"},
				{"as", "market_list_instruments"},
			},
		}},
	}
	projectionValue, err := strconv.Atoi(projection)
	if err == nil && projectionValue == 1 {
		pipeline = append(
			pipeline,
			bson.D{{
				"$project", bson.D{
					{"market_list_instruments._id", 1},
					{"market_list_instruments.symbol", 1},
				},
			}},
		)
	}

	var result bson.M
	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		http.Error(w, "Failed to execute query", http.StatusInternalServerError)
		return
	}
	defer cursor.Close(context.Background())
	if cursor.Next(context.Background()) {
		if err := cursor.Decode(&result); err != nil {
			http.Error(w, "Failed to retrieve result", http.StatusInternalServerError)
			return
		}
	} else {
		http.Error(w, "No documents found", http.StatusNoContent)
		return
	}

	jsonMarketListsInstruments, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "Failed to marshal data", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonMarketListsInstruments)
}

func getSectorInstruments(mdb *mongo.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		sector := r.URL.Query().Get("sector")

		collection := mdb.Collection(INSTRUMENTS_COLLECTION)

		var results []bson.M
		cursor, err := collection.Find(context.Background(), bson.M{"industry": sector})
		if err != nil {
			http.Error(w, "Failed to execute query", http.StatusInternalServerError)
			return
		}
		defer cursor.Close(context.Background())
		if err = cursor.All(context.Background(), &results); err != nil {
			http.Error(w, "Failed to retrieve result", http.StatusInternalServerError)
			return
		}
		if len(results) == 0 {
			http.Error(w, "No documents found", http.StatusNoContent)
			return
		}

		jsonSectors, err := json.Marshal(results)
		if err != nil {
			http.Error(w, "Failed to marshal data", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonSectors)
	}
}

func getSectors(mdb *mongo.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		collection := mdb.Collection(INSTRUMENTS_COLLECTION)

		results, err := collection.Distinct(context.Background(), "industry", bson.M{})
		if err != nil {
			http.Error(w, "Failed to execute query", http.StatusInternalServerError)
			return
		}

		jsonSectors, err := json.Marshal(results)
		if err != nil {
			http.Error(w, "Failed to marshal data", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonSectors)
	}
}

func getSectorInstrumentsForMarketLists(mdb *mongo.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		sector := r.URL.Query().Get("sector")
		type MarketListIdsBody struct {
			MarketListIds []string `json:"market-list-ids"`
		}
		var marketListIds MarketListIdsBody
		if err := json.NewDecoder(r.Body).Decode(&marketListIds); err != nil {
			http.Error(w, "Failed to parse request body", http.StatusBadRequest)
			return
		}
		objIds, err := convertStringIds(marketListIds.MarketListIds)
		if len(objIds) == 0 {
			http.Error(w, "Invalid object ID/IDs", http.StatusBadRequest)
			return
		}

		collection := mdb.Collection(INSTRUMENTS_COLLECTION)

		pipeline := mongo.Pipeline{
			bson.D{{
				"$match", bson.D{{"industry", sector}},
			}},
			bson.D{{
				"$match", bson.M{
					"$nor": []bson.M{{
						"market_list_ids": bson.M{
							"$nin": objIds,
						}},
					},
				},
			}},
		}

		var results []bson.M
		cursor, err := collection.Aggregate(context.Background(), pipeline)
		if err != nil {
			http.Error(w, "Failed to execute query", http.StatusInternalServerError)
			return
		}
		defer cursor.Close(context.Background())
		if err = cursor.All(context.Background(), &results); err != nil {
			http.Error(w, "Failed to retrieve result", http.StatusInternalServerError)
			return
		}
		if len(results) == 0 {
			http.Error(w, "No documents found", http.StatusNoContent)
			return
		}

		jsonSectorInstruments, err := json.Marshal(results)
		if err != nil {
			http.Error(w, "Failed to marshal data", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonSectorInstruments)
	}
}

func marketListAction(mdb *mongo.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getMarketListId(w, r, mdb)

		case http.MethodPost:
			insertMarketList(w, r, mdb)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func insertMarketList(w http.ResponseWriter, r *http.Request, mdb *mongo.Database) {
	var marketList MarketList
	if err := json.NewDecoder(r.Body).Decode(&marketList); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	collection := mdb.Collection(MARKET_LISTS_COLLECTION)

	var findResult bson.M
	var insertResult *mongo.InsertOneResult
	err := collection.FindOne(
		context.Background(),
		marketList,
		options.FindOne().SetProjection(bson.M{"_id": 1}),
	).Decode(&findResult)
	if err == mongo.ErrNoDocuments {
		insertResult, err = collection.InsertOne(context.Background(), marketList)
		if err != nil {
			http.Error(w, "Failed to insert market list", http.StatusInternalServerError)
			return
		}
	} else if err != nil {
		http.Error(w, "Failed to insert market list", http.StatusInternalServerError)
		return
	} else {
		response := map[string]string{"message": "Entry already exists"}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
		return
	}

	jsonInsertResult, err := json.Marshal(insertResult)
	if err != nil {
		http.Error(w, "Failed to marshal data", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonInsertResult)
}

func getMarketListId(w http.ResponseWriter, r *http.Request, mdb *mongo.Database) {
	marketList := r.URL.Query().Get("market-list")

	collection := mdb.Collection(MARKET_LISTS_COLLECTION)

	var result bson.M
	err := collection.FindOne(
		context.Background(),
		bson.M{"market_list": marketList},
		options.FindOne().SetProjection(bson.M{"_id": 1, "market_list": 0}),
	).Decode(&result)
	if err == mongo.ErrNoDocuments {
		http.Error(w, "No documents found", http.StatusNoContent)
		return
	} else if err != nil {
		http.Error(w, "Failed to execute query", http.StatusInternalServerError)
		return
	}

	jsonMarketListId, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "Failed to marshal data", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonMarketListId)
}

func getMarketLists(mdb *mongo.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		collection := mdb.Collection(MARKET_LISTS_COLLECTION)

		var results []bson.M
		cursor, err := collection.Find(context.Background(), bson.M{})
		if err != nil {
			http.Error(w, "Failed to execute query", http.StatusInternalServerError)
			return
		}
		defer cursor.Close(context.Background())
		if err = cursor.All(context.Background(), &results); err != nil {
			http.Error(w, "Failed to retrieve result", http.StatusInternalServerError)
			return
		}
		if len(results) == 0 {
			http.Error(w, "No documents found", http.StatusNoContent)
			return
		}

		jsonMarketLists, err := json.Marshal(results)
		if err != nil {
			http.Error(w, "Failed to marshal data", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonMarketLists)
	}
}
