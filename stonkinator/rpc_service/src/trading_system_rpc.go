package main

import (
	"context"
	"encoding/json"
	"errors"
	pb "stonkinator_rpc_service/stonkinator_rpc_service"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *server) InsertTradingSystem(ctx context.Context, req *pb.TradingSystem) (*pb.TradingSystem, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query := s.pgPool.QueryRow(
		ctx,
		`
			INSERT INTO trading_systems(trading_system_name, current_date_time)
			VALUES($1, $2)
			ON CONFLICT DO NOTHING
			RETURNING id, trading_system_name
		`,
		req.Name, req.CurrentDateTime.DateTime,
	)
	res := &pb.TradingSystem{}
	if err := query.Scan(&res.Id, &res.Name); err != nil && err == pgx.ErrNoRows {
		query = s.pgPool.QueryRow(
			ctx,
			`
				SELECT id, trading_system_name 
				FROM trading_systems 
				WHERE trading_system_name = $1
			`,
			req.Name,
		)

		if err = query.Scan(&res.Id, &res.Name); err != nil {
			s.errorLog.Println(err)
			return nil, err
		}
	} else if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	return res, nil
}

func (s *server) GetTradingSystem(ctx context.Context, req *pb.GetBy) (*pb.TradingSystem, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	var query pgx.Row
	strIdentifier := req.GetStrIdentifier()
	altStrIdentifier := req.GetAltStrIdentifier()
	if len(strIdentifier) > 0 {
		query = s.pgPool.QueryRow(
			ctx,
			`
				SELECT id, trading_system_name, current_date_time
				FROM trading_systems
				WHERE id = $1
			`,
			strIdentifier,
		)
	} else if len(altStrIdentifier) > 0 {
		query = s.pgPool.QueryRow(
			ctx,
			`
				SELECT id, trading_system_name, current_date_time
				FROM trading_systems
				WHERE trading_system_name = $1
			`,
			altStrIdentifier,
		)
	} else {
		err := errors.New("no identifier passed with the request")
		s.errorLog.Println(err)
		return nil, err
	}

	var dateTime time.Time
	res := &pb.TradingSystem{}
	if err := query.Scan(&res.Id, &res.Name, &dateTime); err != nil {
		s.errorLog.Println(err)
		return nil, err
	}
	res.CurrentDateTime = &pb.DateTime{DateTime: dateTime.Format(DATE_TIME_FORMAT)}

	return res, nil
}

func (s *server) UpdateTradingSystemMetrics(ctx context.Context, req *pb.TradingSystem) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	var metrics map[string]interface{}
	if err := json.Unmarshal([]byte(req.Metrics), &metrics); err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	result, err := s.pgPool.Exec(
		ctx,
		`
			UPDATE trading_systems
			SET metrics = $1
			WHERE trading_system_id = $2
		`,
		metrics, req.Id,
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: int32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) UpsertMarketState(ctx context.Context, req *pb.MarketState) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	var metrics map[string]interface{}
	if err := json.Unmarshal([]byte(req.Metrics), &metrics); err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	result, err := s.pgPool.Exec(
		ctx,
		`
			INSERT INTO market_states(instrument_id, trading_system_id, signal_date_time, metrics)
			VALUES($1, $2, $3, $4)
			ON CONFLICT(instrument_id, trading_system_id) DO UPDATE 
			SET signal_date_time = EXCLUDED.signal_date_time,
				metrics = EXCLUDED.metrics
		`,
		req.InstrumentId, req.TradingSystemId, req.SignalDateTime.DateTime, metrics,
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: int32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) GetMarketState(ctx context.Context, req *pb.GetBy) (*pb.MarketState, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query := s.pgPool.QueryRow(
		ctx,
		`
			SELECT instrument_id, trading_system_id, metrics
			FROM market_states
			WHERE instrument_id = $1
			AND trading_system_id = $2
		`,
		req.GetStrIdentifier(), req.GetAltStrIdentifier(),
	)

	res := &pb.MarketState{}
	if err := query.Scan(&res.InstrumentId, &res.TradingSystemId, &res.Metrics); err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	return res, nil
}

func (s *server) GetMarketStates(ctx context.Context, req *pb.GetBy) (*pb.MarketStates, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query, err := s.pgPool.Query(
		ctx,
		`
			SELECT instrument_id, trading_system_id, metrics
			FROM market_states
			WHERE trading_system_id = $1
		`,
		req.GetStrIdentifier(),
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}
	defer query.Close()

	marketStates := []*pb.MarketState{}
	for query.Next() {
		var marketState pb.MarketState
		err := query.Scan(
			&marketState.InstrumentId,
			&marketState.TradingSystemId,
			&marketState.Metrics,
		)

		if err == nil {
			marketStates = append(marketStates, &marketState)
		}
	}

	res := &pb.MarketStates{
		MarketStates: marketStates,
	}

	return res, nil
}

func (s *server) UpdateCurrentDateTime(ctx context.Context, req *pb.UpdateCurrentDateTimeRequest) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	dateTime, err := time.Parse(DATE_FORMAT, req.DateTime.DateTime)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	result, err := s.pgPool.Exec(
		ctx,
		`
			UPDATE trading_systems
			SET current_date_time = $1
			WHERE id = $2
		`,
		dateTime, req.TradingSystemId,
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: int32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) GetCurrentDateTime(ctx context.Context, req *pb.GetBy) (*pb.DateTime, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query := s.pgPool.QueryRow(
		ctx,
		`
			SELECT current_date_time
			FROM trading_systems
			WHERE id = $1
		`,
		req.GetStrIdentifier(),
	)

	var dateTime time.Time
	if err := query.Scan(&dateTime); err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.DateTime{
		DateTime: dateTime.Format(DATE_TIME_FORMAT),
	}

	return res, nil
}

func (s *server) UpsertOrder(ctx context.Context, req *pb.Order) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	createdDateTime, err := time.Parse(DATE_FORMAT, req.CreatedDateTime.DateTime)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	result, err := s.pgPool.Exec(
		ctx,
		`
			INSERT INTO orders(
				instrument_id, trading_system_id, order_type, order_action, created_date_time,
				active, direction_long, price, max_order_duration, duration
			)
			VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT(instrument_id, trading_system_id) DO UPDATE
			SET order_type = EXCLUDED.order_type, 
				order_action = EXCLUDED.order_action, 
				created_date_time = EXCLUDED.created_date_time,
				active = EXCLUDED.active, 
				direction_long = EXCLUDED.direction_long, 
				price = EXCLUDED.price, 
				max_order_duration = EXCLUDED.max_order_duration, 
				duration = EXCLUDED.duration
		`,
		req.InstrumentId, req.TradingSystemId, req.OrderType, req.Action, createdDateTime,
		req.Active, req.DirectionLong, req.Price, req.MaxDuration, req.Duration,
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: int32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) GetOrder(ctx context.Context, req *pb.GetBy) (*pb.Order, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query := s.pgPool.QueryRow(
		ctx,
		`
			SELECT instrument_id, trading_system_id, order_type, order_action, created_date_time, 
				active, direction_long, price, max_order_duration, duration
			FROM orders
			WHERE instrument_id = $1
			AND trading_system_id = $2
		`,
		req.GetStrIdentifier(), req.GetAltStrIdentifier(),
	)

	var createdDateTime time.Time
	res := &pb.Order{}
	if err := query.Scan(
		&res.InstrumentId, &res.TradingSystemId, &res.OrderType, &res.Action, &createdDateTime,
		&res.Active, &res.DirectionLong, &res.Price, &res.MaxDuration, &res.Duration,
	); err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res.CreatedDateTime = &pb.DateTime{
		DateTime: createdDateTime.Format(DATE_TIME_FORMAT),
	}

	return res, nil
}

func (s *server) InsertPosition(ctx context.Context, req *pb.Position) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	var positionData map[string]interface{}
	if err := json.Unmarshal([]byte(req.PositionData), &positionData); err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	dateTime, err := time.Parse(DATE_FORMAT, req.DateTime.DateTime)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	result, err := s.pgPool.Exec(
		ctx,
		`
			INSERT INTO positions(instrument_id, trading_system_id, date_time, position_data, serialized_position)
			VALUES($1, $2, $3, $4, $5)
		`,
		req.InstrumentId, req.TradingSystemId, dateTime, positionData, req.SerializedPosition,
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: int32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) InsertPositions(ctx context.Context, req *pb.Positions) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	batch := &pgx.Batch{}

	var err error
	for _, position := range req.Positions {
		dateTime, err := time.Parse(DATE_FORMAT, position.DateTime.DateTime)
		if err != nil {
			s.errorLog.Println(err)
		}

		batch.Queue(
			`
				INSERT INTO positions(instrument_id, trading_system_id, date_time, position_data, serialized_position)
				VALUES($1, $2, $3, $4, $5)
			`,
			position.InstrumentId, position.TradingSystemId, dateTime, position.PositionData, position.SerializedPosition,
		)
	}

	batchQueryResult := s.pgPool.SendBatch(ctx, batch)
	defer batchQueryResult.Close()

	numAffected := 0

	for range req.Positions {
		commandTag, err := batchQueryResult.Exec()
		if err != nil {
			s.errorLog.Println(err)
		} else {
			numAffected += int(commandTag.RowsAffected())
		}
	}

	res := &pb.CUD{
		NumAffected: int32(numAffected),
	}

	return res, err
}

func (s *server) GetPosition(ctx context.Context, req *pb.GetBy) (*pb.Position, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query := s.pgPool.QueryRow(
		ctx,
		`
			SELECT instrument_id, trading_system_id, date_time, position_data, serialized_position
			FROM positions
			WHERE instrument_id = $1
			AND trading_system_id = $2
			ORDER BY date_time DESC
			LIMIT 1
		`,
		req.GetStrIdentifier(), req.GetAltStrIdentifier(),
	)

	var dateTime time.Time
	res := &pb.Position{}
	if err := query.Scan(&res.InstrumentId, &res.TradingSystemId, &dateTime, &res.PositionData, &res.SerializedPosition); err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res.DateTime = &pb.DateTime{
		DateTime: dateTime.Format(DATE_TIME_FORMAT),
	}

	return res, nil
}

func (s *server) GetPositions(ctx context.Context, req *pb.GetBy) (*pb.Positions, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query, err := s.pgPool.Query(
		ctx,
		`
			SELECT instrument_id, trading_system_id, date_time, position_data, serialized_position
			FROM positions
			WHERE instrument_id = $1
			AND trading_system_id = $2
		`,
		req.GetStrIdentifier(), req.GetAltStrIdentifier(),
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}
	defer query.Close()

	positions := []*pb.Position{}
	for query.Next() {
		var position pb.Position
		var dateTime time.Time
		err = query.Scan(
			&position.InstrumentId,
			&position.TradingSystemId,
			&dateTime,
			&position.PositionData,
			&position.SerializedPosition,
		)

		if err == nil {
			position.DateTime = &pb.DateTime{
				DateTime: dateTime.Format(DATE_TIME_FORMAT),
			}
			positions = append(positions, &position)
		}
	}

	res := &pb.Positions{
		Positions: positions,
	}

	return res, nil
}

// func (s *server) IncrementNumOfPeriods(ctx context.Context, req *pb.NumOfPeriodsRequest) (*pb.CUD, error) {

// }

func (s *server) InsertTradingSystemModel(ctx context.Context, req *pb.TradingSystemModel) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	result, err := s.pgPool.Exec(
		ctx,
		`
			INSERT INTO trading_system_models(trading_system_id, instrument_id, serialized_model)
			VALUES($1, $2, $3)
		`,
		req.TradingSystemId, req.InstrumentId, req.SerializedModel,
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: int32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) GetTradingSystemModel(ctx context.Context, req *pb.GetBy) (*pb.TradingSystemModel, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	var query pgx.Row
	altStrIdentifier := req.GetAltStrIdentifier()
	if len(altStrIdentifier) > 0 {
		query = s.pgPool.QueryRow(
			ctx,
			`
				SELECT trading_system_id, instrument_id, serialized_model
				FROM trading_system_models
				WHERE trading_system_id = $1
				AND instrument_id = $2
			`,
			req.GetStrIdentifier(), req.GetAltStrIdentifier(),
		)
	} else {
		query = s.pgPool.QueryRow(
			ctx,
			`
				SELECT trading_system_id, instrument_id, serialized_model
				FROM trading_system_models
				WHERE trading_system_id = $1
			`,
			req.GetStrIdentifier(),
		)
	}

	res := &pb.TradingSystemModel{}
	if err := query.Scan(&res.TradingSystemId, &res.InstrumentId, &res.SerializedModel); err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	return res, nil
}
