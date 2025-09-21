package main

import (
	"context"
	"encoding/json"
	"io"
	pb "stonkinator_rpc_service/stonkinator_rpc_service"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *server) GetOrInsertTradingSystem(ctx context.Context, req *pb.TradingSystem) (*pb.TradingSystem, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query := s.pgPool.QueryRow(
		ctx,
		`
			INSERT INTO trading_systems(trading_system_name, current_date_time)
			VALUES($1, $2)
			ON CONFLICT (trading_system_name) DO UPDATE
			SET trading_system_name = EXCLUDED.trading_system_name
			RETURNING id, trading_system_name, current_date_time
		`,
		req.Name, req.CurrentDateTime.DateTime,
	)

	var dateTime time.Time
	res := &pb.TradingSystem{}
	if err := query.Scan(&res.Id, &res.Name, &dateTime); err != nil {
		s.errorLog.Println(err)
		return nil, err
	}
	res.CurrentDateTime = &pb.DateTime{DateTime: dateTime.Format(DATE_TIME_FORMAT)}

	return res, nil
}

func (s *server) GetTradingSystemMetrics(ctx context.Context, req *pb.GetBy) (*pb.TradingSystem, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query := s.pgPool.QueryRow(
		ctx,
		`
			SELECT metrics
			FROM trading_systems
			WHERE id = $1
		`,
		req.GetStrIdentifier(),
	)

	res := &pb.TradingSystem{}
	if err := query.Scan(&res.Metrics); err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

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
			SET metrics = metrics || $1
			WHERE id = $2
		`,
		metrics, req.Id,
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: uint32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) RemoveTradingSystemRelations(ctx context.Context, req *pb.OperateOn) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	var numAffected int64 = 0
	result, err := s.pgPool.Exec(
		ctx,
		`
			DELETE FROM orders
			WHERE orders.trading_system_id = $1
		`,
		req.GetStrIdentifier(),
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}
	numAffected += result.RowsAffected()

	result, err = s.pgPool.Exec(
		ctx,
		`
			DELETE FROM positions
			WHERE positions.trading_system_id = $1
		`,
		req.GetStrIdentifier(),
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}
	numAffected += result.RowsAffected()

	result, err = s.pgPool.Exec(
		ctx,
		`
			DELETE FROM market_states
			WHERE market_states.trading_system_id = $1
		`,
		req.GetStrIdentifier(),
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}
	numAffected += result.RowsAffected()

	res := &pb.CUD{
		NumAffected: uint32(numAffected),
	}

	return res, nil
}

func (s *server) UpsertMarketState(ctx context.Context, req *pb.MarketState) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	var err error
	var metrics map[string]interface{}
	if err = json.Unmarshal([]byte(req.Metrics), &metrics); err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	var query string
	var queryArgs []interface{}
	if req.Action != "" {
		query = `
			INSERT INTO market_states(instrument_id, trading_system_id, signal_date_time, metrics, market_action)
			VALUES($1, $2, $3, $4, $5)
			ON CONFLICT(instrument_id, trading_system_id) DO UPDATE 
			SET signal_date_time = EXCLUDED.signal_date_time,
				metrics = EXCLUDED.metrics,
				market_action = EXCLUDED.market_action
		`
		queryArgs = []interface{}{req.InstrumentId, req.TradingSystemId, req.SignalDateTime.DateTime, metrics, req.Action}
	} else {
		query = `
			UPDATE market_states
			SET metrics = metrics || $1
			WHERE instrument_id = $2
			AND trading_system_id = $3
		`
		queryArgs = []interface{}{metrics, req.InstrumentId, req.TradingSystemId}
	}

	result, err := s.pgPool.Exec(ctx, query, queryArgs...)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: uint32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) GetMarketStates(req *pb.GetBy, stream pb.TradingSystemsService_GetMarketStatesServer) error {
	ctx, cancel := context.WithTimeout(stream.Context(), DB_TIMEOUT)
	defer cancel()

	query, err := s.pgPool.Query(
		ctx,
		`
			SELECT instrument_id, trading_system_id, metrics, market_action
			FROM market_states
			WHERE trading_system_id = $1
			AND market_action = $2
		`,
		req.GetStrIdentifier(), req.GetAltStrIdentifier(),
	)
	if err != nil {
		s.errorLog.Println(err)
		return err
	}
	defer query.Close()

	for query.Next() {
		var marketState pb.MarketState
		err := query.Scan(
			&marketState.InstrumentId,
			&marketState.TradingSystemId,
			&marketState.Metrics,
			&marketState.Action,
		)
		if err != nil {
			s.errorLog.Println(err)
			continue
		}

		if err := stream.Send(&marketState); err != nil {
			s.errorLog.Println(err)
			return err
		}
	}

	return nil
}

func (s *server) UpdateCurrentDateTime(ctx context.Context, req *pb.UpdateCurrentDateTimeRequest) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	result, err := s.pgPool.Exec(
		ctx,
		`
			UPDATE trading_systems
			SET current_date_time = $1
			WHERE id = $2
		`,
		req.DateTime.DateTime, req.TradingSystemId,
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: uint32(result.RowsAffected()),
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
		req.InstrumentId, req.TradingSystemId, req.OrderType, req.Action, req.CreatedDateTime.DateTime,
		req.Active, req.DirectionLong, req.Price, req.MaxDuration, req.Duration,
	)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: uint32(result.RowsAffected()),
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
	); err == pgx.ErrNoRows {
		return nil, nil
	} else if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res.CreatedDateTime = &pb.DateTime{
		DateTime: createdDateTime.Format(DATE_TIME_FORMAT),
	}

	return res, nil
}

func (s *server) UpsertPosition(ctx context.Context, req *pb.Position) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	var err error
	var positionData map[string]interface{}
	if err = json.Unmarshal([]byte(req.PositionData), &positionData); err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	var query string
	var queryArgs []interface{}
	if req.Id != "" {
		query = `
			UPDATE positions
			SET date_time = $1, position_data = $2, serialized_position = $3
			WHERE id = $4
		`
		queryArgs = []interface{}{req.DateTime.DateTime, positionData, req.SerializedPosition, req.Id}
	} else {
		query = `
			INSERT INTO positions(instrument_id, trading_system_id, date_time, position_data, serialized_position)
			VALUES($1, $2, $3, $4, $5)
		`
		queryArgs = []interface{}{req.InstrumentId, req.TradingSystemId, req.DateTime.DateTime, positionData, req.SerializedPosition}
	}

	result, err := s.pgPool.Exec(ctx, query, queryArgs...)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: uint32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) InsertPositions(stream pb.TradingSystemsService_InsertPositionsServer) error {
	ctx, cancel := context.WithTimeout(stream.Context(), DB_TIMEOUT)
	defer cancel()

	batch := &pgx.Batch{}
	var count int
	for {
		position, err := stream.Recv()
		if err == io.EOF {
			batchQueryResult := s.pgPool.SendBatch(ctx, batch)
			defer batchQueryResult.Close()

			var numAffected int64
			for i := 0; i < count; i++ {
				commandTag, err := batchQueryResult.Exec()
				if err != nil {
					s.errorLog.Println(err)
				} else {
					numAffected += commandTag.RowsAffected()
				}
			}

			return stream.SendAndClose(&pb.CUD{NumAffected: uint32(numAffected)})
		}
		if err != nil {
			s.errorLog.Println(err)
			return err
		}

		var positionData map[string]interface{}
		if err = json.Unmarshal([]byte(position.PositionData), &positionData); err != nil {
			s.errorLog.Println(err)
		}
		batch.Queue(
			`
				INSERT INTO positions(instrument_id, trading_system_id, date_time, position_data, serialized_position)
				VALUES($1, $2, $3, $4, $5)
			`,
			position.InstrumentId, position.TradingSystemId, position.DateTime.DateTime, positionData, position.SerializedPosition,
		)

		count++
	}
}

func (s *server) GetPosition(ctx context.Context, req *pb.GetBy) (*pb.Position, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	query := s.pgPool.QueryRow(
		ctx,
		`
			SELECT id, instrument_id, trading_system_id, date_time, position_data, serialized_position
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
	if err := query.Scan(&res.Id, &res.InstrumentId, &res.TradingSystemId, &dateTime, &res.PositionData, &res.SerializedPosition); err == pgx.ErrNoRows {
		return &pb.Position{}, nil
	} else if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res.DateTime = &pb.DateTime{
		DateTime: dateTime.Format(DATE_TIME_FORMAT),
	}

	return res, nil
}

func (s *server) GetPositions(req *pb.GetBy, stream pb.TradingSystemsService_GetPositionsServer) error {
	ctx, cancel := context.WithTimeout(stream.Context(), DB_TIMEOUT)
	defer cancel()

	query, err := s.pgPool.Query(
		ctx,
		`
			SELECT id, instrument_id, trading_system_id, date_time, position_data, serialized_position
			FROM positions
			WHERE instrument_id = $1
			AND trading_system_id = $2
			AND position_data ? 'active'
			AND (position_data ->> 'active')::boolean = false
			ORDER BY date_time DESC
		`,
		req.GetStrIdentifier(), req.GetAltStrIdentifier(),
	)
	if err != nil {
		s.errorLog.Println(err)
		return err
	}
	defer query.Close()

	for query.Next() {
		var position pb.Position
		var dateTime time.Time
		err = query.Scan(
			&position.Id,
			&position.InstrumentId,
			&position.TradingSystemId,
			&dateTime,
			&position.PositionData,
			&position.SerializedPosition,
		)
		if err != nil {
			s.errorLog.Println(err)
			continue
		}

		position.DateTime = &pb.DateTime{
			DateTime: dateTime.Format(DATE_TIME_FORMAT),
		}
		if err := stream.Send(&position); err != nil {
			s.errorLog.Println(err)
			return err
		}
	}

	return nil
}

func (s *server) GetTradingSystemPositions(req *pb.GetBy, stream pb.TradingSystemsService_GetTradingSystemPositionsServer) error {
	ctx, cancel := context.WithTimeout(stream.Context(), DB_TIMEOUT)
	defer cancel()

	query, err := s.pgPool.Query(
		ctx,
		`
			SELECT id, instrument_id, trading_system_id, date_time, position_data, serialized_position
			FROM positions
			WHERE trading_system_id = $1
			AND position_data ? 'active'
			AND (position_data ->> 'active')::boolean = false
			ORDER BY position_data->>'entry_dt' DESC
			LIMIT $2
		`,
		req.GetStrIdentifier(), req.GetAltIntIdentifier(),
	)
	if err != nil {
		s.errorLog.Println(err)
		return err
	}
	defer query.Close()

	for query.Next() {
		var position pb.Position
		var dateTime time.Time
		err = query.Scan(
			&position.Id,
			&position.InstrumentId,
			&position.TradingSystemId,
			&dateTime,
			&position.PositionData,
			&position.SerializedPosition,
		)
		if err != nil {
			s.errorLog.Println(err)
			continue
		}

		position.DateTime = &pb.DateTime{
			DateTime: dateTime.Format(DATE_TIME_FORMAT),
		}
		if err := stream.Send(&position); err != nil {
			s.errorLog.Println(err)
			return err
		}
	}

	return nil
}

func (s *server) InsertTradingSystemModel(ctx context.Context, req *pb.TradingSystemModel) (*pb.CUD, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	var query string
	var queryArgs []interface{}
	if len(req.OptionalIdentifier) > 0 {
		query = `
			INSERT INTO trading_system_models(trading_system_id, instrument_id, serialized_model)
			VALUES($1, $2, $3)
			ON CONFLICT(trading_system_id, instrument_id) DO UPDATE
			SET serialized_model = EXCLUDED.serialized_model
		`
		queryArgs = []interface{}{req.TradingSystemId, req.OptionalIdentifier, req.SerializedModel}
	} else {
		query = `
			INSERT INTO trading_system_models(trading_system_id, serialized_model)
			VALUES($1, $2)
			ON CONFLICT (trading_system_id) WHERE instrument_id IS NULL DO UPDATE
			SET serialized_model = EXCLUDED.serialized_model
		`
		queryArgs = []interface{}{req.TradingSystemId, req.SerializedModel}
	}

	result, err := s.pgPool.Exec(ctx, query, queryArgs...)
	if err != nil {
		s.errorLog.Println(err)
		return nil, err
	}

	res := &pb.CUD{
		NumAffected: uint32(result.RowsAffected()),
	}

	return res, nil
}

func (s *server) GetTradingSystemModel(ctx context.Context, req *pb.GetBy) (*pb.TradingSystemModel, error) {
	ctx, cancel := context.WithTimeout(ctx, DB_TIMEOUT)
	defer cancel()

	var query pgx.Row
	altStrIdentifier := req.GetAltStrIdentifier()
	res := &pb.TradingSystemModel{}
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
		if err := query.Scan(&res.TradingSystemId, &res.OptionalIdentifier, &res.SerializedModel); err != nil {
			s.errorLog.Println(err)
			return nil, err
		}
	} else {
		query = s.pgPool.QueryRow(
			ctx,
			`
				SELECT trading_system_id, serialized_model
				FROM trading_system_models
				WHERE trading_system_id = $1
			`,
			req.GetStrIdentifier(),
		)
		if err := query.Scan(&res.TradingSystemId, &res.SerializedModel); err != nil {
			s.errorLog.Println(err)
			return nil, err
		}
	}

	return res, nil
}
