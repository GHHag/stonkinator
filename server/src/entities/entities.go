package entities

type Entities struct {
	Exchange      Exchange
	Instrument    Instrument
	MarketList    MarketList
	Watchlist     Watchlist
	Price         Price
	TradingSystem TradingSystem
	MarketState   MarketState
	Order         Order
	Position      Position
	User          User
}

func New() Entities {
	return Entities{
		Exchange:      Exchange{},
		Instrument:    Instrument{},
		MarketList:    MarketList{},
		Watchlist:     Watchlist{},
		Price:         Price{},
		TradingSystem: TradingSystem{},
		MarketState:   MarketState{},
		Order:         Order{},
		Position:      Position{},
		User:          User{},
	}
}
