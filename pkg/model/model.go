package model

type Spans []Span

type Span struct {
	Context
	Point
	Consumption
	Timing
}

type Context struct {
	CodeCID   string `json:"code_cid,omitempty" db:"code_cid"`
	MethodNum uint8  `json:"method_num,omitempty" db:"method_num"`
}

type Point struct {
	Event string `json:"event,omitempty"`
	Label string `json:"label,omitempty"`
}

type Consumption struct {
	FuelConsumed *uint64 `json:"fuel_consumed,omitempty"`
	GasConsumed  *uint64 `json:"gas_consumed,omitempty"`
}

type Timing struct {
	ElapsedCumNs uint64 `json:"elapsed_cum_ns,omitempty"`
	ElapsedRelNs uint64 `json:"elapsed_rel_ns,omitempty"`
}
