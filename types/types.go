package types

type Item struct {
	Id     string  `json:"id"`
	Year   int     `json:"year"`
	Title  string  `json:"title"`
	Plot   string  `json:"plot"`
	Rating float64 `json:"rating"`
}
