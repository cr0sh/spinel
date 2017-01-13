package spinel

// ListElement defines a list entry in user rankings.
type ListElement struct {
	Rank          int `json:",string"`
	Move          int `json:",string"`
	Icon          string
	ImgURL        string
	Nick          string
	Job           string
	Detail_job    string
	Level         int   `json:",string"`
	Exp           int64 `json:",string"`
	Popular       int   `json:",string"`
	Guild         string
	Guild_seq     int `json:",string"`
	Guild_worldid int `json:",string"`
}