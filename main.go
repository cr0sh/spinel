package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"encoding/hex"
	"sync/atomic"

	"sync"
	"time"

	"github.com/boltdb/bolt"
)

const nxQueryFmt = `http://m.maplestory.nexon.com/MapleStory/Data/Json/Ranking/TotalRankingListJson.aspx?cateType=recent&rankidx=%d`
const chanBufSize = 32

var numberOfAvatars int64 = 0

func main() {
	listFetchers := make([]listFetcher, 8)
	avatarFetchers := make([]avatarFetcher, 8)

	idChan := make(chan int, chanBufSize)
	imgUrlChan := make(chan []listElement, chanBufSize)

	db, err := bolt.Open("database.db", 0600, nil)
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("numberOfAvatars"))
		if err != nil {
			return fmt.Errorf("Error while opening bucket: %s", err)
		}
		res := b.Get([]byte("value"))
		if len(res) != 0 {
			n, err := strconv.ParseInt(string(res), 10, 64)
			if err != nil {
				return fmt.Errorf("Invalid numOfAvatars value: \n%s", hex.Dump(res))
			}
			log.Printf("Current numOfAvatars value: %d (%05.2f%%)", n, float64(n)/1e6*100)
			atomic.StoreInt64(&numberOfAvatars, n)
		} else {
			log.Printf("Current numOfAvatars value: %d (%05.2f%%)", 0, float64(0))
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	wg := new(sync.WaitGroup)

	for i := range listFetchers {
		listFetchers[i] = listFetcher{
			in:  idChan,
			out: imgUrlChan,
			wg:  wg,
		}
		wg.Add(1)
		go listFetchers[i].work()
	}

	for i := range avatarFetchers {
		avatarFetchers[i] = avatarFetcher{
			in: imgUrlChan,
			db: db,
			wg: wg,
		}
		wg.Add(1)
		go avatarFetchers[i].work()
	}
	// TODO: Fire workers, start statistics loggers with time.Tick(), numberOfAvatars updater goroutine
	go func() {
		for range time.Tick(time.Minute * 5) {
			n := atomic.LoadInt64(&numberOfAvatars)
			log.Printf("Current numOfAvatars value: %d (%05.2f%%)", n, float64(n)/1e6*100)
		}
	}()

	go func() {
		for range time.Tick(time.Minute) {
			if err := db.Batch(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("numberOfAvatars"))
				if b == nil {
					return fmt.Errorf("Failed to get numberOfAvatars bucket: %s", err)
				}
				if err := b.Put(
					[]byte("value"),
					[]byte(strconv.FormatInt(atomic.LoadInt64(&numberOfAvatars), 10)),
				); err != nil {
					return fmt.Errorf("Error while storing numOfAvatars: %s", err)
				}
				return nil
			}); err != nil {
				log.Fatal(err)
			}
		}
	}()

	then := time.After(time.Hour * 14)
	for i := 1; i <= 1e6; i += 10 {
		select {
		case <-then:
			for i := range listFetchers {
				listFetchers[i].stop()
			}

			for i := range avatarFetchers {
				avatarFetchers[i].stop()
			}
			wg.Wait()
			return
		case idChan <- i:
		}
	}

}

type listElement struct {
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

type listFetcher struct {
	in  chan int
	out chan []listElement
	wg  *sync.WaitGroup
}

func newListFetcher() listFetcher {
	in := make(chan int, chanBufSize)
	out := make(chan []listElement, chanBufSize)
	l := listFetcher{in: in, out: out}
	go l.work()
	return l
}

func (l listFetcher) work() {
	for id := range l.in {
		req, err := http.NewRequest("POST", fmt.Sprintf(nxQueryFmt, id), nil)
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Add("Content-Type", "x-www-form-urlencoded")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
		}

		type jsonResult struct {
			Result string
			List   []listElement `json:",omitempty"`
		}
		result := new(jsonResult)
		if err := json.Unmarshal(func() []byte {
			buf := new(bytes.Buffer)
			io.Copy(buf, resp.Body)
			return buf.Bytes()
		}(), &result); err != nil {
			log.Fatal(err)
		}
		l.out <- result.List
	}
	if l.wg != nil {
		l.wg.Done()
	}
}

func (l listFetcher) stop() {
	close(l.in)
}

type avatarFetcher struct {
	in chan []listElement
	db *bolt.DB
	wg *sync.WaitGroup
}

func newAvatarFetcher(db *bolt.DB) avatarFetcher {
	in := make(chan []listElement, chanBufSize)
	a := avatarFetcher{in: in, db: db}
	go a.work()
	return a
}

func (a avatarFetcher) work() {
	for list := range a.in {
		for _, element := range list {
			if resp, err := http.Get(element.ImgURL); err != nil {
				log.Fatal(err)
			} else if err := a.db.Batch(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists([]byte("AvatarImages"))
				if err != nil {
					return fmt.Errorf("Error while creating bucket: %s", err)
				}
				buf := new(bytes.Buffer)
				if _, err := io.Copy(buf, resp.Body); err != nil {
					return fmt.Errorf("Error while copying body stream: %s", err)
				}
				b.Put([]byte(strconv.FormatInt(int64(element.Rank), 10)), buf.Bytes())
				return nil
			}); err != nil {
				log.Fatal(err)
			} else {
				atomic.AddInt64(&numberOfAvatars, 1)
			}
		}
	}
	if a.wg != nil {
		a.wg.Done()
	}
}

func (a avatarFetcher) stop() {
	close(a.in)
}
