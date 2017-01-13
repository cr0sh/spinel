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
	"github.com/cr0sh/spinel"
	"labix.org/v2/mgo/bson"
)

const chanBufSize = 32

var numberOfAvatars int64 = 0

func main() {
	listFetchers := make([]listFetcher, 4)
	avatarFetchers := make([]avatarFetcher, 48)

	idChan := make(chan int, chanBufSize)
	imgUrlChan := make(chan []spinel.ListElement, chanBufSize)

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
	listFetcherWg := new(sync.WaitGroup)
	avatarFetcherWg := new(sync.WaitGroup)

	for i := range listFetchers {
		listFetchers[i] = listFetcher{
			in:  idChan,
			out: imgUrlChan,
			wg:  listFetcherWg,
		}
		listFetcherWg.Add(1)
		go listFetchers[i].work()
	}

	for i := range avatarFetchers {
		avatarFetchers[i] = avatarFetcher{
			in: imgUrlChan,
			db: db,
			wg: avatarFetcherWg,
		}
		avatarFetcherWg.Add(1)
		go avatarFetchers[i].work()
	}
	// TODO: Fire workers, start statistics loggers with time.Tick(), numberOfAvatars updater goroutine
	go func() {
		for range time.Tick(time.Minute * 10) {
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

	stop := time.After(time.Hour * 16)
	for i := 1; i <= 1e6; i += 10 {
		select {
		case <-stop:
			log.Printf("Stopping workers...")
			close(idChan)
			listFetcherWg.Wait()
			close(imgUrlChan)
			avatarFetcherWg.Wait()
			log.Printf("All workers are stopped. Done.")
			return
		case idChan <- i:
		}
	}

}

type listFetcher struct {
	in  chan int
	out chan []spinel.ListElement
	wg  *sync.WaitGroup
}

func newListFetcher() listFetcher {
	in := make(chan int, chanBufSize)
	out := make(chan []spinel.ListElement, chanBufSize)
	l := listFetcher{in: in, out: out}
	go l.work()
	return l
}

func (l listFetcher) work() {
	for id := range l.in {
		req, err := http.NewRequest("POST", fmt.Sprintf(spinel.NxQueryFmt, id), nil)
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
			List   []spinel.ListElement `json:",omitempty"`
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
	in chan []spinel.ListElement
	db *bolt.DB
	wg *sync.WaitGroup
}

func newAvatarFetcher(db *bolt.DB) avatarFetcher {
	in := make(chan []spinel.ListElement, chanBufSize)
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
				b1, err := tx.CreateBucketIfNotExists([]byte("AvatarImages"))
				if err != nil {
					return fmt.Errorf("Error while getting image bucket: %s", err)
				}
				b2, err := tx.CreateBucketIfNotExists([]byte("PlayerMetadata"))
				if err != nil {
					return fmt.Errorf("Error while getting metadata bucket: %s", err)
				}
				buf := new(bytes.Buffer)
				if _, err := io.Copy(buf, resp.Body); err != nil {
					return fmt.Errorf("Error while copying body stream: %s", err)
				}
				b1.Put([]byte(strconv.FormatInt(int64(element.Rank), 10)), buf.Bytes())
				bs, err := bson.Marshal(element)
				if err != nil {
					return fmt.Errorf("Error while marshaling element to BSON: %s", err)
				}
				b2.Put([]byte(strconv.FormatInt(int64(element.Rank), 10)), bs)
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
