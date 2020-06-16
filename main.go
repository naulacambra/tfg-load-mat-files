package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"time"

	arangoDriver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"

	mongoDriver "go.mongodb.org/mongo-driver/mongo"
	mongoOptions "go.mongodb.org/mongo-driver/mongo/options"
)

type ChannelInfo struct {
	Channel int
	Value   int
	Time    time.Time
}

type ChannelInfoJson struct {
	Channel int       `json:"Channel"`
	Values  []int     `json:"Values"`
	From    time.Time `json:"From"`
	To      time.Time `json:"To"`
}

type ChannelInfoJsonList struct {
	Channel int       `json:"Channel"`
	Values  []int     `json:"Values"`
	From    time.Time `json:"From"`
	To      time.Time `json:"To"`
}

type ChannelInfoAveragedJsonList struct {
	Channel int       `json:"Channel"`
	Values  []float64 `json:"Values"`
	From    time.Time `json:"From"`
	To      time.Time `json:"To"`
}

func getArangoClient() arangoDriver.Client {
	conn, _ := http.NewConnection(http.ConnectionConfig{
		// Endpoints: []string{"http://161.35.218.190:8529/"},
		Endpoints: []string{"http://localhost:8529/"},
	})

	client, _ := arangoDriver.NewClient(arangoDriver.ClientConfig{
		Connection:     conn,
		Authentication: arangoDriver.BasicAuthentication("root", "root"),
	})

	return client
}

func getMongoClient(ctx context.Context) *mongoDriver.Client {
	client, err := mongoDriver.NewClient(mongoOptions.Client().ApplyURI("mongodb://127.0.0.1:27017"))

	if err != nil {
		fmt.Println("ERROR CONNECTING TO MONGO DATABASE : ", err)
	}

	err = client.Connect(ctx)

	if err != nil {
		fmt.Println("ERROR CONNECTING TO MONGO DATABASE : ", err)
	}

	return client
}

func getArangoDb(ctx context.Context, dbName string) arangoDriver.Database {
	client := getArangoClient()
	db, _ := client.Database(ctx, dbName)

	return db
}

func getMongoDb(ctx context.Context, dbName string) *mongoDriver.Database {
	client := getMongoClient(ctx)
	db := client.Database(dbName)

	return db
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour

	m := d / time.Minute
	d -= m * time.Minute

	s := d / time.Second
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func average(xs []int) float64 {
	total := int(0)
	for _, v := range xs {
		total += v
	}
	return float64(total) / float64(len(xs))
}

func originalLoadJSON() {
	ctx := context.Background()
	db := getArangoDb(ctx, "wifi-vis")
	colName := "coll_wifi_vis"

	// Create collection
	options := &arangoDriver.CreateCollectionOptions{
		WaitForSync: true,
	}
	start := time.Now()
	exists, _ := db.CollectionExists(ctx, colName)
	var col arangoDriver.Collection = nil

	if !exists {
		col, _ = db.CreateCollection(
			ctx,
			colName,
			options,
		)
	} else {
		col, _ = db.Collection(ctx, colName)
	}

	files, _ := ioutil.ReadDir("D:\\json")

	for fileIndex, f := range files {
		// loading file
		jsonFile, _ := ioutil.ReadFile(fmt.Sprintf("D:\\json\\%s", f.Name()))

		// parse title to get time info
		var channelInfoArr []interface{}
		_ = json.Unmarshal([]byte(json_file), &channelInfoArr)

		for _, channelInfo := range channelInfoArr {
			field, _ := channelInfo.(map[string]interface{})

			from, _ := time.Parse("02-Jan-2006 15:04:05", field["From"].(string))
			to, _ := time.Parse("02-Jan-2006 15:04:05", field["To"].(string))
			doc := ChannelInfoJson{
				Channel: int(field["Channel"].(float64)),
				Values:  field["Values"].([]int),
				From:    from,
				To:      to,
			}

			_, err := col.CreateDocument(ctx, doc)
			if err != nil {
				fmt.Println("document is not created", err)
			}
		}
	}
}

func loadJSON(ratio int) {
	ctx := context.Background()
	db := getArangoDb(ctx, "wifi-vis")
	colName := fmt.Sprintf("channel_info_collection_%d_test", ratio)

	// Create collection
	options := &arangoDriver.CreateCollectionOptions{
		WaitForSync: true,
	}
	start := time.Now()
	exists, _ := db.CollectionExists(ctx, colName)
	var col arangoDriver.Collection = nil

	if !exists {
		col, _ = db.CreateCollection(
			ctx,
			colName,
			options,
		)
	} else {
		col, _ = db.Collection(ctx, colName)
	}

	files, _ := ioutil.ReadDir(fmt.Sprintf("D:\\Documentos\\TFG\\json-%d", ratio))
	docs := []ChannelInfoJson{}
	batchSize := 10

	for fileIndex, f := range files {

		// loading file
		json_file, _ := ioutil.ReadFile(fmt.Sprintf("D:\\Documentos\\TFG\\json-%d\\%s", ratio, f.Name()))

		// parse title to get time info
		var channelInfoArr []interface{}
		_ = json.Unmarshal([]byte(json_file), &channelInfoArr)

		for _, channelInfo := range channelInfoArr {
			field, _ := channelInfo.(map[string]interface{})

			from, _ := time.Parse("02-Jan-2006 15:04:05", field["From"].(string))
			to, _ := time.Parse("02-Jan-2006 15:04:05", field["To"].(string))
			doc := ChannelInfoJson{
				Channel: int(field["Channel"].(float64)),
				Values:  field["Values"].([]int),
				From:    from,
				To:      to,
			}
			docs = append(docs, doc)
		}

		t := time.Now()
		elapsed := t.Sub(start)
		durStr := fmtDuration(elapsed)
		fmt.Println(fmt.Sprintf("full file %d loaded - %s", fileIndex, durStr))

		if (fileIndex+1)%batchSize == 0 {
			t := time.Now()
			elapsed := t.Sub(start)
			durStr := fmtDuration(elapsed)
			fmt.Println(fmt.Sprintf("batch documents loaded %s", durStr))

			_, _, err := col.CreateDocuments(ctx, docs)
			if err != nil {
				fmt.Println("Error in batch creation", err)
				os.Exit(-1)
			}

			t2 := time.Now()
			elapsed = t.Sub(t2)
			durStr = fmtDuration(elapsed)
			fmt.Println("batch documents inserted")
			fmt.Println(fmt.Sprintf("elapsed %s", durStr))

			docs = nil
			docs = []ChannelInfoJson{}
		}
	}

	t := time.Now()
	elapsed := t.Sub(start)
	durStr := fmtDuration(elapsed)
	fmt.Println(fmt.Sprintf("elapsed %s", durStr))
}

func transferJSON() {
	actx := arangoDriver.WithQueryCount(context.Background())
	mctx, _ := context.WithTimeout(context.Background(), 1000*time.Second)
	adb := getArangoDb(actx, "wifi-viewer")
	mdb := getMongoDb(mctx, "wifivisualizer")

	//defer cancel()

	fromCollectionName := "coll_wifi_vis"
	toCollectionName := "coll_wifi_vis"

	opts := mongoOptions.CreateCollection()

	mdb.CreateCollection(mctx, toCollectionName, opts)

	toCollection := mdb.Collection(toCollectionName)

	for channel := 1; channel <= 24; channel++ {
		query := fmt.Sprintf(`
			FOR doc IN %s
			FILTER doc.Channel == @channel
			RETURN {
				Channel: doc.Channel,
				Values: doc.Values,
				From: doc.From,
				To: doc.To
			}`, fromCollectionName)

		bindVars := map[string]interface{}{
			"channel": channel,
		}

		cursor, err := adb.Query(actx, query, bindVars)
		if err != nil {
			fmt.Println("ERROR IN INTERLINKING COUNT CURSOR : ", err)
		}

		defer cursor.Close()

		count := int(cursor.Count())

		for i := 0; i < count; i++ {
			var row ChannelInfoJson

			_, err := cursor.ReadDocument(actx, &row)

			opts := mongoOptions.InsertOne()
			_, err = toCollection.InsertOne(mctx, row, opts)

			if err != nil {
				fmt.Println("Item is not saved", err)
				break
			}

			if arangoDriver.IsNoMoreDocuments(err) {
				break
			}
		}
	}
}

func loadJSONList(ratio int) {
	actx := arangoDriver.WithQueryCount(context.Background())
	mctx, _ := context.WithTimeout(context.Background(), 1000*time.Second)
	adb := getArangoDb(actx, "wifi-viewer")
	mdb := getMongoDb(mctx, "wifivisualizer")

	//defer cancel()

	fromCollectionName := fmt.Sprintf("channel_info_collection_%d", ratio)
	toCollectionName := fmt.Sprintf("channel_info_list_%d", ratio)

	start := time.Now()

	opts := mongoOptions.CreateCollection()

	mdb.CreateCollection(mctx, toCollectionName, opts)

	toCollection := mdb.Collection(toCollectionName)

	for channel := 1; channel <= 24; channel++ {

		query := fmt.Sprintf(`
			FOR doc IN %s
			FILTER doc.Channel == @channel
			RETURN {
				Channel: doc.Channel,
				Values: doc.Values,
				From: doc.From,
				To: doc.To
			}`, fromCollectionName)

		bindVars := map[string]interface{}{
			"channel": channel,
		}

		cursor, err := adb.Query(actx, query, bindVars)
		if err != nil {
			fmt.Println("ERROR IN INTERLINKING COUNT CURSOR : ", err)
		} else {
			fmt.Println(fmt.Sprintf("Retrieved data from channel %d", channel))
		}

		defer cursor.Close()

		count := int(cursor.Count())

		fmt.Println(fmt.Sprintf("Ready to split and save data from channel %d, %d items", channel, count))
		for i := 0; i < count; i++ {
			var row ChannelInfoJson

			_, err := cursor.ReadDocument(actx, &row)
			var items []interface{}

			for j := 0; j < len(row.Values); j += 100 {

				valuesSlice := row.Values[j : j+100]

				item := ChannelInfoJsonList{
					Channel: row.Channel,
					Values:  valuesSlice,
					From:    row.From.Add(time.Millisecond * time.Duration(j)),
					To:      row.From.Add(time.Millisecond * time.Duration(j+100)),
				}

				items = append(items, item)
			}

			opts := mongoOptions.InsertMany().SetOrdered(false)
			_, err = toCollection.InsertMany(mctx, items, opts)

			if err != nil {
				fmt.Println("Items are not saved", err)
				break
			}

			fmt.Println(fmt.Sprintf("Saved data from channel %d, index %d", channel, i))

			if arangoDriver.IsNoMoreDocuments(err) {
				break
			}
		}

		fmt.Println(fmt.Sprintf("Splitted and saved data from channel %d", channel))
	}

	t := time.Now()
	elapsed := t.Sub(start)
	durStr := fmtDuration(elapsed)
	fmt.Println(fmt.Sprintf("elapsed %s", durStr))

}

func loadAndReduceJSONList(ratio int) {
	actx := arangoDriver.WithQueryCount(context.Background())
	mctx, _ := context.WithTimeout(context.Background(), 1000*time.Second)
	adb := getArangoDb(actx, "wifi-viewer")
	mdb := getMongoDb(mctx, "wifivisualizer")

	//defer cancel()

	//fromCollectionName := fmt.Sprintf("channel_info_collection_%d", ratio)
	fromCollectionName := fmt.Sprintf("channel_info_collection_1")
	toCollectionName := fmt.Sprintf("channel_info_list_averaged_from_1_to_%d", ratio)

	start := time.Now()

	opts := mongoOptions.CreateCollection()

	mdb.CreateCollection(mctx, toCollectionName, opts)

	toCollection := mdb.Collection(toCollectionName)

	for channel := 17; channel <= 24; channel++ {

		query := fmt.Sprintf(`
			FOR doc IN %s
			FILTER doc.Channel == @channel
			RETURN {
				Channel: doc.Channel,
				Values: doc.Values,
				From: doc.From,
				To: doc.To
			}`, fromCollectionName)

		bindVars := map[string]interface{}{
			"channel": channel,
		}

		cursor, err := adb.Query(actx, query, bindVars)
		if err != nil {
			fmt.Println("ERROR IN INTERLINKING COUNT CURSOR : ", err)
		} else {
			fmt.Println(fmt.Sprintf("Retrieved data from channel %d", channel))
		}

		defer cursor.Close()

		count := int(cursor.Count())

		fmt.Println(fmt.Sprintf("Ready to split and save data from channel %d, %d items", channel, count))
		for i := 0; i < count; i++ {
			var row ChannelInfoJson
			var averagedStruct ChannelInfoAveragedJsonList

			_, err := cursor.ReadDocument(actx, &row)
			//var items []interface{}
			var averagedValues []float64

			for j := 0; j < len(row.Values); j += ratio {

				valuesSlice := row.Values[j : j+ratio]

				averagedValue := average(valuesSlice)
				averagedValues = append(averagedValues, averagedValue)

				/*item := ChannelInfoJsonList{
					Channel: row.Channel,
					Values:  valuesSlice,
					From:    row.From.Add(time.Millisecond * time.Duration(j)),
					To:      row.From.Add(time.Millisecond * time.Duration(j+100)),
				}

				items = append(items, item)*/
			}
			averagedStruct.Channel = row.Channel
			averagedStruct.Values = averagedValues
			averagedStruct.From = row.From
			averagedStruct.To = row.To

			opts := mongoOptions.InsertOne()
			_, err = toCollection.InsertOne(mctx, averagedStruct, opts)

			if err != nil {
				fmt.Println("Items are not saved", err)
				break
			}

			fmt.Println(fmt.Sprintf("Saved data from channel %d, index %d", channel, i))

			if arangoDriver.IsNoMoreDocuments(err) {
				break
			}
		}

		fmt.Println(fmt.Sprintf("Splitted and saved data from channel %d", channel))
	}

	t := time.Now()
	elapsed := t.Sub(start)
	durStr := fmtDuration(elapsed)
	fmt.Println(fmt.Sprintf("elapsed %s", durStr))

}

func loadCsv() {
	ctx := context.Background()
	db := getArangoDb(ctx, "wifi-viewer")

	// Create collection
	options := &arangoDriver.CreateCollectionOptions{ /* ... */ }
	start := time.Now()
	col, err := db.CreateCollection(ctx, fmt.Sprintf("channel-info-collection-%d%d%d_%d%d%d", start.Day(), start.Month(), start.Year(), start.Hour(), start.Minute(), start.Second()), options)
	if err != nil {
		fmt.Println("collection is not created", err)
	} else {
		fmt.Println("collection created")
	}

	files, _ := ioutil.ReadDir("D:\\Documentos\\TFG\\csv")

	for fileIndex, f := range files {
		fmt.Println(fmt.Sprintf("start loading file %d", fileIndex))
		// loading file
		csv_file, _ := os.Open(fmt.Sprintf("D:\\Documentos\\TFG\\csv\\%s", f.Name()))
		dateReg := regexp.MustCompile(`it\d{4}_(?P<date>\d*)-(?P<month>\d*)-(?P<year>\d*)_(?P<hour>\d*)-(?P<minute>\d*)-(?P<second>\d*)`)

		// parse title to get time info
		match := dateReg.FindAllStringSubmatch(f.Name(), -1)

		date, _ := strconv.Atoi(match[0][1])
		month, _ := strconv.Atoi(match[0][2])
		year, _ := strconv.Atoi(match[0][3])
		hour, _ := strconv.Atoi(match[0][4])
		minute, _ := strconv.Atoi(match[0][5])
		second, _ := strconv.Atoi(match[0][6])

		reader := csv.NewReader(csv_file)
		channelIndex := 1

		for {
			fmt.Println(fmt.Sprintf("start loading channel %d", channelIndex))
			//read one line
			record, err := reader.Read()
			if err == io.EOF {
				break
			}

			// for each value
			for timeIndex, sValue := range record {
				value, _ := strconv.Atoi(sValue)

				// create document
				doc := ChannelInfo{
					Channel: channelIndex,
					Value:   value,
					Time:    time.Date(2000+year, time.Month(month), date, hour, minute, second, timeIndex*10000, time.UTC),
				}
				// save document
				_, err := col.CreateDocument(ctx, doc)
				if err != nil {
					fmt.Println("document is not created", err)
				}
				if timeIndex%10000 == 0 && timeIndex > 0 {
					fmt.Println(fmt.Sprintf("%dK rows inserted from channel %d from file %d", timeIndex/1000, channelIndex, fileIndex))
				}
			}

			channelIndex++

			t := time.Now()
			elapsed := t.Sub(start)
			durStr := fmtDuration(elapsed)
			fmt.Println(fmt.Sprintf("full channel inserted - %s", durStr))
		}
		fmt.Println("full file inserted")
	}

	t := time.Now()
	elapsed := t.Sub(start)
	durStr := fmtDuration(elapsed)

	fmt.Println(fmt.Sprintf("elapsed %s", durStr))
}

func main() {
	//loadCsv()
	//loadJson(1)
	//loadJson(10)
	loadAndReduceJSONList(100)
}
