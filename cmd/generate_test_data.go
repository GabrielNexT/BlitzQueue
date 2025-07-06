package main

import (
	"encoding/json"
	"math/rand"
	"os"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

type Data struct {
	Data     string `json:"data"`
	Priority *int   `json:"priority,omitempty"`
}

func main() {
	stringList := make([]Data, 1e7)

	for i := 0; i < 1e7; i++ {
		stringList[i] = Data{
			Data: randSeq(200),
		}
	}

	jsonData, _ := json.Marshal(stringList)

	f, err := os.Create("test_data.json")

	if err != nil {
		panic(err)
	}

	_, err = f.Write(jsonData)

	if err != nil {
		panic(err)
	}

}
