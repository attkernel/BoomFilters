package main

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/dicteam/wallet-base/db"
	"github.com/dicteam/wallet-base/models"
	"github.com/spf13/cobra"
	boom "github.com/tylertreat/BoomFilters"
)

var (
	sqlString  string
	resultPath string
	decStr     string
)

var rootCmd = &cobra.Command{
	Use:   "createBloomFilter",
	Short: "createBloomFilter",
	Long:  `createBloomFilter`,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&sqlString, "sqlString", "s", "root:12345678@tcp(127.0.0.1:3306)/testDb?charset=utf8&parseTime=True&loc=Local", "root:12345678@tcp(127.0.0.1:3306)/testDb?charset=utf8&parseTime=True&loc=Local")
	rootCmd.PersistentFlags().StringVarP(&resultPath, "resultPath", "p", "~", "~")
	rootCmd.PersistentFlags().StringVarP(&decStr, "dec", "d", "1000000", "1000000")
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func DoZlibCompress(src []byte) []byte {
	var in bytes.Buffer
	w := zlib.NewWriter(&in)
	w.Write(src)
	w.Close()
	return in.Bytes()
}

func Execute() error {
	rootCmd.Run = func(cmd *cobra.Command, args []string) {
		dec, err := strconv.ParseUint(decStr, 10, 64)
		if err != nil {
			panic(err)
		}
		sbf := boom.NewDefaultStableBloomFilter(uint(dec), 0.01)
		existDir, err := PathExists(resultPath)
		if err != nil {
			panic(err)
		}
		if !existDir {
			fmt.Printf("no dir![%v]\n", resultPath)
			err := os.MkdirAll(resultPath, os.ModePerm)
			if err != nil {
				fmt.Printf("mkdir failed![%v]\n", err)
				panic(err)
			} else {
				fmt.Printf("mkdir success!\n")
			}
		}
		_, err = db.New(sqlString, "")
		if err != nil {
			panic(err)
		}
		addrList := models.GetAllAddresses()
		for _, addrInof := range addrList {
			sbf.Add([]byte(strings.ToLower(addrInof.Address)))
		}
		bloomBytes, _ := sbf.GobEncode()
		compressedBytes := DoZlibCompress(bloomBytes)
		err = ioutil.WriteFile(resultPath+"/result", compressedBytes, 0644)
		if err != nil {
			panic(err)
		}
		sbf.Reset()
		fmt.Println("filled filter success!")
		compressedBits, _ := ioutil.ReadFile(resultPath + "/result")
		var out bytes.Buffer
		var num int
		newBuffer := bytes.NewBuffer(compressedBits)
		r, _ := zlib.NewReader(newBuffer)
		io.Copy(&out, r)
		sbfNew := boom.NewDefaultStableBloomFilter(uint(dec), 0.01)
		_ = sbfNew.GobDecode(out.Bytes())
		for _, addrInfo := range addrList {
			if !sbfNew.Test([]byte(strings.ToLower(addrInfo.Address))) {
				fmt.Println(addrInfo.Address)
				num++
			}
		}
		sbfNew.Reset()
		fmt.Printf("num: %d", num)
	}
	return rootCmd.Execute()
}

func main() {
	if err := Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
