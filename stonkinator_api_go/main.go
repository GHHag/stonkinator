package main

// import (
// 	// "os"
// )

// const port string = os.Getenv("API_PORT")
// const api_url string = os.Getenv("API_URL")
const port string = "5000"
const api_url string = "/api"

func main() {
	register(port, api_url)
	testGobware()
}
