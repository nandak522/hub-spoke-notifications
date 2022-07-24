# hub-spoke-notifications
A simple experiment showing hub-spoke design pattern for sending notifications, written in Golang, using channels and with read-write mutexes.

# Test the app for data races
```sh
go run -race main.go
```

# Run the app
```sh
go run main.go
```
