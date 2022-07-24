package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type Message struct {
	topic string
	data  string
}

type Subscriber struct {
	Id    string
	Queue chan Message // This can be a web-socket too
}

type PubSubGroup struct {
	publisherQueue chan Message
	subscribers    []Subscriber
}

type NotificationHub struct {
	sync.RWMutex
	registry map[string]PubSubGroup // topic vs publisher and subscriber(s) combination
}

func (hub *NotificationHub) GetTopics() []string {
	fmt.Println("Hub lock is getting acquired...")
	hub.RLock()
	fmt.Println("Hub lock acquired...")
	defer hub.RUnlock()
	topics := []string{}
	for topic := range hub.registry {
		topics = append(topics, topic)
	}
	return topics
}

func (hub *NotificationHub) SetPubSubGroupForTopic(topic string, pubSubGroup PubSubGroup) {
	fmt.Println("Hub lock is getting acquired...")
	hub.Lock()
	defer hub.Unlock()
	fmt.Println("Hub lock acquired...")
	hub.registry[topic] = pubSubGroup
}

func (hub *NotificationHub) GetPubSubGroupForTopic(topic string) (PubSubGroup, bool) {
	fmt.Println("Hub lock is getting acquired...")
	hub.RLock()
	defer hub.RUnlock()
	fmt.Println("Hub lock acquired...")
	if pubSubGroup, ok := hub.registry[topic]; ok {
		return pubSubGroup, true
	}
	return PubSubGroup{}, false
}

func (hub *NotificationHub) RunConnector(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		for _, topic := range hub.GetTopics() {
			pubSubGroup, isTopicPresent := hub.GetPubSubGroupForTopic(topic)
			if !isTopicPresent {
				break
			}
			select {
			case msg := <-pubSubGroup.publisherQueue:
				fmt.Println(fmt.Sprintf("\t\t\t\t\t\tConnector received msg: '%s' from publisher: %s", msg.data, topic))
				if len(pubSubGroup.subscribers) == 0 {
					fmt.Println(fmt.Sprintf("\t\t\t\t\t\tConnector didn't find any subscribers for %s Topic. Hence ignoring the received msg", topic))
					break
				}
				for i := 0; i < len(pubSubGroup.subscribers); i += 1 {
					fmt.Println(fmt.Sprintf("\t\t\t\t\t\tConnector sent msg: '%s' to subscriber: %s", msg.data, pubSubGroup.subscribers[i].Id))
					pubSubGroup.subscribers[i].Queue <- msg
				}
			default:
				fmt.Println(fmt.Sprintf("\t\t\t\t\t\tConnector listening for new msgs. Nothing yet!"))
				time.Sleep(10 * time.Millisecond)
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func (subscriber *Subscriber) GetMessages() {
	for {
		select {
		case msg := <-subscriber.Queue:
			fmt.Println(fmt.Sprintf("\t\t\t\t\t\t%s Subscriber received: %s", subscriber.Id, msg))
		default:
			fmt.Println(fmt.Sprintf("\t\t\t\t\t\t%s Subscriber listening for new msgs. Nothing yet!", subscriber.Id))
			time.Sleep(1 * time.Second)
		}
	}
}

func (hub *NotificationHub) RegisterSubscriber(topic string, subscriberId string) (Subscriber, error) {
	pubSubGroup, isTopicPresent := hub.GetPubSubGroupForTopic(topic)
	if !isTopicPresent {
		return Subscriber{}, errors.New(fmt.Sprintf("\t\t\t\t\t\tTopic: %s not found in registry.", topic))
	}
	fmt.Println(fmt.Sprintf("\t\t\t\t\t\tAll existing subscribers against the topic: %s are: %v", topic, pubSubGroup.subscribers))
	subscriber := Subscriber{
		Id:    subscriberId,
		Queue: make(chan Message),
	}
	pubSubGroup.subscribers = append(pubSubGroup.subscribers, subscriber)
	hub.SetPubSubGroupForTopic(topic, pubSubGroup)
	return subscriber, nil
}

func sendMessage(hub *NotificationHub, topic, data string) {
	fmt.Println(fmt.Sprintf("Sending data: '%s' to topic: '%s'", data, topic))
	pubSubGroup, isTopicPresent := hub.GetPubSubGroupForTopic(topic)
	if isTopicPresent {
		pubSubGroup.publisherQueue <- Message{
			topic: topic,
			data:  data,
		}
		fmt.Println(fmt.Sprintf("Data sent: '%s' to topic: '%s'", data, topic))
	} else {
		fmt.Println(fmt.Sprintf("Topic: %s not found in registry. Creating...", topic))
		pubSubGroup := PubSubGroup{
			publisherQueue: make(chan Message),
			subscribers:    []Subscriber{},
		}
		hub.SetPubSubGroupForTopic(topic, pubSubGroup)
	}
}

// func doBuild(wg *sync.WaitGroup, hub *NotificationHub, topic string) {
func doBuild(hub *NotificationHub, topic string) {
	fmt.Println(fmt.Sprintf("Task started for %s topic", topic))
	for i := 0; i < 10; i += 1 {
		sendMessage(hub, topic, fmt.Sprintf("I am working on %s task. Item[%d]", topic, i))
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Println(fmt.Sprintf("Task finished for %s topic", topic))
}

func main() {
	hub := NotificationHub{
		registry: map[string]PubSubGroup{},
	}
	go doBuild(&hub, "build-123")
	go doBuild(&hub, "build-456")
	go doBuild(&hub, "proxy-refresh")
	time.Sleep(5 * time.Second)

	var connectionWG sync.WaitGroup
	go hub.RunConnector(&connectionWG)
	connectionWG.Add(1)

	time.Sleep(5 * time.Second)
	fmt.Println("\t\t\t\t\t\tSubscribers coming up now...")
	c1_subscriber, _ := hub.RegisterSubscriber("build-123", "c1")
	go c1_subscriber.GetMessages()
	c2_subscriber, _ := hub.RegisterSubscriber("build-123", "c2")
	go c2_subscriber.GetMessages()
	c3_subscriber, _ := hub.RegisterSubscriber("build-456", "c3")
	go c3_subscriber.GetMessages()
	c4_subscriber, _ := hub.RegisterSubscriber("proxy-refresh", "c4")
	go c4_subscriber.GetMessages()

	connectionWG.Wait()
}
