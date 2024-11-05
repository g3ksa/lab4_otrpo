package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

var (
	accessToken   string
	defaultUserID string
	logger        = log.New(os.Stdout, "VK_Neo4j_Fill: ", log.LstdFlags)
)

type User struct {
	ID            string
	ScreenName    string
	FirstName     string
	LastName      string
	Sex           int
	HomeTown      string
	Followers     []*User
	Subscriptions []*Group
}

type Group struct {
	ID         string
	Name       string
	ScreenName string
	Type       string
}

type VKData struct {
	Users  map[string]*User
	Groups map[string]*Group
}

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	accessToken = os.Getenv("ACCESS_TOKEN")
	defaultUserID = os.Getenv("DEFAULT_USER_ID")
	if defaultUserID == "" {
		defaultUserID = "337773226"
	}
}

func createNeo4jDriver() neo4j.DriverWithContext {
	uri := os.Getenv("NEO4J_URI")
	if uri == "" {
		uri = "bolt://localhost:7687"
	}
	user := os.Getenv("NEO4J_USER")
	password := os.Getenv("NEO4J_PASSWORD")

	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(user, password, ""))
	if err != nil {
		log.Fatalf("Failed to create Neo4j driver: %v", err)
	}
	return driver
}

func vkAPIRequest(method string, params map[string]string) (map[string]interface{}, error) {
	baseURL := "https://api.vk.com/method/"
	params["access_token"] = accessToken
	params["v"] = "5.131"

	url := baseURL + method + "?" + encodeParams(params)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}
	if errorData, found := result["error"]; found {
		logger.Printf("VK API Error: %v", errorData)
		return nil, fmt.Errorf("VK API Error: %v", errorData)
	}
	return result["response"].(map[string]interface{}), nil
}

func encodeParams(params map[string]string) string {
	var builder strings.Builder
	for k, v := range params {
		builder.WriteString(fmt.Sprintf("%s=%s&", k, v))
	}
	return builder.String()
}

func fetchFollowers(userID string) ([]*User, error) {
	followers := []*User{}
	offset, count := 0, 100

	for {
		response, err := vkAPIRequest("users.getFollowers", map[string]string{
			"user_id": userID,
			"fields":  "first_name,last_name,sex,city,screen_name",
			"offset":  strconv.Itoa(offset),
			"count":   strconv.Itoa(count),
		})
		if err != nil {
			return nil, err
		}

		items := response["items"].([]interface{})
		for _, item := range items {
			followerData := item.(map[string]interface{})
			user := &User{
				ID:        strings.Split(fmt.Sprintf("%f", followerData["id"]), ".")[0],
				FirstName: getString(followerData, "first_name", ""),
				LastName:  getString(followerData, "last_name", ""),
			}
			if followerData["city"] != nil {
				user.HomeTown = getString(followerData["city"].(map[string]interface{}), "title", "")
			}
			if followerData["sex"] != nil {
				user.Sex = int(followerData["sex"].(float64))
			}
			if followerData["screen_name"] != nil {
				user.ScreenName = getString(followerData, "screen_name", "")
			}
			followers = append(followers, user)
		}

		offset += count
		if offset >= int(response["count"].(float64)) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	return followers, nil
}

func fetchFollowersAndSubscriptions(userID string, depth int) (*VKData, error) {
	data := &VKData{
		Users:  make(map[string]*User),
		Groups: make(map[string]*Group),
	}
	var fetchLevel func(string, int)
	apiRequestCount := 0

	fetchLevel = func(uid string, currentDepth int) {
		if currentDepth > depth {
			return
		}

		if _, exists := data.Users[uid]; !exists {
			data.Users[uid] = &User{
				ID:            uid,
				Followers:     []*User{},
				Subscriptions: []*Group{},
			}
		}
		followers, err := fetchFollowers(uid)
		if err != nil {
			logger.Printf("Error fetching followers for %s: %v", uid, err)
			return
		}
		apiRequestCount++
		logger.Printf("VK API Request %d: users.getFollowers for user %s", apiRequestCount, uid)

		data.Users[uid].Followers = followers

		for _, follower := range followers {
			data.Users[follower.ID] = &User{}
			data.Users[follower.ID].ID = follower.ID
			data.Users[follower.ID].HomeTown = follower.HomeTown
			data.Users[follower.ID].ScreenName = follower.ScreenName
			data.Users[follower.ID].Sex = follower.Sex
			data.Users[follower.ID].FirstName = follower.FirstName
			data.Users[follower.ID].LastName = follower.LastName

			fetchLevel(follower.ID, currentDepth+1)
		}

		subscriptions, err := vkAPIRequest("users.getSubscriptions", map[string]string{
			"user_id":  uid,
			"extended": "1",
		})
		if err == nil {
			apiRequestCount++
			logger.Printf("VK API Request %d: users.getSubscriptions for user %s", apiRequestCount, uid)
			for _, item := range subscriptions["items"].([]interface{}) {
				subscription := item.(map[string]interface{})
				groupID := strconv.FormatFloat(subscription["id"].(float64), 'f', 0, 64)
				group := &Group{
					ID:         groupID,
					Name:       getString(subscription, "name", "Неизвестное сообщество"),
					ScreenName: getString(subscription, "screen_name", ""),
					Type:       getString(subscription, "type", "unknown"),
				}
				if _, exists := data.Groups[groupID]; !exists {
					data.Groups[groupID] = group
				}
				data.Users[uid].Subscriptions = append(data.Users[uid].Subscriptions, group)
			}
		}
	}
	fetchLevel(userID, 1)
	return data, nil
}

func getString(data map[string]interface{}, key string, defaultValue string) string {
	if data == nil {
		return defaultValue
	}
	if value, ok := data[key]; ok && value != nil {
		return value.(string)
	}
	return defaultValue
}

func saveDataToNeo4j(driver neo4j.DriverWithContext, data *VKData) {
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	for userID, user := range data.Users {
		_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
			return tx.Run(ctx, `
				MERGE (u:User {id: $id})
				SET u.screen_name = $screen_name,
					u.name = $name,
					u.sex = $sex,
					u.home_town = $home_town
			`, map[string]interface{}{
				"id":          userID,
				"name":        fmt.Sprintf("%s %s", user.FirstName, user.LastName),
				"screen_name": user.ScreenName,
				"sex":         user.Sex,
				"home_town":   user.HomeTown,
			})
		})
		if err != nil {
			logger.Printf("Error saving user %s: %v", userID, err)
		}
	}

	for groupID, group := range data.Groups {
		_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
			return tx.Run(ctx, `
				MERGE (g:Group {id: $id})
				SET g.name = $name,
					g.screen_name = $screen_name
			`, map[string]interface{}{
				"id":          groupID,
				"name":        group.Name,
				"screen_name": group.ScreenName,
			})
		})
		if err != nil {
			logger.Printf("Error saving group %s: %v", groupID, err)
		}
	}

	for userID, user := range data.Users {
		for _, follower := range user.Followers {
			_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
				return tx.Run(ctx, `
					MATCH (follower:User {id: $follower_id})
					MATCH (user:User {id: $user_id})
					MERGE (follower)-[:FOLLOWS]->(user)
				`, map[string]interface{}{
					"follower_id": follower.ID,
					"user_id":     userID,
				})
			})
			if err != nil {
				logger.Printf("Error creating FOLLOWS relationship for %s and %s: %v", follower.ID, userID, err)
			}
		}
	}

	for userID, user := range data.Users {
		for _, group := range user.Subscriptions {
			group := data.Groups[group.ID]
			if group.Type == "group" || group.Type == "page" || group.Type == "event" {
				_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
					return tx.Run(ctx, `
						MATCH (user:User {id: $user_id})
						MATCH (group:Group {id: $group_id})
						MERGE (user)-[:SUBSCRIBES]->(group)
					`, map[string]interface{}{
						"user_id":  userID,
						"group_id": group.ID,
					})
				})
				if err != nil {
					logger.Printf("Error creating SUBSCRIBES relationship for %s and group %s: %v", userID, group.ID, err)
				}
			}
		}
	}
}

func queryNeo4jData(db neo4j.DriverWithContext, queryType string) {
	ctx := context.Background()

	switch queryType {
	case "total_users":
		result, err := neo4j.ExecuteQuery(ctx, db, "MATCH (u:User) RETURN count(u) AS total_users", nil, neo4j.EagerResultTransformer)
		if err != nil {
			log.Fatalf("Query failed: %v", err)
		}
		for _, record := range result.Records {
			fmt.Println(record.AsMap())
		}

	case "total_groups":
		result, err := neo4j.ExecuteQuery(ctx, db, "MATCH (g:Group) RETURN count(g) AS total_groups", nil, neo4j.EagerResultTransformer)
		if err != nil {
			log.Fatalf("Query failed: %v", err)
		}
		for _, record := range result.Records {
			fmt.Println(record.AsMap())
		}

	case "top_users":
		fmt.Println("Топ 5 пользователей по количеству фолловеров:")
		result, err := neo4j.ExecuteQuery(ctx, db, `
			MATCH (u:User)<-[:FOLLOWS]-(f:User)
			RETURN u.name AS name, count(f) AS followers_count
			ORDER BY followers_count DESC LIMIT 5`, nil, neo4j.EagerResultTransformer)
		if err != nil {
			log.Fatalf("Query failed: %v", err)
		}
		for _, record := range result.Records {
			fmt.Println(record.AsMap())
		}

	case "top_groups":
		fmt.Println("Топ 5 самых популярных групп:")
		result, err := neo4j.ExecuteQuery(ctx, db, `
			MATCH (g:Group)<-[:SUBSCRIBES]-(u:User)
			RETURN g.name AS name, count(u) AS subscribers_count
			ORDER BY subscribers_count DESC LIMIT 5`, nil, neo4j.EagerResultTransformer)
		if err != nil {
			log.Fatalf("Query failed: %v", err)
		}
		for _, record := range result.Records {
			fmt.Println(record.AsMap())
		}

	case "mutual_followers":
		fmt.Println("Пользователи, которые фолловеры друг друга:")
		result, err := neo4j.ExecuteQuery(ctx, db, `
			MATCH (u1:User)-[:FOLLOWS]->(u2:User)
			WHERE (u2)-[:FOLLOWS]->(u1)
			RETURN u1.name AS User1, u2.name AS User2`, nil, neo4j.EagerResultTransformer)
		if err != nil {
			log.Fatalf("Query failed: %v", err)
		}
		for _, record := range result.Records {
			fmt.Println(record.AsMap())
		}

	default:
		fmt.Println("Неверный тип запроса. Доступные запросы: total_users, total_groups, top_users, top_groups, mutual_followers")
	}
}

func main() {
	action := flag.String("action", "", "Действие: fillDB, queries")
	queryType := flag.String("query_type", "", "Тип запроса: total_users, total_groups, top_users, top_groups, mutual_followers")
	flag.Parse()

	if *queryType == "" {
		fmt.Println("Укажите тип запроса: total_users, total_groups, top_users, top_groups, mutual_followers")
		return
	}
	if *action == "" {
		fmt.Println("Укажите действие: fillDB, queries")
		return
	}

	driver := createNeo4jDriver()
	defer driver.Close(context.Background())

	if *action == "queries" {
		queryNeo4jData(driver, *queryType)
		return
	} else if *action == "fillDB" {
		data, err := fetchFollowersAndSubscriptions(defaultUserID, 2)
		if err != nil {
			logger.Fatalf("Failed to fetch data: %v", err)
		}
		saveDataToNeo4j(driver, data)
	}
}
