package main

import (
	"C"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/streadway/amqp"
)

type rabbitmqFLBPluginContext struct {
	host                     string
	port                     string
	user                     string
	password                 string
	exchangeName             string
	exchangeType             string
	routingKey               string
	routingKeyDelimiter      string
	routingKeyDefaultValue   string
	routingKeyErrorOnEmpty   bool
	removeRkValuesFromRecord bool
	addTagToRecord           bool
	addTimestampToRecord     bool
	connection               *amqp.Connection
	channel                  *amqp.Channel
}

// reconnect make sure that the connection and the channel in the context is defined and is not closed, or try to reconnect
func (c *rabbitmqFLBPluginContext) reconnect() error {
	if c.connection != nil && c.channel != nil && !c.connection.IsClosed() {
		return nil
	}

	c.connection = nil
	c.channel = nil

	var err error
	c.connection, err = amqp.Dial("amqp://" + c.user + ":" + c.password + "@" + c.host + ":" + c.port + "/")
	if err != nil {
		return fmt.Errorf("failed to establish a connection to RabbitMQ: %v", err)
	}

	c.channel, err = c.connection.Channel()
	if err != nil {
		c.connection.Close()
		c.connection = nil
		return fmt.Errorf("failed to open a channel: %v", err)
	}

	logInfo("Established successfully a connection to the RabbitMQ-Server")

	err = c.channel.ExchangeDeclare(
		c.exchangeName, // name
		c.exchangeType, // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)

	if err != nil {
		c.connection.Close()
		c.connection = nil
		c.channel = nil
		return fmt.Errorf("failed to declare an exchange: %v", err)
	}

	return nil
}

func (c *rabbitmqFLBPluginContext) close() error {
	c.channel = nil
	if c.connection != nil {
		con := c.connection
		c.connection = nil
		return con.Close()
	}
	return nil
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	// Gets called only once when the plugin.so is loaded
	return output.FLBPluginRegister(def, "rabbitmq", "Stdout GO!")
}

//export FLBPluginInit
func FLBPluginInit(ctx unsafe.Pointer) int {
	// Gets called only once for each instance you have configured.
	var err error

	goPluginContext := rabbitmqFLBPluginContext{}

	goPluginContext.host = output.FLBPluginConfigKey(ctx, "RabbitHost")
	goPluginContext.port = output.FLBPluginConfigKey(ctx, "RabbitPort")
	goPluginContext.user = output.FLBPluginConfigKey(ctx, "RabbitUser")
	goPluginContext.password = output.FLBPluginConfigKey(ctx, "RabbitPassword")
	goPluginContext.exchangeName = output.FLBPluginConfigKey(ctx, "ExchangeName")
	goPluginContext.exchangeType = output.FLBPluginConfigKey(ctx, "ExchangeType")
	goPluginContext.routingKey = output.FLBPluginConfigKey(ctx, "RoutingKey")
	goPluginContext.routingKeyDelimiter = output.FLBPluginConfigKey(ctx, "RoutingKeyDelimiter")
	goPluginContext.routingKeyDefaultValue = output.FLBPluginConfigKey(ctx, "RoutingKeyDefaultValue")
	routingKeyErrorOnEmptyStr := output.FLBPluginConfigKey(ctx, "RoutingKeyErrorOnEmpty")
	removeRkValuesFromRecordStr := output.FLBPluginConfigKey(ctx, "RemoveRkValuesFromRecord")
	addTagToRecordStr := output.FLBPluginConfigKey(ctx, "AddTagToRecord")
	addTimestampToRecordStr := output.FLBPluginConfigKey(ctx, "AddTimestampToRecord")

	if len(goPluginContext.routingKeyDelimiter) < 1 {
		goPluginContext.routingKeyDelimiter = "."
		logInfo("The routing-key-delimiter is set to the default value '" + goPluginContext.routingKeyDelimiter + "' ")
	}

	goPluginContext.routingKeyErrorOnEmpty, err = strconv.ParseBool(routingKeyErrorOnEmptyStr)
	if err != nil {
		logError("Couldn't parse RemoveRkValuesFromRecord to boolean: ", err)
		return output.FLB_ERROR
	}

	goPluginContext.removeRkValuesFromRecord, err = strconv.ParseBool(removeRkValuesFromRecordStr)
	if err != nil {
		logError("Couldn't parse RemoveRkValuesFromRecord to boolean: ", err)
		return output.FLB_ERROR
	}

	goPluginContext.addTagToRecord, err = strconv.ParseBool(addTagToRecordStr)
	if err != nil {
		logError("Couldn't parse AddTagToRecord to boolean: ", err)
		return output.FLB_ERROR
	}

	goPluginContext.addTimestampToRecord, err = strconv.ParseBool(addTimestampToRecordStr)
	if err != nil {
		logError("Couldn't parse AddTimestampToRecord to boolean: ", err)
		return output.FLB_ERROR
	}

	err = RoutingKeyIsValid(goPluginContext.routingKey, goPluginContext.routingKeyDelimiter)
	if err != nil {
		logError("The Parsing of the Routing-Key failed: ", err)
		return output.FLB_ERROR
	}

	// initial connection
	err = goPluginContext.reconnect()
	if err != nil {
		logError("Failed to initialize RabbitMQ connection: ", err)
		return output.FLB_ERROR
	}

	logInfo("Connection to the RabbitMQ-Server is established successfully")

	output.FLBPluginSetContext(ctx, &goPluginContext)

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	// get context
	goPluginContext, _ := output.FLBPluginGetContext(ctx).(*rabbitmqFLBPluginContext)
	if goPluginContext == nil {
		return output.FLB_ERROR
	}

	err := goPluginContext.reconnect()
	if err != nil {
		logError("Failed to re-initialize RabbitMQ connection: ", err)
		return output.FLB_RETRY
	}

	// Gets called with a batch of records to be written to an instance.
	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	// Iterate Records
	for {
		// Extract Record
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		timestamp := ts.(output.FLBTime)

		parsedRecord := ParseRecord(record)

		if goPluginContext.addTagToRecord {
			parsedRecord["@tag"] = C.GoString(tag)
		}
		if goPluginContext.addTimestampToRecord {
			parsedRecord["@timestamp"] = timestamp.String()
		}

		rk, err := CreateRoutingKey(goPluginContext, &parsedRecord)
		if rk == "" {
			rk = goPluginContext.routingKeyDefaultValue
		}

		if rk == "" && goPluginContext.routingKeyErrorOnEmpty {
			if err == nil {
				err = fmt.Errorf("empty routing key")
			}
			logError("Couldn't create the Routing-Key", err)
			return output.FLB_ERROR
		}
		if rk == "" {
			if err != nil {
				logError("Couldn't access the Routing-Key", err)
				continue
			}
			continue
		}

		// marshal the record to json
		jsonString, err := json.Marshal(parsedRecord)
		if err != nil {
			logError("Couldn't marshal record: ", err)
			continue
		}

		err = goPluginContext.channel.Publish(
			goPluginContext.exchangeName, // exchange
			rk,                           // routing key
			false,                        // mandatory
			false,                        // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        jsonString,
			})
		if err != nil {
			goPluginContext.close()
			logError("Couldn't publish record: ", err)
			return output.FLB_RETRY
		}
	}
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func logInfo(msg string) {
	log.Printf("%s", msg)
}

func logError(msg string, err error) {
	log.Printf("%s: %s", msg, err)
}

func arrayContainsString(arr []string, str string) bool {
	for _, item := range arr {
		if item == str {
			return true
		}
	}
	return false
}

func main() {
}
