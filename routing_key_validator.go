package main

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var validRKRegexp = regexp.MustCompile(`^\$((\[\"([^\s\"]+)\"\])|(\[\'([^\s\']+)\'\])|(\[[1-9][0-9]*\])|(\[[0]\]))+$`)

// RoutingKeyIsValid checks if a routing key is valid
func RoutingKeyIsValid(rk string, rkDelimiter string) error {
	if len(rk) <= 0 {
		return errors.New("Routing-Key shouldn't be empty")
	}

	if strings.Contains(rk, rkDelimiter) {
		splittedRk := strings.Split(rk, rkDelimiter)
		if arrayContainsString(splittedRk, "") {
			return errors.New("the given routing-key contains an empty value")
		}
		for _, subRk := range splittedRk {
			if strings.HasPrefix(subRk, "$") {
				if !validRKRegexp.MatchString(subRk) {
					return fmt.Errorf("the record_accessor '%s' is invalid", rk)
				}
			}
		}
	} else {
		if strings.HasPrefix(rk, "$") {
			if !validRKRegexp.MatchString(rk) {
				return fmt.Errorf("the record_accessor '%s' is invalid", rk)
			}
		}
	}
	return nil
}
