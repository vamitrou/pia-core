package connman

import (
	"net"
	"strconv"
	"strings"
)

func GetFreePort() int {
	c, _ := net.Listen("tcp", ":0")
	defer c.Close()
	addr := strings.Split(c.Addr().String(), ":")
	port := addr[len(addr)-1]
	portd, _ := strconv.Atoi(port)
	return portd
}
