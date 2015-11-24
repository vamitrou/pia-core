package connman

import (
	"net"
	"os/exec"
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

func GetPWD() string {
	pwd, _ := exec.Command("pwd").Output()
	pwdstr := strings.Trim(string(pwd), "\n\t\r")
	return pwdstr
}
