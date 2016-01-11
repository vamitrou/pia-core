#!/bin/bash

# chkconfig: 2345 99 01
# description: pia-core startup script

. /etc/init.d/functions

prog=pia-core
path=/data/fraud/test02
lockfile=/var/lock/subsys/$prog

start() {
        # Start daemons.
        echo -n $"Starting $prog: "
        exec $path/$prog >> /var/log/$prog.log 2>&1 &
	RETVAL=$?
	[ $RETVAL -eq 0 ] && touch $lockfile
	[ $RETVAL -eq 0 ] && echo -ne '\t\t\t\t\t[  \033[32mOK\033[0m  ]\n'
	return $RETVAL
}

stop() {
	echo -n "Stopping $prog: "
	killproc $prog
	rm -f $lockfile
	echo
}

case "$1" in
	start)
		start
		;;
	stop)
		stop
		;;
	status)
		status $prog
		;;
	restart|reload|condrestart)
		stop
		start
		;;
	*)
		echo $"Usage: $0 {start|stop|restart|reload|status}"
		exit 1

esac
exit 0

