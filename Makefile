# Variables to control Makefile operation
 
CC = /usr/bin/gcc
CFLAGS = -Wall -g
main = main

# ****************************************************
all: $(main)

$(main): $(main).c 
	$(CC) $(CFLAGS) -o $(main) $(main).c -lcassandra

clean:
	$(RM) $(main)
